"""
micro borgbackup clone

PoC with in-process inter-thread communication via zeromq.

not optimized (yet?) for serialization or zero-copy efficiency.
very simplistic repo structure.
no file metadata.
no encryption, no authentication.
"""

import hashlib
import json
import os.path
import stat
import sys
import zlib

from threading import Thread

import zmq
from zmq.decorators import socket


COMPRESSION_LEVEL = 9  # zlib 1..9
HWM_CHUNK_DATA =  200  # high-water-mark for sockets that carry messages that contain chunk data
CHUNK_SIZE = 2 ** 20

DIE = None  # signal we send over the socket to terminate the receiver.


def meta_path(repo, id):
    return os.path.join(repo, 'meta', id)


def chunk_path(repo, id):
    return os.path.join(repo, 'data', id)


def id_hash(data):
    return hashlib.new('sha256', data).hexdigest()  # using hexdigest for simplicity


def start_thread(func, *args):
    thread = Thread(target=func, args=args)
    thread.start()
    return thread


@socket('discover_socket', zmq.PULL)
@socket('reader_socket', zmq.PUSH)
def discover_worker(context, discover_url, reader_url, discover_socket, reader_socket):
    """discover filenames of regular files below <root>"""
    discover_socket.bind(discover_url)
    reader_socket.connect(reader_url)
    while True:
        obj = discover_socket.recv_pyobj()
        if obj is DIE:
            break
        root, = obj
        for current, dirs, files in os.walk(root):
            for name in files:
                fpath = os.path.join(current, name)
                st = os.stat(fpath, follow_symlinks=False)
                if stat.S_ISREG(st.st_mode):
                    reader_socket.send_pyobj((fpath, ))
    reader_socket.send_pyobj(DIE)


@socket('reader_socket', zmq.PULL)
@socket('hasher_socket', zmq.PUSH)
@socket('item_handler_socket', zmq.PUSH)
def reader_worker(context, reader_url, hasher_url, item_handler_url,
                  reader_socket, hasher_socket, item_handler_socket):
    """read a file <src> and chunk it"""
    reader_socket.bind(reader_url)
    hasher_socket.hwm = HWM_CHUNK_DATA
    hasher_socket.connect(hasher_url)
    item_handler_socket.connect(item_handler_url)
    while True:
        obj = reader_socket.recv_pyobj()
        if obj is DIE:
            break
        src, = obj
        chunk_no = 0
        with open(src, 'rb') as f:
            while True:
                data = f.read(CHUNK_SIZE)  # for simplicity use fixed-size chunks
                if not data:
                    # chunk_no is the total number of chunks now, send it with id == None
                    item_handler_socket.send_pyobj((src, chunk_no, None))
                    break
                else:
                    # chunk_no is the sequence number of the current chunk
                    hasher_socket.send_pyobj((src, chunk_no, data))
                chunk_no += 1
    hasher_socket.send_pyobj(DIE)
    item_handler_socket.send_pyobj(DIE)


@socket('hasher_socket', zmq.PULL)
@socket('checker_socket', zmq.PUSH)
@socket('item_handler_socket', zmq.PUSH)
def hasher_worker(context, hasher_url, checker_url, item_handler_url,
                  hasher_socket, checker_socket, item_handler_socket):
    """compute hash of a chunk"""
    hasher_socket.hwm = HWM_CHUNK_DATA
    hasher_socket.bind(hasher_url)
    checker_socket.hwm = HWM_CHUNK_DATA
    checker_socket.connect(checker_url)
    item_handler_socket.connect(item_handler_url)
    while True:
        obj = hasher_socket.recv_pyobj()
        if obj is DIE:
            break
        src, chunk_no, data = obj
        id = id_hash(data)
        checker_socket.send_pyobj((src, chunk_no, id, data))
        item_handler_socket.send_pyobj((src, chunk_no, id))
    checker_socket.send_pyobj(DIE)
    item_handler_socket.send_pyobj(DIE)


@socket('checker_socket', zmq.PULL)
@socket('compressor_socket', zmq.PUSH)
def checker_worker(context, checker_url, compressor_url, repo, checker_socket, compressor_socket):
    """check if we already have a chunk in the repo"""
    checker_socket.hwm = HWM_CHUNK_DATA
    checker_socket.bind(checker_url)
    compressor_socket.hwm = HWM_CHUNK_DATA
    compressor_socket.connect(compressor_url)
    while True:
        obj = checker_socket.recv_pyobj()
        if obj is DIE:
            break
        src, chunk_no, id, data = obj
        have_chunk = os.path.exists(chunk_path(repo, id))
        if not have_chunk:
            compressor_socket.send_pyobj((id, data))
    compressor_socket.send_pyobj(DIE)


@socket('compressor_socket', zmq.PULL)
@socket('writer_socket', zmq.PUSH)
def compressor_worker(context, compressor_url, writer_url, compressor_socket, writer_socket):
    """compress a chunk"""
    compressor_socket.hwm = HWM_CHUNK_DATA
    compressor_socket.bind(compressor_url)
    writer_socket.hwm = HWM_CHUNK_DATA
    writer_socket.connect(writer_url)
    while True:
        obj = compressor_socket.recv_pyobj()
        if obj is DIE:
            break
        id, data = obj
        cdata = zlib.compress(data, COMPRESSION_LEVEL)
        writer_socket.send_pyobj((id, cdata))
    writer_socket.send_pyobj(DIE)


@socket('writer_socket', zmq.PULL)
def writer_worker(context, writer_url, repo, writer_socket):
    """write a chunk to the repo"""
    writer_socket.hwm = HWM_CHUNK_DATA
    writer_socket.bind(writer_url)
    while True:
        obj = writer_socket.recv_pyobj()
        if obj is DIE:
            break
        id, data = obj
        with open(chunk_path(repo, id), 'wb') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())


@socket('item_handler_socket', zmq.PULL)
def item_handler_worker(context, item_handler_url, repo, item_handler_socket):
    """collect metadata about an item, write it to repo"""
    item_handler_socket.bind(item_handler_url)
    items = {}
    dying = 0
    while True:
        obj = item_handler_socket.recv_pyobj()
        if obj is DIE:
            dying += 1
            if dying < 2:
                continue
            else:
                break
        src, chunk_no, id = obj
        item = items.setdefault(src, {})
        chunks = item.setdefault('chunks', [])
        if id is not None:
            chunks.append((chunk_no, id))
        else:
            item['chunks_total'] = chunk_no
        if len(chunks) == item.get('chunks_total'):
            items.pop(src)
            item['name'] = src
            item.pop('chunks_total')
            item['chunks'] = [id for chunk_no, id in sorted(item['chunks'])]
            item_json = json.dumps(item, indent=4).encode()
            item_id = id_hash(item_json)
            with open(meta_path(repo, item_id), 'wb') as f:
                f.write(item_json)
                f.flush()
                os.fsync(f.fileno())
    assert not items


def start_threads(repo):
    discover_url, reader_url, hasher_url, checker_url, compressor_url, writer_url, item_handler_url = \
        ['inproc://%s' % name
         for name in 'discover,reader,hasher,checker,compressor,writer,item_handler'.split(',')]
    context = zmq.Context()
    discover_socket = context.socket(zmq.PUSH)
    discover_socket.connect(discover_url)
    start_thread(discover_worker, context, discover_url, reader_url)
    start_thread(reader_worker, context, reader_url, hasher_url, item_handler_url)
    start_thread(hasher_worker, context, hasher_url, checker_url, item_handler_url)
    start_thread(checker_worker, context, checker_url, compressor_url, repo)
    start_thread(compressor_worker, context, compressor_url, writer_url)
    start_thread(writer_worker, context, writer_url, repo)
    start_thread(item_handler_worker, context, item_handler_url, repo)
    return discover_socket


def repo_prepare(repo):
    os.makedirs(meta_path(repo, ''), exist_ok=True)
    os.makedirs(chunk_path(repo, ''), exist_ok=True)


def main(repo, paths):
    repo_prepare(repo)
    discover_socket = start_threads(repo)
    for path in paths:
        discover_socket.send_pyobj((path, ))
    discover_socket.send_pyobj(DIE)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('python -m zborg repo path [path...]')
        sys.exit(1)
    repo, paths = sys.argv[1], sys.argv[2:]
    main(repo, paths)
