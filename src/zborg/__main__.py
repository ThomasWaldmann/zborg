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

COMPRESSION_LEVEL = 9  # zlib 1..9
HWM_CHUNK_DATA =  200  # high-water-mark for sockets that carry messages that contain chunk data

DIE = None  # signal we send over the socket to terminate the receiver.


def meta_path(repo, id):
    return os.path.join(repo, 'meta', id)


def chunk_path(repo, id):
    return os.path.join(repo, 'data', id)


def discover_worker(context, discover_url, reader_url):
    """discover filenames of regular files below <root>"""
    discover_socket = context.socket(zmq.PULL)
    discover_socket.bind(discover_url)
    reader_socket = context.socket(zmq.PUSH)
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


def reader_worker(context, reader_url, hasher_url, item_handler_url):
    """read a file <src> and chunk it"""
    reader_socket = context.socket(zmq.PULL)
    reader_socket.bind(reader_url)
    hasher_socket = context.socket(zmq.PUSH)
    hasher_socket.set_hwm(HWM_CHUNK_DATA)
    hasher_socket.connect(hasher_url)
    item_handler_socket = context.socket(zmq.PUSH)
    item_handler_socket.connect(item_handler_url)
    chunksize = 2 ** 20  # for simplicity: fixed blocksize chunking
    while True:
        obj = reader_socket.recv_pyobj()
        if obj is DIE:
            break
        src, = obj
        chunk_no = 0
        with open(src, 'rb') as f:
            while True:
                data = f.read(chunksize)
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


def hasher_worker(context, hasher_url, checker_url, item_handler_url):
    """compute hash of a chunk"""
    hasher_socket = context.socket(zmq.PULL)
    hasher_socket.set_hwm(HWM_CHUNK_DATA)
    hasher_socket.bind(hasher_url)
    checker_socket = context.socket(zmq.PUSH)
    checker_socket.set_hwm(HWM_CHUNK_DATA)
    checker_socket.connect(checker_url)
    item_handler_socket = context.socket(zmq.PUSH)
    item_handler_socket.connect(item_handler_url)
    while True:
        obj = hasher_socket.recv_pyobj()
        if obj is DIE:
            break
        src, chunk_no, data = obj
        id = hashlib.new('sha256', data).hexdigest()
        checker_socket.send_pyobj((src, chunk_no, id, data))
        item_handler_socket.send_pyobj((src, chunk_no, id))
    checker_socket.send_pyobj(DIE)
    item_handler_socket.send_pyobj(DIE)


def checker_worker(context, checker_url, compressor_url, repo):
    """check if we already have a chunk in the repo"""
    checker_socket = context.socket(zmq.PULL)
    checker_socket.set_hwm(HWM_CHUNK_DATA)
    checker_socket.bind(checker_url)
    compressor_socket = context.socket(zmq.PUSH)
    compressor_socket.set_hwm(HWM_CHUNK_DATA)
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


def compressor_worker(context, compressor_url, writer_url):
    """compress a chunk"""
    compressor_socket = context.socket(zmq.PULL)
    compressor_socket.set_hwm(HWM_CHUNK_DATA)
    compressor_socket.bind(compressor_url)
    writer_socket = context.socket(zmq.PUSH)
    writer_socket.set_hwm(HWM_CHUNK_DATA)
    writer_socket.connect(writer_url)
    while True:
        obj = compressor_socket.recv_pyobj()
        if obj is DIE:
            break
        id, data = obj
        cdata = zlib.compress(data, COMPRESSION_LEVEL)
        writer_socket.send_pyobj((id, cdata))
    writer_socket.send_pyobj(DIE)


def writer_worker(context, writer_url, repo):
    """write a chunk to the repo"""
    writer_socket = context.socket(zmq.PULL)
    writer_socket.set_hwm(HWM_CHUNK_DATA)
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


def item_handler_worker(context, item_handler_url, repo):
    """collect metadata about an item, write it to repo"""
    item_handler_socket = context.socket(zmq.PULL)
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
            item_id = hashlib.new('sha256', item_json).hexdigest()
            with open(meta_path(repo, item_id), 'wb') as f:
                f.write(item_json)
                f.flush()
                os.fsync(f.fileno())
    assert not items


def start_threads(repo):
    def start_thread(func, *args):
        thread = Thread(target=func, args=args)
        thread.start()
        return thread

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


def main(repo, paths):
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