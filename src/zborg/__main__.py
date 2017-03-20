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
from zmq.devices.basedevice import ThreadDevice

SYNC = True
N_READERS = N_HASHERS = 4
N_COMPRESSORS = 4
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


def write_file(path, data, sync=SYNC):
    with open(path, 'wb') as f:
        f.write(data)
        if sync:
            f.flush()
            os.fsync(f.fileno())


@socket('discover_socket', zmq.PULL)
@socket('reader_socket', zmq.PUSH)
def discover_worker(context, discover_url, reader_url, discover_socket, reader_socket):
    """discover filenames of regular files below <root>"""
    discover_socket.bind(discover_url)
    reader_socket.bind(reader_url)
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
    for i in range(N_READERS):
        reader_socket.send_pyobj(DIE)


@socket('reader_socket', zmq.PULL)
@socket('hasher_socket', zmq.PUSH)
@socket('item_handler_socket', zmq.PUSH)
def reader_worker(context, reader_url, rh_s_fe_url, item_handler_url,
                  reader_socket, hasher_socket, item_handler_socket):
    """read a file <src> and chunk it"""
    reader_socket.connect(reader_url)
    hasher_socket.hwm = HWM_CHUNK_DATA
    hasher_socket.connect(rh_s_fe_url)
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
def hasher_worker(context, rh_s_be_url, checker_url, item_handler_url,
                  hasher_socket, checker_socket, item_handler_socket):
    """compute hash of a chunk"""
    hasher_socket.hwm = HWM_CHUNK_DATA
    hasher_socket.connect(rh_s_be_url)
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
    compressor_socket.bind(compressor_url)
    dying = 0
    while True:
        obj = checker_socket.recv_pyobj()
        if obj is DIE:
            dying += 1
            if dying < N_HASHERS:
                continue
            else:
                break
        src, chunk_no, id, data = obj
        have_chunk = os.path.exists(chunk_path(repo, id))
        if not have_chunk:
            compressor_socket.send_pyobj((id, data))
    for i in range(N_COMPRESSORS):
        compressor_socket.send_pyobj(DIE)


@socket('compressor_socket', zmq.PULL)
@socket('data_writer_socket', zmq.PUSH)
def compressor_worker(context, compressor_url, data_writer_url, compressor_socket, data_writer_socket):
    """compress a chunk"""
    compressor_socket.hwm = HWM_CHUNK_DATA
    compressor_socket.connect(compressor_url)
    data_writer_socket.hwm = HWM_CHUNK_DATA
    data_writer_socket.connect(data_writer_url)
    while True:
        obj = compressor_socket.recv_pyobj()
        if obj is DIE:
            break
        id, data = obj
        cdata = zlib.compress(data, COMPRESSION_LEVEL)
        data_writer_socket.send_pyobj((id, cdata))
    data_writer_socket.send_pyobj(DIE)


@socket('data_writer_socket', zmq.PULL)
def data_writer_worker(context, data_writer_url, repo, data_writer_socket):
    """write a chunk to the repo"""
    data_writer_socket.hwm = HWM_CHUNK_DATA
    data_writer_socket.bind(data_writer_url)
    dying = 0
    while True:
        obj = data_writer_socket.recv_pyobj()
        if obj is DIE:
            dying += 1
            if dying < N_COMPRESSORS:
                continue
            else:
                break
        id, data = obj
        write_file(chunk_path(repo, id), data)


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
            if dying < N_READERS + N_HASHERS:
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
            write_file(meta_path(repo, item_id), item_json)
    assert not items


def start_threads(repo):
    discover_url, reader_url, rh_s_fe_url, rh_s_be_url, hasher_url, checker_url, compressor_url, \
    data_writer_url, item_handler_url = ['inproc://%s' % name
    for name in 'discover,reader,rhsfe,rhsbe,hasher,checker,compressor,data_writer,item_handler'.split(',')]
    context = zmq.Context()
    discover_socket = context.socket(zmq.PUSH)
    discover_socket.connect(discover_url)
    start_thread(discover_worker, context, discover_url, reader_url)
    for i in range(N_READERS):
        start_thread(reader_worker, context, reader_url, rh_s_fe_url, item_handler_url)
    # we have multiple readers and multiple hashers, thus we need a streamer to connect them:
    rh_streamer = ThreadDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)
    rh_streamer.context_factory = lambda: context
    rh_streamer.bind_in(rh_s_fe_url)
    rh_streamer.bind_out(rh_s_be_url)
    rh_streamer.start()  # XXX since adding this thread, process does not terminate
    for i in range(N_HASHERS):
        start_thread(hasher_worker, context, rh_s_be_url, checker_url, item_handler_url)
    start_thread(checker_worker, context, checker_url, compressor_url, repo)
    for i in range(N_COMPRESSORS):
        start_thread(compressor_worker, context, compressor_url, data_writer_url)
    start_thread(data_writer_worker, context, data_writer_url, repo)
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
