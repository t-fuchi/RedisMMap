#!/usr/bin/env python3

import pytest
import redis
import subprocess
import time
import os
import struct

@pytest.fixture(scope="module", autouse=True)
def scope_module():
    if os.path.exists('dump.rdb'):
        os.remove('dump.rdb')
    subprocess.Popen(["redis-server", "redis.conf"],
                     stdout=open('log.txt', mode='w', buffering=1))
    time.sleep(1)
    r = redis.Redis()
    yield r
    r.shutdown()

def test_int8(scope_module):
    r = scope_nodule
    r.execute_command('del db')
    os.remove('file.mmap')
    assert r.execute_command('mmap db file.mmap int8 writable') == 0
    assert r.execute_command('vadd db 0 2 4 6 8 10') == 6
    assert r.execute_command('vcount db') == 6
    assert r.execute_command('vget db 1') == b'2'
    assert r.execute_command('vmget db 0 1 2 3 4 5') == [b'0', b'2', b'4', b'6', b'8', b'10']
    assert r.execute_command('vall db') == [b'0', b'2', b'4', b'6', b'8', b'10']
    assert r.execute_command('vpop db') == b'10'
    assert r.execute_command('vcount db') == 5
    assert r.execute_command('vfilepath db') == b'file.mmap'
    assert r.execute_command('vset db 1 -2 2 -4') == 2
    with pytest.raises(Exception):
      r.execute_command('vadd db 128')
    with pytest.raises(Exception):
      r.execute_command('vadd db -129')
    with pytest.raises(Exception):
      r.execute_command('vset db 5 0')
    with pytest.raises(Exception):
      r.execute_command('vset db -1 0')
    assert r.execute_command('vall db') == [b'0', b'-2', b'-4', b'6', b'8']
    assert r.execute_command('vtype db') == b'int8'
    assert r.execute_command('vsize db') == 1
    assert r.execute_command('del db') == 1

    assert r.execute_command('mmap db2 file.mmap int8') == 5
    assert r.execute_command('vall db2') == [b'0', b'-2', b'-4', b'6', b'8']
    assert r.execute_command('vfilepath db2') == b'file.mmap'
    with pytest.raises(Exception):
      r.execute_command('vclear db2')
    with pytest.raises(Exception):
      r.execute_command('vset db2 1 0')
    with pytest.raises(Exception):
      r.execute_command('vadd db2 1')
    with pytest.raises(Exception):
      r.execute_command('vpop db2')
    assert r.execute_command('del db2') == 1

    assert r.execute_command('mmap db file.mmap int8 writable') == 5
    assert r.execute_command('vclear db') == 5
    assert r.execute_command('vcount db') == 0
    with pytest.raises(Exception):
      r.execute_command('vget db 0')
    with pytest.raises(Exception):
      r.execute_command('vset db 0 0')
    assert r.execute_command('vpop db') == None
    assert r.execute_command('del db') == 1

    assert os.path.getsize('file.mmap') == 0
    with open('file.mmap', 'wb') as fout:
      for i in range(-100, 100):
        fout.write(struct.pack('b', i))
    assert r.execute_command('mmap db3 file.mmap int8') == 200
    for i in range(200):
      assert r.execute_command(f'vget db3 {i}') == i - 100
    assert r.execute_command('del db3') == 1



