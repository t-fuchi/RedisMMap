#!/usr/bin/env python3

import pytest
import redis
import subprocess
import time
import os

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

def test_vadd_int32(scope_module):
    r = scope_nodule
    r.execute_command('del db')
    os.remove('file.mmap')
    assert r.execute_command('mmap db file.mmap int32 writable') == 0
    assert r.execute_command('vadd db 0 2 4 6 8 10') == 6
    assert r.execute_command('vsize db') == 6
    assert r.execute_command('vget db 1') == b'2'
    assert r.execute_command('vmget db 0 1 2 3 4 5') == [b'0', b'2', b'4', b'6', b'8', b'10']
    assert r.execute_command('vall db') == [b'0', b'2', b'4', b'6', b'8', b'10']
    assert r.execute_command('vpop db') == b'10'
    assert r.execute_command('vsize db') == 5
    assert r.execute_command('vfilepath db') == b'file.mmap'
    assert r.execute_command('vset db 1 -2 2 -4') == 2
    assert r.execute_command('vall db') == [b'0', b'-2', b'-4', b'6', b'8', b'10']
    assert r.execute_command('del db') == 1
    assert r.execute_command('mmap db2 file.mmap int32') == 6
    assert r.execute_command('vall db2') == [b'0', b'-2', b'-4', b'6', b'8', b'10']
    assert r.execute_command('vfilepath db2') == b'file.mmap'
    assert r.execute_command('del db2') == 1


