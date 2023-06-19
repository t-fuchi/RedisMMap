# RedisMMap
 mmap module for redis

## Install
Install Redis [Instruction](https://redis.io/docs/getting-started/installation)
```
$ cd RedisMMap/src
$ make
$ mv fmmap.so /{your_redis_path}/
$ echo enable-module-command yes >> /etc/redis/redis.conf
$ echo loadmodule /{your_redis_path}/fmmap.so >> /etc/redis/redis.conf
$ service redis-server start
```
## Usage
```
// mmap file_path to key
// value_type is int8, uint8, int16, uint16, int32, uint32, int64, uint64, float, double, long double or string
MMAP key file_path value_type [value_size] [writable]

// clear contents in key (trancate file_path)
VCLEAR key

// add value at the end of key
VADD key value [value ...]

// get value from key at index
VGET key index

// get values from key at indices
VMGET key index [index ...]

// set value at index in key
VSET key index value [index value ...]

// get the count of items in key
VSIZE key

// pop the last value in key
VPOP key
```
