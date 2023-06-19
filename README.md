# RedisMMap
 mmap module for redis

## Install Redis
[Instruction](https://redis.io/docs/getting-started/installation)

## Build module and setup redis.conf
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
// return number of values
// value_type is int8, uint8, int16, uint16, int32, uint32, int64, uint64, float, double, long double or string
MMAP key file_path value_type [value_size] [writable]

// clear contents in key (trancate file_path)
// return number of values which are cleared
VCLEAR key

// add value at the end of key
// return number of values added
VADD key value [value ...]

// get value from key at index
// return value
VGET key index

// get values from key at indices
// return array of values
VMGET key index [index ...]

// get all values from key
// return array of values
VALL key

// set value at index in key
// return number of values set
VSET key index value [index value ...]

// get number of elements in key
// return number of values
VSIZE key

// pop the last value in key
VPOP key
```
