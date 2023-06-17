# RedisMMap
 mmap module for redis

```
// mmap file_path to key
// value_type is int8, uint8, int16, uint16, int32, uint32, int64, uint64, float, double, long double or string
MMAP key file_path value_type [value_size] [writable]

// clear contents in key (trancate file_path)
MCLEAR key

// keyにvalueを追加する
MADD key value [value ...]

// keyからindex位置にある値を取得する
MGET key index

// keyから複数のindex位置にある値を取得する
MMGET key index [index ...]

// keyのindex位置に値を書き込む
MSET key index value [index value ...]

// keyの値の数を取得する
MSIZE key

// keyの最後の値を取得して削除する
MPOP key
```

