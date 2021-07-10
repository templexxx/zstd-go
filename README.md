zstd-go
===

zstd-go is zstd compression in pure Go based on [klauspost's version](https://github.com/klauspost/compress/tree/master/zstd) with
these changes:

1. removing concurrent Encoding/Decoding
2. using 3rd pkg [xxhash](https://github.com/cespare/xxhash) directly