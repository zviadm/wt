# wt - GoLang bindings for WiredTiger
[![Build Status](https://travis-ci.org/zviadm/wt.svg?branch=master)](https://travis-ci.org/zviadm/wt)

These bindings mainly focus on performance and simplicity. Target is to
support WiredTiger only as a simple transactional key/value store.

WiredTiger is a high quality, production ready storage engine. It also has an extensive
in-depth documentation: http://source.wiredtiger.com/3.2.1/index.html.

If you have a need for a storage engine, give WiredTiger a shot instead of RocksDB.

# Testing

Uses [tt](https://github.com/zviadm/tt) for testing. Note that first run of the tests will take a long time since
WiredTiger libraries take a long time to build from scratch.

```
$: go install github.com/zviadm/tt/tt
$: tt -v ./...
```

