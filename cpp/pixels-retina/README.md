# Pixels-Retina (C++)
This module contains the implementation of the server and client of the Retina grpc service.
They are implemented using C++.
The server is responsible for accepting single-point CRUD requests and row-group visibility requests and maintaining the write buffer and bitmaps+timestamp indexes.
The Java part of Pixels-Retina is only responsible for flushing a write buffer into a file.

> Warning: This module is deprecated and is to be reimplemented.

## Pre-requirements

proto-buf: https://github.com/protocolbuffers/protobuf/blob/main/cmake/README.md

grpc: https://github.com/grpc/grpc/blob/master/BUILDING.md

gtest: https://github.com/google/googletest/blob/main/googletest/README.md

fmt
```bash
sudo apt install libfmt-dev
```

rocksdb
