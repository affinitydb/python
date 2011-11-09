#!/bin/sh
# I use this script to bring mvstore_pb2.py up to date, when kernel/src/mvstore.proto changes.
protoc --proto_path=../kernel/src/ ../kernel/src/mvstore.proto --python_out=.
protoc --proto_path=../protobuf/src/google/protobuf/ ../protobuf/src/google/protobuf/descriptor.proto --python_out=.
