#!/bin/sh
# I use this script to bring affinity_pb2.py up to date, when kernel/src/affinity.proto changes.
protoc --proto_path=../kernel/src/ ../kernel/src/affinity.proto --python_out=.
protoc --proto_path=../protobuf/src/google/protobuf/ ../protobuf/src/google/protobuf/descriptor.proto --python_out=.
