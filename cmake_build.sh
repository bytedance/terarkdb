#!/bin/bash

BASE=$PWD
mkdir -p $BASE/build

cd $BASE/build && cmake ../ && make -j $(nproc)
