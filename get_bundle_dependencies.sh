#!/usr/bin/env bash

if [ `uname` = Linux ]; then
    TERARK_CORE_VERSION=1.0.0.58
    if ! test - terark-core; then
        wget -O terark_core.tar.gz http://d.scm.byted.org/api/download/ceph:toutiao.terark.terark_core_${TERARK_CORE_VERSION}.tar.gz
        rm -rf terark-core
        mkdir terark-core
        tar -xvf terark_core.tar.gz -C terark-core
        rm -rf terark_core.tar.gz
    fi
fi
rm -rf terark-zip-rocksdb
git clone --depth=1 git@code.byted.org:storage/terark-zip-rocksdb.git
rm -rf boost-include
git clone --depth=1 git@code.byted.org:storage/boost-include.git
