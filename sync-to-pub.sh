#/usr/bin/bash

cp -r src ../terark-zip-rocksdb-pub
sed -i '/TerocksPrivateCode/,/#endif.*TerocksPrivateCode/d' `find ../terark-zip-rocksdb-pub/src -type f -a '(' -name '*.cc' -o -name '*.h' ')'`

