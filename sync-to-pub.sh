#/usr/bin/bash

cp -r src ../terark-zip-rocksdb-pub
sed -i '/TerocksPrivateCode/,/#endif/d' `find ../terark-zip-rocksdb-pub -type f`

