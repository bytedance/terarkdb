#!/bin/bash

if [ `uname` == Darwin ]; then
	sysctl -n machdep.cpu.features | tr 'A-Z' 'a-z' | sed -E 's/[[:space:]]+/'$'\\\n/g'
else
	cat /proc/cpuinfo | sed -n '/^flags\s*:\s*/s/^[^:]*:\s*//p' | uniq | tr 'A-Z' 'a-z' | sed 's/\s\+/\n/g'
fi
