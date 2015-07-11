#!/bin/sh

# compile sirius and immortal
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..

#make $@ -j`cat /proc/cpuinfo | grep processor | wc -l` install
make $@
