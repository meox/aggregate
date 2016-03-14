#!/bin/bash

CXX=g++-4.9
CC=gcc-4.9

CXXFLAGS="-std=c++1y -O3"
CCFLAGS="-O3 -std=c99 -Wall -Wextra -Wshadow -Wcast-qual -Wcast-align -Wstrict-prototypes -Wstrict-aliasing=1 -Wswitch-enum -Wundef -pedantic "

$CC $CCFLAGS -c xxHash/xxhash.c
$CXX $CXXFLAGS src/aggregate.cpp xxhash.o -IxxHash/ -o aggregate -lboost_system -lboost_filesystem
