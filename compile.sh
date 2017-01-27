#!/bin/bash

CXX=g++-4.9
CC=gcc-4.9

CXXFLAGS="-O3 -g -std=c++14 -Wall -Wextra -Wshadow -Wcast-qual -Wcast-align -Wswitch-enum -Wundef -pedantic"
CCFLAGS="-O3 -std=c99 -Wall -Wextra -Wshadow -Wcast-qual -Wcast-align -Wstrict-prototypes -Wstrict-aliasing=1 -Wswitch-enum -Wundef -pedantic"

$CC $CCFLAGS -c xxHash/xxhash.c
$CXX $CXXFLAGS src/aggregate.cpp xxhash.o -IxxHash/ -I/opt/boost/boost_1_62_0/include -o aggregate -lboost_system -lboost_filesystem -lpthread

