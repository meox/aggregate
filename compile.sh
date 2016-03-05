#!/bin/bash

CXX=g++-4.9
CXXFLAGS="-std=c++1y -O3"

$CXX $CXXFLAGS src/aggregate.cpp -o aggregate -lboost_system -lboost_filesystem
