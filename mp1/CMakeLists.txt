cmake_minimum_required(VERSION 3.6)
project(mp1)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    testcases/msgdropsinglefailure.conf
    testcases/multifailure.conf
    testcases/singlefailure.conf
    Application.cpp
    Application.h
    dbg.log
    EmulNet.cpp
    EmulNet.h
    Log.cpp
    Log.h
    Member.cpp
    Member.h
    MP1Node.cpp
    MP1Node.h
    msgcount.log
    Params.cpp
    Params.h
    Queue.h
    stats.log
    stdincludes.h)

add_executable(mp1 ${SOURCE_FILES})