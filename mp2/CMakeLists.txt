cmake_minimum_required(VERSION 3.6)
project(assignment2)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
        testcases/create.conf
        testcases/delete.conf
        testcases/read.conf
        testcases/update.conf
        Application.cpp
        Application.h
        common.h
        EmulNet.cpp
        EmulNet.h
        Entry.cpp
        Entry.h
        HashTable.cpp
        HashTable.h
        Log.cpp
        Log.h
        Member.cpp
        Member.h
        Message.cpp
        Message.h
        MP1Node.cpp
        MP1Node.h
        MP2Node.cpp
        MP2Node.h
        Node.cpp
        Node.h
        Params.cpp
        Params.h
        Queue.h
        Trace.cpp
        Trace.h
        stdincludes.h
        stats.log
        )
add_executable(assignment2 ${SOURCE_FILES})