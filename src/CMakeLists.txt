cmake_minimum_required(VERSION 3.16)
project(BPlusTreeDisk)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)

add_executable(bptree
    main.cpp
    page_manager.cpp
    bplustree_disk.cpp
    DataObject.cpp
)
