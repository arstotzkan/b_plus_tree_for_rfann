cmake_minimum_required(VERSION 3.16)
project(BPlusTreeDisk)

set(CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)

# Common source files (excluding main files)
set(COMMON_SOURCES
    page_manager.cpp
    bplustree_disk.cpp
    DataObject.cpp
)

# Build index executable
add_executable(build_index
    build_index_with_synthetic_data.cpp
    ${COMMON_SOURCES}
)

# Search index executable
add_executable(search_index
    search_from_index.cpp
    ${COMMON_SOURCES}
)
