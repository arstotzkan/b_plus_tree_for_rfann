cmake_minimum_required(VERSION 3.16)
project(BPlusTreeDisk)

set(CMAKE_CXX_STANDARD 17)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../include)

# Configurable B+ tree parameters (can be set via cmake -DBPTREE_ORDER=8 etc.)
# Default values for SIFT-128 vectors
if(NOT DEFINED BPTREE_PAGE_SIZE)
    set(BPTREE_PAGE_SIZE 8192)
endif()
if(NOT DEFINED BPTREE_ORDER)
    set(BPTREE_ORDER 4)
endif()
if(NOT DEFINED BPTREE_MAX_VECTOR_SIZE)
    set(BPTREE_MAX_VECTOR_SIZE 128)
endif()

# Add compile definitions
add_compile_definitions(
    BPTREE_PAGE_SIZE=${BPTREE_PAGE_SIZE}
    BPTREE_ORDER=${BPTREE_ORDER}
    BPTREE_MAX_VECTOR_SIZE=${BPTREE_MAX_VECTOR_SIZE}
)

message(STATUS "B+ Tree Config: PAGE_SIZE=${BPTREE_PAGE_SIZE}, ORDER=${BPTREE_ORDER}, MAX_VECTOR_SIZE=${BPTREE_MAX_VECTOR_SIZE}")

# Common source files (in utils subdirectory)
set(COMMON_SOURCES
    utils/page_manager.cpp
    utils/bplustree_disk.cpp
    utils/DataObject.cpp
    utils/query_cache.cpp
    utils/index_directory.cpp
    utils/logger.cpp
    utils/vector_store.cpp
)

# Build index with synthetic data executable
add_executable(build_index_with_synthetic
    build_index_with_synthetic_data.cpp
    ${COMMON_SOURCES}
)


# Build index from fvecs file executable
add_executable(build_index_fvecs
    build_index_from_fvecs.cpp
    ${COMMON_SOURCES}
)

# Search index executable (range search with flags)
add_executable(search_index
    search_from_index.cpp
    ${COMMON_SOURCES}
)

# Search index test executable (RFANN benchmark)
add_executable(search_index_test
    search_from_index_test.cpp
    ${COMMON_SOURCES}
)

# Add node executable (insert new data objects)
add_executable(add_node
    add_node.cpp
    ${COMMON_SOURCES}
)

# Remove node executable (delete data objects)
add_executable(remove_node
    remove_node.cpp
    ${COMMON_SOURCES}
)

# Cache reader utility (inspect cached queries)
add_executable(read_cache
    read_cache.cpp
    ${COMMON_SOURCES}
)

# Cache clear utility (erase all cache files)
add_executable(clear_cache
    clear_cache.cpp
    ${COMMON_SOURCES}
)
