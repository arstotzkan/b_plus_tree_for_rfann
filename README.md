# B+ Tree for Range-Filtered Approximate Nearest Neighbor (RFANN) Search

A high-performance disk-based B+ tree implementation optimized for RFANN queries with intelligent query caching for accelerated search performance.

## Features

- **Disk-based B+ Tree**: Efficient storage and retrieval of high-dimensional vectors with configurable page sizes
- **RFANN Support**: Range-filtered approximate nearest neighbor search with KNN capabilities
- **Query Caching**: Intelligent caching mechanism with 2-way inverted index for ~36-166x query speedup
- **Multiple Data Formats**: Support for `.fvecs`, `.npy`, binary, and synthetic data
- **Configurable Parameters**: Adjustable page size, tree order, and vector dimensions
- **Memory Efficient**: Optimized for large datasets with batch processing and move semantics

## Architecture

### Core Components

- **DiskBPlusTree**: Main B+ tree implementation with disk-based page management
- **QueryCache**: LRU cache with hash-based query identification and result storage
- **IndexDirectory**: Directory-based index management (stores `index.bpt` and `.cache/`)
- **DataObject**: Vector and numeric value storage abstraction
- **PageManager**: Low-level disk I/O and page allocation

### Caching Mechanism

The caching system provides significant performance improvements:

- **Query Hashing**: FNV-1a hash of input vector + range + K parameters
- **2-way Inverted Index**: Maps numeric keys ↔ query hashes for efficient cache invalidation
- **Binary Cache Files**: Compact storage of query results with metadata (creation/access times)
- **LRU Eviction**: Configurable size limits with least-recently-used eviction policy
- **Cache Invalidation**: Automatic invalidation when B+ tree is modified

## Build Instructions

### Prerequisites

- CMake 3.16 or higher
- C++17 compatible compiler (Visual Studio 2019+ on Windows, GCC 7+ on Linux)

### Building

```bash
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
```

### Configuration Options

```bash
# Configure B+ tree parameters (defaults shown)
cmake .. -DBPTREE_PAGE_SIZE=8192 -DBPTREE_ORDER=4 -DBPTREE_MAX_VECTOR_SIZE=128
```

## Usage

### Building Indices

#### From FVECS Files
```bash
./build/Release/build_index_fvecs --input data/siftsmall_base.fvecs --index data/sift_index
```

#### From NPY Files
```bash
./build/Release/build_index_npy --input data/vectors.npy --index data/npy_index
```

#### From Binary Files
```bash
./build/Release/build_index_binary --input data/vectors.bin --index data/binary_index
```

#### Synthetic Data
```bash
./build/Release/build_index --index data/synthetic_index --size 1000
```

### Searching

#### Interactive Search
```bash
# Range search with KNN
./build/Release/search_index --index data/sift_index --min 0 --max 100 --vector 1,2,3,4,5 --K 10

# Value search
./build/Release/search_index --index data/sift_index --value 42 --vector 1,2,3,4,5 --K 5
```

#### Batch Benchmark
```bash
# RFANN benchmark with groundtruth evaluation
./build/Release/search_index_test --index data/sift_index \
  --queries data/siftsmall_query.fvecs \
  --groundtruth data/siftsmall_groundtruth.ivecs \
  --num-queries 100
```

### Cache Management

#### Disable Caching
```bash
# Add --no-cache flag to any command
./build/Release/search_index --index data/sift_index --min 0 --max 100 --no-cache
```

#### Configure Cache
Edit `data/sift_index/config.ini`:
```ini
[cache]
cache_enabled = true
max_cache_size_mb = 100
```

## Performance Results

### Query Performance (SIFT-128 dataset)

| Scenario | Time (µs) | Speedup |
|----------|-----------|---------|
| Without Cache | ~38,000 | 1x |
| With Cache Hit | ~1,000 | ~36x |
| search_index KNN | ~107,000 → ~650 | ~166x |

### Cache Hit Rates
- Repeated identical queries: 100% hit rate
- Similar queries with same parameters: High hit rate
- Cache persistence across sessions

## File Formats

### Index Directory Structure
```
data/sift_index/
├── index.bpt           # B+ tree index file
├── config.ini          # Configuration settings
└── .cache/             # Query cache directory
    ├── inverted_index.bin    # 2-way key↔query mapping
    └── *.qcache             # Individual query result files
```

### Supported Input Formats

#### FVECS Format
```
[dimension:int32][vector:float32[dimension]] × N vectors
```

#### NPY Format
Standard NumPy array format with float32 vectors

#### Binary Format
```
[num_points:int32][dimension:int32][data:float32[num_points×dimension]]
```

## Command Line Reference

### Common Flags

| Flag | Description |
|------|-------------|
| `--index, -i` | Path to index directory (required) |
| `--input` | Input data file path |
| `--no-cache` | Disable query caching |
| `--help, -h` | Show usage information |

### Search-Specific Flags

| Flag | Description |
|------|-------------|
| `--min, --max` | Range search bounds |
| `--value, -v` | Exact value search |
| `--vector` | Query vector (comma-separated) |
| `--K, -k` | Number of nearest neighbors |
| `--queries, -q` | Query file for batch processing |
| `--groundtruth` | Groundtruth file for recall evaluation |
| `--num-queries` | Limit number of queries to process |

## Configuration

### Build-time Parameters

```cpp
// Configurable via CMake
#define BPTREE_PAGE_SIZE 8192      // Page size in bytes
#define BPTREE_ORDER 4             // B+ tree order
#define BPTREE_MAX_VECTOR_SIZE 128 // Maximum vector dimension
```

### Runtime Configuration

`config.ini` in index directory:
```ini
[cache]
cache_enabled = true
max_cache_size_mb = 100

[index]
# Future index configuration options
```

## Examples

### Complete SIFT Workflow

```bash
# 1. Build index from SIFT base vectors
./build/Release/build_index_fvecs \
  --input data/dataset/siftsmall_base.fvecs \
  --index data/sift_index

# 2. Run single query with caching
./build/Release/search_index \
  --index data/sift_index \
  --min 0 --max 1000 \
  --vector 132,7,241,89,56,199,18,220,144,63 \
  --K 10

# 3. Benchmark with groundtruth
./build/Release/search_index_test \
  --index data/sift_index \
  --queries data/dataset/siftsmall_query.fvecs \
  --groundtruth data/dataset/siftsmall_groundtruth.ivecs \
  --num-queries 100

# 4. Same query runs ~166x faster on second execution due to caching
```

### Cache Performance Demonstration

```bash
# First run (cache miss)
time ./build/Release/search_index --index data/sift_index --min 0 --max 30 --vector 1,2,3 --K 5
# Output: Query execution time: 107241 us

# Second run (cache hit)  
time ./build/Release/search_index --index data/sift_index --min 0 --max 30 --vector 1,2,3 --K 5
# Output: Cache HIT! Query execution time (from cache): 647 us
```

## Technical Details

### Memory Management
- Move semantics for efficient vector transfers
- Batch processing to reduce memory pressure
- Automatic cleanup of temporary objects
- Configurable batch sizes for large datasets

### Cache Implementation
- FNV-1a hashing for query identification
- Binary serialization for compact storage
- Atomic cache operations for thread safety
- Automatic directory structure creation

### B+ Tree Features
- Disk-based storage with configurable page sizes
- Range queries with efficient leaf traversal
- Support for both integer and float keys
- Compile-time size validation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with appropriate tests
4. Submit a pull request

## License

[Add your license information here]

## Acknowledgments

Optimized for RFANN (Range-Filtered Approximate Nearest Neighbor) search scenarios with intelligent caching for production-scale vector similarity search applications.