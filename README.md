# B+ Tree for Range-Filtered Approximate Nearest Neighbor (RFANN) Search

A high-performance disk-based B+ tree implementation optimized for RFANN queries with intelligent query caching for accelerated search performance.

## Features

- **Disk-based B+ Tree**: Efficient storage and retrieval of high-dimensional vectors with configurable page sizes
- **RFANN Support**: Range-filtered approximate nearest neighbor search with KNN capabilities
- **Query Caching**: Intelligent caching mechanism with interval tree-based mechanism for query speedup
- **Bulk Loading**: Efficient bottom-up tree construction for initial index creation
- **Multiple Data Formats**: Support for `.fvecs` and synthetic data generation
- **Configurable Parameters**: Adjustable page size, tree order, and vector dimensions
- **Memory Index**: Optional in-memory index loading for faster repeated queries
- **Parallel Search**: Multi-threaded KNN search for large range queries

## Architecture

### Core Components

- **DiskBPlusTree**: Main B+ tree implementation with disk-based page management
- **QueryCache**: LRU cache with interval tree for efficient range-based cache lookups
- **VectorStore**: Separate storage for high-dimensional vectors
- **IndexDirectory**: Directory-based index management (stores `index.bpt`, `index.bpt.vectors`, and `.cache/`)
- **DataObject**: Vector and numeric value storage abstraction
- **PageManager**: Low-level disk I/O and page allocation

### Caching Mechanism

The caching system provides significant performance improvements:

- **Query Hashing**: FNV-1a hash of input vector + range parameters
- **Interval Tree Index**: O(log N + M) range overlap queries instead of O(N) linear scan
- **Similarity Matching**: Configurable vector cosine similarity and range IoU thresholds
- **Binary Cache Files**: Compact storage of query results with metadata (creation/access times)
- **LRU Eviction**: Configurable size limits with least-recently-used eviction policy
- **Cache Invalidation**: Automatic invalidation when B+ tree is modified

---

## Build Instructions

### Prerequisites

| Platform | Requirements |
|----------|--------------|
| **Windows** | Visual Studio 2019 or later (with C++ workload), CMake 3.16+ |
| **Linux** | GCC 7+ or Clang 5+, CMake 3.16+, make |

### Building on Windows

#### Option 1: Visual Studio Developer Command Prompt

```cmd
:: Open "Developer Command Prompt for VS 2019/2022"
cd path\to\b_plus_tree_for_rfann

:: Create build directory
mkdir build
cd build

:: Configure with CMake (generates Visual Studio solution)
cmake .. -G "Visual Studio 17 2022" -A x64

:: Build Release configuration
cmake --build . --config Release

:: Executables will be in build\src\Release\
```

#### Option 2: Visual Studio IDE

1. Open Visual Studio
2. Select **File → Open → CMake...**
3. Navigate to the project root and select `CMakeLists.txt`
4. Select **Release** configuration from the dropdown
5. Build with **Build → Build All** (Ctrl+Shift+B)

#### Option 3: PowerShell with CMake

```powershell
cd path\to\b_plus_tree_for_rfann
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --config Release
```

### Building on Linux

```bash
# Clone or navigate to project directory
cd /path/to/b_plus_tree_for_rfann

# Create build directory
mkdir -p build
cd build

# Configure with CMake
cmake .. -DCMAKE_BUILD_TYPE=Release

# Build (use -j for parallel compilation)
make -j$(nproc)

# Executables will be in build/src/
```

### CMake Configuration Options

Configure B+ tree parameters at build time:

```bash
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DBPTREE_PAGE_SIZE=8192 \
  -DBPTREE_ORDER=4 \
  -DBPTREE_MAX_VECTOR_SIZE=128
```

| Option | Default | Description |
|--------|---------|-------------|
| `BPTREE_PAGE_SIZE` | 8192 | Page size in bytes for disk I/O |
| `BPTREE_ORDER` | 4 | B+ tree order (max keys per node) |
| `BPTREE_MAX_VECTOR_SIZE` | 128 | Maximum vector dimension |

---

## Executables Reference

After building, the following executables are available:

| Executable | Description |
|------------|-------------|
| `build_index` | Build index from synthetic data |
| `build_index_fvecs` | Build index from FVECS file |
| `search_index` | Interactive search (range/value/KNN) |
| `search_index_test` | Batch benchmark with groundtruth |
| `add_node` | Insert a new data object |
| `remove_node` | Delete a data object |
| `read_cache` | Inspect cached queries |
| `clear_cache` | Clear all cached queries |

### build_index

Build a B+ tree index from synthetic random data.

```bash
build_index --index <dir> --size <count> [options]
```

| Flag | Short | Description |
|------|-------|-------------|
| `--index` | `-o` | Path to index directory (required) |
| `--size` | `-s` | Number of synthetic objects to generate (required) |
| `--dimension` | `-d` | Vector dimension (default: 128) |
| `--order` | | B+ tree order (default: auto-calculated) |
| `--max-cache-size` | | Maximum cache size in MB (default: 100) |
| `--help` | `-h` | Show help message |

**Example:**
```bash
# Windows
.\build\src\Release\build_index.exe --index data\my_index --size 10000 --dimension 128

# Linux
./build/src/build_index --index data/my_index --size 10000 --dimension 128
```

### build_index_fvecs

Build a B+ tree index from a `.fvecs` file (standard vector dataset format).

```bash
build_index_fvecs --input <file> --index <dir> [options]
```

| Flag | Short | Description |
|------|-------|-------------|
| `--input` | `-i` | Path to input .fvecs file (required) |
| `--index` | `-o` | Path to index directory (required) |
| `--order` | | B+ tree order (default: auto-calculated) |
| `--batch-size` | | Vectors per batch (default: 10) |
| `--max-cache-size` | | Maximum cache size in MB (default: 100) |
| `--help` | `-h` | Show help message |

**Example:**
```bash
# Windows
.\build\src\Release\build_index_fvecs.exe --input data\siftsmall_base.fvecs --index data\sift_index

# Linux
./build/src/build_index_fvecs --input data/siftsmall_base.fvecs --index data/sift_index
```

### search_index

Interactive search with support for range queries, value queries, and KNN.

```bash
search_index --index <dir> [--min <n> --max <n> | --value <n>] [options]
```

| Flag | Short | Description |
|------|-------|-------------|
| `--index` | `-i` | Path to index directory (required) |
| `--min` | | Minimum key for range search |
| `--max` | | Maximum key for range search |
| `--value` | `-v` | Exact key value search |
| `--vector` | | Query vector (comma-separated floats) |
| `--K` | `-k` | Number of nearest neighbors |
| `--limit` | | Limit number of results displayed |
| `--no-cache` | | Disable query caching |
| `--parallel` | | Enable parallel KNN search |
| `--threads` | | Number of threads (0 = auto) |
| `--memory-index` | | Load index into memory |
| `--vec-sim` | | Vector similarity threshold [0.0-1.0] |
| `--range-sim` | | Range similarity threshold [0.0-1.0] |
| `--help` | `-h` | Show help message |

**Examples:**
```bash
# Range search
search_index --index data/sift_index --min 0 --max 100

# KNN search with query vector
search_index --index data/sift_index --min 0 --max 1000 --vector 1.0,2.0,3.0 --K 10

# Value search with memory index for speed
search_index --index data/sift_index --value 42 --memory-index

# Parallel KNN search
search_index --index data/sift_index --min 0 --max 10000 --vector 1,2,3 --K 50 --parallel --threads 4
```

### search_index_test

Batch benchmark tool for RFANN evaluation with groundtruth comparison.

```bash
search_index_test --index <dir> --queries <file> [options]
```

| Flag | Short | Description |
|------|-------|-------------|
| `--index` | `-i` | Path to index directory (required) |
| `--queries` | `-q` | Path to query vectors file (.fvecs) |
| `--groundtruth` | | Path to groundtruth file (.ivecs) |
| `--num-queries` | | Number of queries to run (default: all) |
| `--no-cache` | | Disable query caching |
| `--parallel` | | Enable parallel KNN search |
| `--threads` | | Number of threads (0 = auto) |
| `--memory-index` | | Load index into memory |
| `--vec-sim` | | Vector similarity threshold [0.0-1.0] |
| `--range-sim` | | Range similarity threshold [0.0-1.0] |
| `--help` | `-h` | Show help message |

**Example:**
```bash
search_index_test --index data/sift_index \
  --queries data/siftsmall_query.fvecs \
  --groundtruth data/siftsmall_groundtruth.ivecs \
  --num-queries 100 --parallel --memory-index
```

### add_node

Insert a new data object into an existing index.

```bash
add_node --index <dir> --key <key> --vector <v1,v2,...>
```

| Flag | Short | Description |
|------|-------|-------------|
| `--index` | `-i` | Path to index directory (required) |
| `--key` | `-k` | Key value for the new entry (required) |
| `--vector` | `-v` | Vector data, comma-separated (required) |
| `--help` | `-h` | Show help message |

**Example:**
```bash
add_node --index data/my_index --key 42 --vector 1.0,2.0,3.0,4.0
```

### remove_node

Delete a data object from an existing index.

```bash
remove_node --index <dir> --key <key> [--vector <v1,v2,...>]
```

| Flag | Short | Description |
|------|-------|-------------|
| `--index` | `-i` | Path to index directory (required) |
| `--key` | `-k` | Key value to delete (required) |
| `--vector` | `-v` | Vector to match (optional, for specific deletion) |
| `--help` | `-h` | Show help message |

**Examples:**
```bash
# Remove first entry with key 42
remove_node --index data/my_index --key 42

# Remove specific entry matching key AND vector
remove_node --index data/my_index --key 42 --vector 1.0,2.0,3.0,4.0
```

### read_cache

Inspect cached query results.

```bash
read_cache --index <dir> [options]
```

| Flag | Short | Description |
|------|-------|-------------|
| `--index` | `-i` | Path to index directory (required) |
| `--query-id` | `-q` | Show specific query by ID |
| `--summary` | `-s` | Show only summary information |
| `--help` | `-h` | Show help message |

**Examples:**
```bash
# List all cached queries
read_cache --index data/my_index

# Show summary only
read_cache --index data/my_index --summary

# Show specific query details
read_cache --index data/my_index --query-id abc123def456
```

### clear_cache

Clear all cached query results.

```bash
clear_cache --index <dir> [options]
```

| Flag | Short | Description |
|------|-------|-------------|
| `--index` | `-i` | Path to index directory (required) |
| `--confirm` | `-c` | Skip confirmation prompt |
| `--help` | `-h` | Show help message |

**Examples:**
```bash
# Clear with confirmation prompt
clear_cache --index data/my_index

# Clear without prompt (for scripts)
clear_cache --index data/my_index --confirm
```

---

## Usage Examples

### Complete SIFT Workflow

```bash
# 1. Build index from SIFT base vectors
./build/src/build_index_fvecs \
  --input data/siftsmall_base.fvecs \
  --index data/sift_index

# 2. Run single query with caching
./build/src/search_index \
  --index data/sift_index \
  --min 0 --max 1000 \
  --vector 132,7,241,89,56,199,18,220,144,63 \
  --K 10

# 3. Benchmark with groundtruth
./build/src/search_index_test \
  --index data/sift_index \
  --queries data/siftsmall_query.fvecs \
  --groundtruth data/siftsmall_groundtruth.ivecs \
  --num-queries 100

# 4. Same query runs ~166x faster on second execution due to caching
```

### Cache Configuration

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
    ├── interval_tree.bin    # Interval tree
    └── *.qcache             # Individual query result files
```

### Supported Input Formats

#### FVECS Format
```
[dimension:int32][vector:float32[dimension]] × N vectors
```

#### NPY Format
Standard NumPy array format with float32 vectors

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