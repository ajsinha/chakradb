Here is the complete, full-length **System Requirements, Architecture, and Design Specification** for **ChakraDB** in Markdown format. You can easily copy and paste the text block below directly into any `.md` file or documentation viewer.

```markdown
# System Requirements, Architecture, and Design Specification

## Project Codename: ChakraDB (Embedded Engine Core)

**Document Version:** 1.0  
**Status:** Architectural Blueprint  
**Target Runtimes:** Rust, Java, Python  
**Target Operating Systems:** Linux, macOS, Windows (POSIX & Win32 abstractions)

---

## 1. Executive Summary & Core Objectives

### 1.1 Executive Summary
**ChakraDB** is an ultra-high-performance, embeddable, in-memory analytical and operational database engine written in **Rust**. It combines $O(1)$ write-ingestion throughput with SIMD-vectorized, zero-copy analytical processing over memory-mapped columnar data. By utilizing PostgreSQL’s native parser frontend (`libpg_query`) and maintaining an internal Apache Arrow physical layer, ChakraDB provides complete PostgreSQL dialect compatibility while enabling **zero-copy ($0\text{ms}$ FFI) memory exchange** with modern data science frameworks like **Polars**, **Pandas**, and **PyArrow**.

ChakraDB addresses the architectural limitations of traditional single-writer embeddable analytical engines (e.g., DuckDB) by utilizing a **Dual-Storage Engine Architecture**: a high-throughput LSM write-ahead logging (WAL) layer powered by Speedb/RocksDB paired with an Apache Arrow memory-mapped hot tier and a durable, cold storage tier built on Apache Iceberg and Parquet.

### 1.2 Key Objectives
* **Postgres Compatibility:** Accept and parse complex PostgreSQL dialect SQL using the official PostgreSQL C-parser.
* **Dual Storage Strategy:** Achieve ultra-low latency write ingestion ($O(1)$) using an LSM-tree while maintaining SIMD-accelerated columnar scan speed on memory-mapped Arrow IPC buffers.
* **Zero-Copy Interoperability:** Provide seamless, copy-free data exchange via the Apache Arrow C Data Interface (`PyCapsule` for Python, Panama FFI for Java).
* **Platform & Hardware Agnostic:** Execute cleanly across Linux, macOS, and Windows on both x86_64 (AVX-512) and AArch64 (ARM Neon) architectures without OS-level locks or platform-specific driver dependencies.
* **Open Durability Standard:** Guarantee ACID durability without proprietary disk formats by backing persistent states with Apache Iceberg catalog manifests and Parquet files.

---

## 2. Comprehensive Requirements Specification

### 2.1 Functional Requirements (FR)

| ID | Category | Requirement Description | Priority |
| :--- | :--- | :--- | :--- |
| **FR-01** | **SQL Parsing** | ChakraDB MUST parse standard PostgreSQL SQL queries using `libpg_query` AST generation, preserving Postgres operators, expressions, and type casts. | **P0** |
| **FR-02** | **In-Memory Querying** | The execution core MUST evaluate relational plans over in-memory Apache Arrow `RecordBatches` using vectorized SIMD kernels. | **P0** |
| **FR-03** | **Dual Storage** | Point writes (INSERT/UPDATE/DELETE) MUST enter an embedded LSM engine (Speedb/RocksDB) for $O(1)$ write response times. Scans MUST run over Arrow columnar layers. | **P0** |
| **FR-04** | **Zero-Copy Python** | ChakraDB MUST accept and emit Arrow data via Python's `__arrow_c_stream__` PyCapsule interface without memory duplication. | **P0** |
| **FR-05** | **Zero-Copy Java** | ChakraDB MUST expose C-ABI FFI endpoints compatible with Java 21+ Foreign Function & Memory API (Panama) for zero-copy off-heap memory mapping. | **P1** |
| **FR-06** | **Cold Storage Flush** | Background threads MUST periodically flush compacted LSM mutations into Apache Iceberg table layouts formatted as Parquet files. | **P1** |
| **FR-07** | **Snapshot Isolation** | Queries MUST read from snapshot-consistent views governed by a monotonic Log Sequence Number (LSN) and durability watermark ($W$). | **P0** |

### 2.2 Non-Functional Requirements (NFR)

| ID | Category | Metric / Specification | Priority |
| :--- | :--- | :--- | :--- |
| **NFR-01** | **Ingestion Throughput** | Must sustain $> 1,000,000$ row insertions per second on standard server-grade hardware without write stalls. | **P0** |
| **NFR-02** | **FFI Boundary Latency** | Memory transfer overhead between Rust, Polars/Pandas (Python), and Java JVM MUST be $0\text{ms}$ (less than $10\,\mu\text{s}$ wrapper instantiation). | **P0** |
| **NFR-03** | **Platform Agnosticism** | Core library MUST compile to target triples `x86_64-unknown-linux-gnu`, `aarch64-apple-darwin`, and `x86_64-pc-windows-msvc`. | **P0** |
| **NFR-04** | **Memory Footprint** | ChakraDB initialization overhead MUST remain under $50\text{ MB}$ RSS when idle. | **P1** |
| **NFR-05** | **Concurrency** | The LSM write engine MUST support concurrent lock-free writes while analytical reader tasks execute concurrently. | **P0** |

---

## 3. High-Level Architecture & Layered Topology

The ChakraDB architecture is partitioned into **four distinct, modular layers**:


```

┌─────────────────────────────────────────────────────────────────────────────┐
│                         LAYER 1: MULTI-LANGUAGE FFI                         │
│   Python Extension (PyO3)  │  Java FFM (Panama C-ABI)  │  Native Rust API │
└──────────────────────────────────────┬──────────────────────────────────────┘
│ (Zero-Copy Arrow C Data Interface)
┌──────────────────────────────────────▼──────────────────────────────────────┐
│                    LAYER 2: PARSER & QUERY OPTIMIZER                        │
│    `libpg_query` (PostgreSQL AST)  ──>  DataFusion Logical & Physical Plan  │
│                        (Locality & NUMA-Aware Planner)                      │
└──────────────────────────────────────┬──────────────────────────────────────┘
│ (Vectorized Execution Kernels)
┌──────────────────────────────────────▼──────────────────────────────────────┐
│                      LAYER 3: IN-MEMORY EXECUTION CORE                      │
│    Apache Arrow IPC Buffers  │  SIMD Array Processing  │  Merge-on-Read     │
└──────────────────────────────────────┬──────────────────────────────────────┘
│ (LSN Watermark & Flush Management)
┌──────────────────────────────────────▼──────────────────────────────────────┐
│                       LAYER 4: DURABLE STORAGE TIER                         │
│     Write Layer: Speedb / RocksDB LSM  │  Cold Tier: Apache Iceberg/Parquet │
└─────────────────────────────────────────────────────────────────────────────┘

```

### 3.1 Layer Responsibilities

1. **Layer 1 (Multi-Language FFI):** Manages foreign function bindings. Exposes low-overhead exports using `PyO3` for Python and C-ABI export symbols (`cdylib`) for Java Panama and C/C++. Implements the Apache Arrow C Data Interface (`FFI_ArrowArrayStream`).
2. **Layer 2 (Parser & Optimizer):** Ingests raw SQL strings. Parses syntax via `libpg_query` into a Postgres AST JSON/Protobuf object. Converts the AST into Apache Arrow DataFusion logical plans, applying cost-based optimization and topology/cache locality heuristics.
3. **Layer 3 (In-Memory Execution Core):** Holds hot columnar records as memory-mapped Apache Arrow IPC slices. Performs zero-copy SIMD processing. Executes Merge-on-Read algorithms to unify live LSM row-deltas with cold Arrow columns.
4. **Layer 4 (Durable Storage Tier):** Contains the write-ahead log (WAL) and primary index via an embedded Speedb/RocksDB LSM engine. Flushes historical data to disk using Apache Iceberg manifest specs and Parquet file formats.

---

## 4. Hardware & Operating System Platform-Agnostic Design

To ensure ChakraDB runs across all major OS platforms (Linux, macOS, Windows) and CPU architectures (x86_64, ARM64) without modification, the following abstraction strategies are specified:


```

```
              ┌────────────────────────────────────────┐
              │       ChakraDB Core (Pure Rust)        │
              └───────────────────┬────────────────────┘
                                  │
     ┌────────────────────────────┼────────────────────────────┐
     ▼                            ▼                            ▼

```

┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│   POSIX Abstr.  │          │  Win32 Abstr.   │          │ macOS CoreMem   │
│ (Linux / macOS) │          │    (Windows)    │          │    (Apple)      │
└────────┬────────┘          └────────┬────────┘          └────────┬────────┘
│                            │                            │
▼                            ▼                            ▼
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│ `mmap` / `fsync`│          │`CreateFileMapping`│        │`mmap` / SIMD    │
│  x86_64 / ARM64 │          │  VirtualAlloc   │          │   ARM Neon      │
└─────────────────┘          └─────────────────┘          └─────────────────┘

```

### 4.1 Storage & File Mapping Abstraction
* **Cross-Platform Virtual Memory Mapping:** Operating system memory mapping varies between POSIX systems (`mmap`/`munmap`) and Windows (`CreateFileMappingW`/`MapViewOfFile`). ChakraDB abstracts physical disk-to-memory mapping using the Rust `memmap2` crate, wrapping platform-specific memory page calls behind a platform-neutral `MemoryMappedBuffer` trait.
* **Asynchronous Direct I/O:** File system I/O avoids platform-specific blocking calls:
  * **Linux:** Utilizes `io_uring` via `tokio-epoll-uring` when available, falling back to standard `epoll` thread pools.
  * **macOS:** Uses `kqueue` for event loop management and multi-threaded asynchronous I/O.
  * **Windows:** Employs **I/O Completion Ports (IOCP)** via `mio`.

### 4.2 SIMD Vectorization Abstraction
Vectorized SIMD processing must adapt dynamically to CPU instruction sets at runtime without requiring separate compiled binaries:
* **Target Support:** Support x86_64 `AVX-512` / `AVX2` and ARM64 `NEON`.
* **Runtime Dispatch:** Implemented using Rust's `std::is_x86_feature_detected!` and `std::arch::aarch64` macro dispatchers. At engine initialization, a CPU feature scan selects the appropriate vector kernel pointer (e.g., AVX2 vs. Neon vs. fallback scalar iterator).

### 4.3 C-Runtime & Compiler Agnosticism
* **No Hard Dependencies on GNU C Library (glibc):** To support minimalist Linux environments (Alpine/musl) and standalone Windows/macOS installations, the C-deps layer (`libpg_query` and Speedb/RocksDB C++ cores) is compiled using static linkage via the `cc` and `cmake` Rust build scripts (`build.rs`).
* **Universal C-ABI Export:** All FFI export boundaries conform strictly to the standard C-ABI (`#[no_mangle] pub extern "C"`), avoiding Rust-specific data layout assumptions across language boundaries.

---

## 5. Dual-Engine Storage & Data Flow Architecture

ChakraDB resolves the tension between fast writes and fast analytical scans by decoupling mutations from state presentation.


```

Incoming Write (INSERT/UPDATE/DELETE)
│
▼
┌──────────────────────────┐
│  Speedb LSM Engine (WAL) │  ──> Fast O(1) Local Mutation Log
└─────────────┬────────────┘
│
▼
┌──────────────────────────┐
│ Durability Watermark (W) │  ──> Background Flush / Compaction
└─────────────┬────────────┘
│
▼
┌──────────────────────────┐
│   Apache Arrow Hot Tier  │  ──> Memory-Mapped IPC Slices
└─────────────┬────────────┘
│
▼
┌──────────────────────────┐
│  Apache Iceberg / Parquet│  ──> Durable Cold Disk Storage
└──────────────────────────┘

```

### 5.1 Write Path Workflow ($O(1)$ Ingestion)
1. An incoming write request is received by the engine.
2. The transaction assigns a monotonically increasing **Log Sequence Number (LSN)** to the record.
3. The row is appended to the Speedb/RocksDB LSM Write-Ahead Log (WAL) and primary key MemTable.
4. The write operation returns an immediate `OK` acknowledgment to the client without blocking for columnar materialization or disk serialization.

### 5.2 Read Path Workflow (Merge-on-Read Snapshot Consistency)
When an analytical scan request is issued against snapshot $LSN = S$:
1. **Columnar Hot Scan:** The execution engine scans the Apache Arrow memory-mapped IPC buffers containing structured data up to the Durability Watermark $W$.
2. **Delta Extraction:** The engine queries the Speedb LSM index for all mutations with $LSN$ in the range $W < LSN \le S$.
3. **Merge-on-Read:** An in-memory vector kernel applies the active LSM mutations (updates, tombstones) over the Arrow RecordBatches, outputting a deduplicated, snapshot-consistent `ArrowArrayStream`.

---

## 6. Zero-Copy Language Interoperability (Polars, Pandas, Java)

### 6.1 Python Integration Specification (PyO3 + Arrow PyCapsule)
To achieve $0\text{ms}$ memory transfer with Polars and Pandas 2.0+, ChakraDB implements the **Arrow PyCapsule Interface**.

#### Python Import Workflow (Zero-Copy Ingestion)
```python
import pandas as pd
import polars as pl
from chakradb import Engine

db = Engine()

# 1. User holds a Polars DataFrame in memory
df_polars = pl.DataFrame({"id": [1, 2, 3], "val": [99.5, 100.2, 88.7]})

# 2. Extract PyCapsule Arrow C Stream pointer (0ms copy)
capsule = df_polars.__arrow_c_stream__()

# 3. Pass raw memory pointer directly into ChakraDB Rust Core
db.register_stream("sensor_data", capsule)

```

#### Python Export Workflow (Zero-Copy Query Output)

```python
# 1. Execute query in Rust; output returns as PyCapsule C Stream
output_capsule = db.execute_to_capsule("SELECT * FROM sensor_data WHERE val > 90.0")

# 2. Consume pointer directly in Polars or Pandas without copying memory
result_polars = pl.from_arrow(output_capsule)

```

### 6.2 Java Integration Specification (Java 21+ Panama FFM API)

ChakraDB avoids legacy JNI performance penalties by exporting C-ABI functions accessed via Java's **Foreign Function & Memory (FFM) API (`java.lang.foreign`)**.

#### Native C Exports (`src/ffi.rs`)

```rust
use std::ffi::c_char;
use std::os::raw::c_void;

#[no_mangle]
pub extern "C" fn chakradb_engine_init() -> *mut c_void {
    let engine = Box::new(ChakraDBEngine::new());
    Box::into_raw(engine) as *mut c_void
}

#[no_mangle]
pub extern "C" fn chakradb_execute_ffi(
    engine_ptr: *mut c_void,
    sql_ptr: *const c_char,
    out_stream_ptr: *mut c_void,
) -> i32 {
    // Populate FFI_ArrowArrayStream directly in memory shared with JVM off-heap space
    0 // Return Status OK
}

```

---

## 7. PostgreSQL Parser & Optimizer Integration

### 7.1 Parser Pipeline

ChakraDB incorporates the C-based PostgreSQL parser via `libpg_query` (C-bindings). This guarantees that syntax parsing matches standard PostgreSQL grammar.

```
                     SQL String
                         │
                         ▼
             ┌───────────────────────┐
             │ `libpg_query` Parser  │
             └───────────┬───────────┘
                         │
                         ▼
             ┌───────────────────────┐
             │  PostgreSQL AST (JSON)│
             └───────────┬───────────┘
                         │
                         ▼
             ┌───────────────────────┐
             │ ChakraDB AST Trans.   │
             └───────────┬───────────┘
                         │
                         ▼
             ┌───────────────────────┐
             │ DataFusion Logical    │
             │      Query Plan       │
             └───────────────────────┘

```

1. **SQL Ingestion:** The engine receives a raw SQL query string.
2. **Postgres Parse:** `libpg_query::parse()` validates the query and generates a JSON-serialized PostgreSQL Abstract Syntax Tree (AST).
3. **AST Translation:** The ChakraDB AST Translator walks the Postgres AST nodes (e.g., `SelectStmt`, `ResTarget`, `A_Expr`) and converts them into an equivalent **Apache Arrow DataFusion `LogicalPlan**`.
4. **Optimization:** DataFusion applies logical optimization passes (e.g., projection pushdown, predicate pushdown, limit simplification).
5. **Physical Execution:** The physical planner generates vectorized execution tasks over local Arrow memory buffers.

---

## 8. Detailed Core Implementation (Rust Production Code)

Below is a production-ready template implementation for the core Rust library, demonstrating zero-copy PyCapsule ingestion and export.

### 8.1 `Cargo.toml`

```toml
[package]
name = "chakradb_core"
version = "0.1.0"
edition = "2021"

[lib]
name = "chakradb"
crate-type = ["cdylib", "rlib"]

[dependencies]
arrow = { version = "51.0", features = ["ffi", "pyarrow"] }
datafusion = "37.0"
pyo3 = { version = "0.21", features = ["extension-module", "abi3-py38"] }
tokio = { version = "1.35", features = ["full"] }
pg_query = "0.5"
memmap2 = "0.9"

[build-dependencies]
cc = "1.0"

```

### 8.2 `src/lib.rs`

```rust
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use pyo3::exceptions::PyValueError;
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Core ChakraDB Context managing execution states
pub struct ChakraDBEngineContext {
    rt: Runtime,
    df_ctx: datafusion::prelude::SessionContext,
}

impl ChakraDBEngineContext {
    pub fn new() -> Self {
        let rt = Runtime::new().expect("Failed to initialize Tokio runtime");
        let df_ctx = datafusion::prelude::SessionContext::new();
        Self { rt, df_ctx }
    }

    pub fn parse_sql(&self, sql: &str) -> Result<String, String> {
        pg_query::parse(sql)
            .map(|result| result.protobuf_json())
            .map_err(|e| format!("Postgres Parser Error: {:?}", e))
    }
}

/// PyO3 Python Wrapper Module
#[pyclass]
pub struct Engine {
    ctx: Arc<ChakraDBEngineContext>,
}

#[pymethods]
impl Engine {
    #[new]
    fn new() -> Self {
        Engine {
            ctx: Arc::new(ChakraDBEngineContext::new()),
        }
    }

    /// Parse Postgres SQL string into raw AST JSON
    fn parse_postgres_ast(&self, sql: &str) -> PyResult<String> {
        self.ctx
            .parse_sql(sql)
            .map_err(|e| PyErr::new::<PyValueError, _>(e))
    }

    /// Ingest data ZERO-COPY from Polars/Pandas via Arrow PyCapsule
    fn register_stream(&self, name: &str, capsule: &Bound<'_, PyCapsule>) -> PyResult<String> {
        let stream_ptr = capsule.pointer() as *mut arrow::ffi_stream::FFI_ArrowArrayStream;

        if stream_ptr.is_null() {
            return Err(PyErr::new::<PyValueError, _>(
                "Null pointer provided in Arrow PyCapsule",
            ));
        }

        // Import C Stream into Rust Arrow Reader without memory duplication
        let reader = unsafe {
            ArrowArrayStreamReader::from_raw(stream_ptr)
                .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?
        };

        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Memory-register Arrow MemTable into DataFusion engine context
        let provider = datafusion::datasource::MemTable::try_new(schema, vec![batches])
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;

        self.ctx
            .df_ctx
            .register_table(name, Arc::new(provider))
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;

        Ok(format!(
            "Table '{}' registered successfully with {} rows (Zero-Copy).",
            name, total_rows
        ))
    }

    /// Export query results ZERO-COPY as a PyCapsule Stream to Polars/Pandas
    fn execute_to_capsule<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let ctx = self.ctx.clone();
        let sql_str = sql.to_string();

        // Run execution within Tokio runtime
        let batches = ctx
            .rt
            .block_on(async move {
                let df = ctx.df_ctx.sql(&sql_str).await?;
                df.collect().await
            })
            .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;

        if batches.is_empty() {
            return Err(PyErr::new::<PyValueError, _>("Query returned no records"));
        }

        let schema = batches[0].schema();
        let batch_reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        // Package Rust execution output into Arrow C Stream FFI structure
        let ffi_stream = arrow::ffi_stream::FFI_ArrowArrayStream::new(Box::new(batch_reader));

        let capsule_name =
            std::ffi::CString::new("arrow_array_stream").expect("CString creation failed");
        PyCapsule::new_bound(py, ffi_stream, Some(capsule_name))
    }
}

/// PyO3 Module Export
#[pymodule]
fn chakradb(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Engine>()?;
    Ok(())
}

// -------------------------------------------------------------------
// C-ABI Export Layer for Java Panama (FFM) and C/C++ Integration
// -------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn chakradb_c_engine_create() -> *mut ChakraDBEngineContext {
    Box::into_raw(Box::new(ChakraDBEngineContext::new()))
}

#[no_mangle]
pub extern "C" fn chakradb_c_engine_free(ctx_ptr: *mut ChakraDBEngineContext) {
    if !ctx_ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(ctx_ptr);
        }
    }
}

```

---

## 9. Verification & Performance Validation Matrix

To validate the non-functional requirements and platform agnosticism, the build pipeline must run the following verification suite:

```
                  ┌────────────────────────────────────────┐
                  │      GitHub Actions CI Test Matrix     │
                  └───────────────────┬────────────────────┘
                                      │
         ┌────────────────────────────┼────────────────────────────┐
         ▼                            ▼                            ▼
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│ Ubuntu 24.04    │          │ macOS 14 (M-Series)│        │ Windows Server  │
│ (x86_64 / GCC)  │          │ (aarch64 / Clang)  │        │ (x86_64 MSVC)   │
└────────┬────────┘          └────────┬────────┘          └────────┬────────┘
         │                            │                            │
         ▼                            ▼                            ▼
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│ pytest (Polars) │          │ pytest (Pandas) │          │ JUnit 5 (Java)  │
│ Valgrind / ASAN │          │ Cargo Bench     │          │ Memory Leaks    │
└─────────────────┘          └─────────────────┘          └─────────────────┘

```

### 9.1 Verification Test Cases

| Test Case | Objective | Target Standard |
| --- | --- | --- |
| **TC-01: Zero-Copy Verification** | Verify memory address pointers match between Polars DataFrame and Rust Arrow struct. | Address of `ArrowArray` in Rust MUST equal address generated in Python `__arrow_c_stream__()`. |
| **TC-02: PostgreSQL Syntax Coverage** | Validate parsing of complex queries containing window functions, CTEs, and lateral joins. | `libpg_query` AST generation returns `0` error code on 100% of TPC-DS standard queries. |
| **TC-03: Cross-Platform Compilation** | Verify clean compilation across x86_64 Linux, ARM64 macOS, and MSVC Windows. | `cargo build --target <triple>` succeeds without warnings across all three targets. |
| **TC-04: Concurrency & Write Stalls** | Execute high-throughput concurrent `INSERT` operations while running an analytical scan loop. | LSM layer absorbs $10^6\text{ rows/sec}$; read latency standard deviation stays below $5\text{ms}$. |

```

```
