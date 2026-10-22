# UltraSQL

UltraSQL is the storage engine of a database, built from scratch in Go to learn how databases work underneath SQL: how bytes are laid out on disk, how pages are cached in memory, and how transactions survive a crash.

![Go](https://img.shields.io/badge/go-v1.19-blue) ![License](https://img.shields.io/badge/license-MIT-green)

It is a learning project, not a database you can query yet. There is no SQL parser, query engine or index. Those would sit on top of the layers below.

---

## What's built

| Layer | Package | What it does |
|---|---|---|
| File manager | `kfile/` | Reads and writes fixed-size blocks to database files |
| Slotted pages | `kfile/` | Lays out variable-length records (cells) inside a block, with a slot directory so records can move without breaking references |
| Buffer pool | `buffer/` | Caches blocks in memory, pins and unpins them, and evicts with the clock algorithm |
| Log manager | `log/`, `log_record/` | Append-only write-ahead log: start, commit, rollback, checkpoint and update records |
| Recovery | `recovery/` | Rollback scans the log backwards and undoes a transaction's updates; crash recovery undoes every transaction that never committed, stopping at a checkpoint |
| Concurrency | `concurrency/` | A lock table with shared and exclusive locks per block |
| Transactions | `transaction/` | Ties it together: pin blocks, insert and find cells, commit, roll back, recover |

---

## Writing about it

I wrote up each layer as I built it, on [dev.to/just_kojo](https://dev.to/just_kojo):

- [A Deep Dive into Database Architecture: Building the Backbone of Modern Systems](https://dev.to/just_kojo/a-deep-dive-into-database-architecture-building-the-backbone-of-modern-systems-3o3)
- [Database Internals: Building a File Manager in Go from Scratch](https://dev.to/just_kojo/database-internals-building-a-file-manager-in-go-from-scratch-573)
- [Building a Go Database Page Management System](https://dev.to/just_kojo/building-a-go-database-page-management-system-a-deep-dive-into-efficient-data-storage-3mfm)
- [TOAST: PostgreSQL's Secret Sauce for Handling Big Data](https://dev.to/just_kojo/toast-postgresqls-secret-sauce-for-handling-big-data-50mm)
- [Dive Into Slotted Pages: Building Efficient Record Management in Go](https://dev.to/just_kojo/dive-into-slotted-pages-building-efficient-record-management-in-go-4aeg)

---

## Running it

Requires Go 1.19 or later.

```bash
git clone https://github.com/Anthony4m/ultraSQL.git
cd ultraSQL
go run main.go          # writes a slotted page to a block and reads it back
go test ./kfile/...     # file manager and slotted page tests
```

`main.go` is a small demo of the storage layer: it appends a block, writes an int, a string, a date and a bool into a slotted page, writes the page to disk, and reads it back.

---

## Status

- The storage layers above are in place.
- Some tests still refer to an older LRU eviction policy that was replaced by the clock algorithm, so `go test ./...` does not fully pass yet. `kfile` tests pass.
- Not started: SQL parsing, query planning and execution, indexes.

---

## Directory structure

```plaintext
.
├── kfile/          # file manager, blocks, slotted pages, cells
├── buffer/         # buffer pool and clock eviction
├── log/            # write-ahead log manager
├── log_record/     # log record types (start, commit, rollback, checkpoint, update)
├── recovery/       # rollback and crash recovery
├── concurrency/    # lock table, shared and exclusive locks
├── transaction/    # transaction manager
├── txinterface/    # transaction interface shared by recovery and log records
├── utils/          # iterators and helpers
└── main.go         # storage-layer demo
```

---

## License

MIT. See [LICENSE](LICENSE).
