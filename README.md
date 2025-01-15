# UltraSQL

UltraSQL is a minimalist database system built in Go, designed to be lightweight, fast, and easy to use. It provides file-based storage, a custom query engine, and extendable indexing, making it ideal for learners and small-scale applications.

![Go](https://img.shields.io/badge/go-v1.19-blue) ![License](https://img.shields.io/badge/license-MIT-green) ![Build](https://img.shields.io/github/actions/workflow/status/Anthony4m/ultraSQL/build.yml?branch=main) ![Go Report Card](https://goreportcard.com/badge/github.com/Anthony4m/ultraSQL)

---

## Key Features
- **File-based Storage**: Minimalistic, efficient file handling for data persistence.
- **Custom Query Engine**: Support for basic SQL-like queries.
- **Extendable Indexing**: Flexible indexing mechanisms for optimizing queries.
- **Transaction Management**: Ensures data consistency with ACID properties.
- **Built-in Logging**: Debugging and performance tracking tools.

---

## Why UltraSQL?
- Lightweight and ideal for embedded use cases.
- Perfect for learning database internals.
- Built in Go for simplicity, extensibility, and high performance.

---

## Getting Started

### Prerequisites
- **Go** (version 1.19 or later).
- A basic understanding of Go programming and database concepts.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Anthony4m/ultraSQL.git
   cd ultraSQL
   ```
2. Install dependencies:
   ```bash
   go mod tidy
   ```

### Usage
To run the application:
```bash
go run main.go
```

To test the implementation:
```bash
go test ./...
```

---

## Example Usage
```go
package main
import "ultrasql"

func main() {
    db := ultrasql.New()
    
    // Create table
    db.CreateTable("users", []string{"id", "name"})
    
    // Insert data
    db.Insert("users", []interface{}{1, "John Doe"})
    db.Insert("users", []interface{}{2, "Jane Smith"})
    
    // Query data
    rows := db.Query("SELECT * FROM users WHERE id = 1")
    fmt.Println(rows)
    
    // Transaction Example
    tx := db.BeginTransaction()
    tx.Insert("users", []interface{}{3, "Alice Johnson"})
    tx.Commit()
}
```

---

## Directory Structure
```plaintext
.
├── kfile/                 # Core file handling and storage management
├── log/                   # Logging utilities
├── mydb/                  # Core database engine and operations
├── utils/                 # Shared utility functions and helpers
├── .gitignore             # Specifies files ignored by Git
├── README.md              # Project overview and usage instructions
├── go.mod                 # Go module dependencies
└── main.go                # Main entry point for running the application
```

---

## Use Cases
- **Learning Tool**: Understand database internals and Go development.
- **Prototyping**: Quickly build data-driven applications.
- **Embedded Database**: Lightweight storage solution for small-scale projects.

---

## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a feature branch: `git checkout -b feature-name`.
3. Commit your changes: `git commit -m "Add feature-name"`.
4. Push to the branch: `git push origin feature-name`.
5. Open a pull request.

---

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Acknowledgments
- Inspired by lightweight database implementations.
- Special thanks to the Go community for their invaluable resources.
