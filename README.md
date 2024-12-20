# UltraSQL

UltraSQL is a minimalist database system built in Go, designed to be lightweight, fast, and easy to use. It provides file-based storage, a custom query engine, and extendable indexing, making it ideal for learners and small-scale applications.

![Go](https://img.shields.io/badge/go-v1.19-blue) ![License](https://img.shields.io/badge/license-MIT-green)

## Features
- Lightweight and file-based storage
- Custom query engine with basic SQL-like functionality
- Extendable indexing and transaction management
- Built with Go for simplicity and performance

## Getting Started

### Prerequisites
- **Go** (version 1.19 or later)
- A basic understanding of Go programming and database concepts

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Anthony4m/awesomeDB.git
   cd awesomeDB
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

### Example Usage
```go
package main
import "ultrasql"

func main() {
    db := ultrasql.New()
    db.CreateTable("users", []string{"id", "name"})
    db.Insert("users", []interface{}{1, "John Doe"})
    rows := db.Query("SELECT * FROM users WHERE id = 1")
    fmt.Println(rows)
}
```

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

## Use Cases
- Learning database internals and Go development
- Quick prototyping of data-driven applications
- Embedded database for small-scale projects

## Contributing
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a feature branch: `git checkout -b feature-name`.
3. Commit your changes: `git commit -m "Add feature-name"`.
4. Push to the branch: `git push origin feature-name`.
5. Open a pull request.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments
- Inspired by lightweight database implementations
- Special thanks to the Go community for their invaluable resources
