# AwesomeDB

This repository implements a lightweight database system in Go.

## Getting Started

### Prerequisites
- **Go** (version 1.19 or later)
- A working knowledge of database systems and basic Go programming.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Anthony4m/awesomeDB.git
   cd awesomeDB
   ```
2. Install dependencies (if any):
   ```bash
   go mod tidy
   ```

### Usage
- To run the application:
   ```bash
   go run main.go
   ```
- To test the implementation:
   ```bash
   go test ./...
   ```

---

## Directory Structure
```plaintext
.
├── file/                   # File management components (FileManager, BlockId)
├── page/                   # Page management components (Page, PageManager)
├── allocator/              # Page allocator implementation
├── buffer/                 # Page buffer implementation
├── record/                 # RecordPage and related utilities
├── benchmarks/             # Performance benchmarks
├── tests/                  # Test cases for all components
├── main.go                 # Entry point for the application
└── README.md               # This file
```

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
- **Edward Sciore** for *Database Design and Implementation*, which inspired this project.
- The open-source Go community for guidance and tools.
```

Feel free to modify this to better suit your repository’s specific details and goals. Let me know if you'd like further adjustments!
