An embedded, tiny, fast, on-disk, concurrent key-value store using b+trees.

A database is stored in a single file.

On most POSIX systems, multiple processes can safely access a single file.

Supports put(key,value), get(key), and delete(key).

Usage examples can be found in main()