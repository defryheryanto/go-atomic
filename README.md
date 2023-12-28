# go-atomic
Go Atomic is a Go library that simplifies and ensures atomicity across database operations between service layers. It provides a convenient and reliable way to handle transactions and ensure the integrity of your database operations.

## Features
1. Atomicity Operation -> Ensure that a series of database operations either complete successfully or leave the database unchanged.
2. Database Agnostic -> Works seamlessly with popular databases like MySQL, PostgreSQL, SQLite, and more.
3. Seamless -> If you're using this in your service layer, it will not pollute your service layer for database implementation

## Installation
To install the library, just run this command and use this in your go application
```
go get github.com/defryheryanto/go-atomic
```

## Examples
See [Examples](https://github.com/defryheryanto/go-atomic/example) for detailed implementation
