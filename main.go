package main

import (
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "log"
)

func main() {
    log.Printf("Opening SQLite file %s\n", DATABASE_PATH)
    tempDB, err := sql.Open("sqlite3", DATABASE_PATH)
    if err != nil {
        log.Printf("main.go: failed to open database: %v\n", err)
        return
    }
    db := Database{tempDB}
    defer db.Close()
    log.Printf("Successfully opened database %s\n", DATABASE_PATH)
}