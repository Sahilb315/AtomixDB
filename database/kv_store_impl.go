package database

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func newKV(filename string) *KV {
	return &KV{
		Path: filename,
	}
}

func newDB() *DB {
	return &DB{
		Path:   "database.db",
		kv:     *newKV("database.db"),
		tables: make(map[string]*TableDef),
	}
}

func initializeInternalTables(db *DB) error {
	fmt.Println("Creating TDEF_META...")
	if err := db.TableNew(TDEF_META); err != nil {
		if !strings.Contains(err.Error(), "table exists") {
			return fmt.Errorf("failed to create TDEF_META: %v", err)
		}
		fmt.Println("TDEF_META already exists")
	}
	fmt.Println("Creating TDEF_TABLE...")
	if err := db.TableNew(TDEF_TABLE); err != nil {
		if !strings.Contains(err.Error(), "table exists") {
			return fmt.Errorf("failed to create TDEF_TABLE: %v", err)
		}
		fmt.Println("TDEF_TABLE already exists")
	}
	return nil
}

func StoreImpl() {
	scanner := bufio.NewReader(os.Stdin)
	db := newDB() // Initialize the database
	if err := db.kv.Open(); err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	err := initializeInternalTables(db)
	if err != nil {
		fmt.Println("Error while init table: ", err)
		os.Exit(0)
	}
	fmt.Println("Welcome to AtomixDB")
	fmt.Println("Available Commands:")
	fmt.Println("  CREATE       - Create a new table")
	fmt.Println("  INSERT       - Add a record to a table")
	fmt.Println("  DELETE       - Delete a record from a table")
	fmt.Println("  GET          - Retrieve a record from a table")
	fmt.Println("  UPDATE       - Update a record in a table")
	fmt.Println("  LIST TABLES  - List all tables")
	fmt.Println("  EXIT         - Exit the program")
	fmt.Println()
	for {
		fmt.Print("> ") // Display prompt
		line, _, err := scanner.ReadLine()
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		command := strings.TrimSpace(string(line))
		switch command {
		case "CREATE":
			fmt.Print("Enter table name: ")
			name, _ := scanner.ReadString('\n')
			name = strings.TrimSpace(name)

			fmt.Print("Enter column names (comma-separated): ")
			colsInput, _ := scanner.ReadString('\n')
			colsInput = strings.TrimSpace(colsInput)
			cols := strings.Split(colsInput, ",")

			fmt.Print("Enter column types (comma-separated as numbers): ")
			typesInput, _ := scanner.ReadString('\n')
			typesInput = strings.TrimSpace(typesInput)
			typesStr := strings.Split(typesInput, ",")
			types := make([]uint32, len(typesStr))
			for i, t := range typesStr {
				var typeValue uint32
				fmt.Sscanf(t, "%d", &typeValue)
				types[i] = typeValue
			}

			tdef := &TableDef{
				Name:        name,
				Cols:        cols,
				Types:       types,
				PKeys:       1,
				Indexes:     make([][]string, 0),
				IndexPrefix: make([]uint32, 0),
			}
			if err := db.TableNew(tdef); err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("Table '%s' created successfully.\n", name)
			}

		case "INSERT":
			fmt.Print("Enter table name: ")
			tableName, _ := scanner.ReadString('\n')
			tableName = strings.TrimSpace(tableName)

			rec := Record{
				Cols: []string{},
				Vals: []Value{},
			}

			tdef := getTableDef(db, tableName)
			if tdef == nil {
				fmt.Printf("Table '%s' not found.\n", tableName)
				continue
			}

			for _, col := range tdef.Cols {
				fmt.Printf("Enter value for %s: ", col)
				valStr, _ := scanner.ReadString('\n')
				valStr = strings.TrimSpace(valStr)

				var val Value
				if col == tdef.Cols[0] { // Assuming first column is primary key (int64)
					var key int64
					fmt.Sscanf(valStr, "%d", &key)
					val = Value{Type: TYPE_INT64, I64: key}
				} else { // Assuming other columns are bytes
					val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
				}
				rec.Cols = append(rec.Cols, col)
				rec.Vals = append(rec.Vals, val)
			}

			if added, err := db.Insert(tableName, rec); err != nil {
				fmt.Println(err)
			} else if added {
				fmt.Println("Record inserted successfully.")
			} else {
				fmt.Println("Failed to insert record.")
			}

		case "DELETE":
			fmt.Print("Enter table name: ")
			tableName, _ := scanner.ReadString('\n')
			tableName = strings.TrimSpace(tableName)

			rec := Record{
				Cols: []string{},
				Vals: []Value{},
			}

			tdef := getTableDef(db, tableName)
			if tdef == nil {
				fmt.Printf("Table '%s' not found.\n", tableName)
				continue
			}

			for _, col := range tdef.Cols[:tdef.PKeys] { // Only ask for primary key(s) for deletion
				fmt.Printf("Enter value for %s (primary key): ", col)
				valStr, _ := scanner.ReadString('\n')
				valStr = strings.TrimSpace(valStr)

				var val Value
				var key int64
				fmt.Sscanf(valStr, "%d", &key)
				val = Value{Type: TYPE_INT64, I64: key}

				rec.Cols = append(rec.Cols, col)
				rec.Vals = append(rec.Vals, val)
			}

			if deleted, err := db.Delete(tableName, rec); err != nil {
				fmt.Println(err)
			} else if deleted {
				fmt.Println("Record deleted successfully.")
			} else {
				fmt.Println("Failed to delete record.")
			}

		case "GET":
			fmt.Print("Enter table name: ")
			tableName, _ := scanner.ReadString('\n')
			tableName = strings.TrimSpace(tableName)

			rec := Record{
				Cols: []string{},
				Vals: []Value{},
			}

			tdef := getTableDef(db, tableName)
			if tdef == nil {
				fmt.Printf("Table '%s' not found.\n", tableName)
				continue
			}

			for _, col := range tdef.Cols[:tdef.PKeys] { // Only ask for primary key(s) for retrieval
				fmt.Printf("Enter value for %s (primary key): ", col)
				valStr, _ := scanner.ReadString('\n')
				valStr = strings.TrimSpace(valStr)

				var val Value
				var key int64
				fmt.Sscanf(valStr, "%d", &key)
				val = Value{Type: TYPE_INT64, I64: key}

				rec.Cols = append(rec.Cols, col)
				rec.Vals = append(rec.Vals, val)
			}

			// Assume Get function retrieves the record based on primary keys.
			foundRec, err := db.Get(tableName, &rec) // You might need to implement this method.
			if err != nil {
				fmt.Println(err)
			} else if foundRec {
				fmt.Printf("Retrieved Record: %v\n", rec) // Print retrieved record
			} else {
				fmt.Println("Record not found.")
			}

		case "UPDATE":
			fmt.Print("Enter table name: ")
			tableName, _ := scanner.ReadString('\n')
			tableName = strings.TrimSpace(tableName)

			rec := Record{
				Cols: []string{},
				Vals: []Value{},
			}

			tdef := getTableDef(db, tableName)
			if tdef == nil {
				fmt.Printf("Table '%s' not found.\n", tableName)
				continue
			}

			for _, col := range tdef.Cols { // Ask for all columns including primary keys and values to update
				fmt.Printf("Enter value for %s (leave blank to skip): ", col)
				valStr, _ := scanner.ReadString('\n')
				valStr = strings.TrimSpace(valStr)

				var val Value
				if col == tdef.Cols[0] { // Assuming first column is primary key (int64)
					var key int64
					fmt.Sscanf(valStr, "%d", &key)
					val = Value{Type: TYPE_INT64, I64: key}
				} else { // Assuming other columns are bytes
					val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
				}

				rec.Cols = append(rec.Cols, col)
				rec.Vals = append(rec.Vals, val)
			}

			if updated, err := db.Update(tableName, rec); err != nil {
				fmt.Println(err)
			} else if updated {
				fmt.Println("Record updated successfully.")
			} else {
				fmt.Println("Failed to update record.")
			}

		case "LIST TABLES":
			fmt.Println("Existing Tables:")
			for name := range db.tables {
				fmt.Println(" -", name)
			}

		case "EXIT":
			db.kv.Close()
			fmt.Println("Exiting...")
			os.Exit(0)

		default:
			fmt.Println("Unknown command:", command)
		}
	}
}
