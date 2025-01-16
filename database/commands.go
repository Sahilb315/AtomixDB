package database

import (
	"atomixDB/database/helper"
	"bufio"
	"fmt"
	"strings"
)

type Command func(scanner *bufio.Reader, db *DB, currentTX *DBTX)

func RegisterCommands() map[string]Command {
	return map[string]Command{
		"CREATE": HandleCreate,
		"INSERT": HandleInsert,
		"DELETE": HandleDelete,
		"GET":    HandleGet,
		"UPDATE": HandleUpdate,
		"BEGIN":  HandleBegin,
		"ABORT":  HandleAbort,
		"COMMIT": HandleCommit,
	}
}

func HandleCreate(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	td := helper.GetTableInput(scanner)
	var writer KVTX
	tdef := &TableDef{
		Name:        td.Name,
		Cols:        td.Cols,
		Types:       td.Types,
		Indexes:     td.Indexes,
		PKeys:       1,
		IndexPrefix: make([]uint32, 0),
	}
	db.KV.Begin(&writer)
	if err := db.TableNew(tdef, &writer); err != nil {
		db.KV.Abort(&writer)
		fmt.Println("Error creating table: ", err)
	} else {
		db.KV.Commit(&writer)
		fmt.Printf("Table '%s' created successfully.\n", td.Name)
	}
}

func HandleInsert(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	tableName := helper.GetTableName(scanner)

	rec := Record{
		Cols: []string{},
		Vals: []Value{},
	}

	var writer KVTX
	var reader KVReader
	db.KV.BeginRead(&reader)
	tdef := GetTableDef(db, tableName, &reader.Tree)
	db.KV.EndRead(&reader)
	if tdef == nil {
		fmt.Printf("Table '%s' not found.\n", tableName)
		return
	}

	for i, col := range tdef.Cols {
		fmt.Printf("Enter value for %s: ", col)
		valStr, _ := scanner.ReadString('\n')
		valStr = strings.TrimSpace(valStr)

		var val Value
		if tdef.Types[i] == TYPE_BYTES {
			val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
		} else {
			var key int64
			fmt.Sscanf(valStr, "%d", &key)
			val = Value{Type: TYPE_INT64, I64: key}
		}
		rec.Cols = append(rec.Cols, col)
		rec.Vals = append(rec.Vals, val)
	}
	if currentTX != nil {
		if inserted, err := currentTX.Set(tableName, rec, MODE_INSERT_ONLY); err != nil {
			fmt.Println("Failed to insert: ", err.Error())
		} else if inserted {
			fmt.Println("Record inserted successfully.")
		} else {
			fmt.Println("Failed to insert record.")
		}
	} else {
		db.KV.Begin(&writer)
		if inserted, err := db.Insert(tableName, rec, &writer); err != nil {
			db.KV.Abort(&writer)
			fmt.Println("Failed to insert: ", err.Error())
		} else if inserted {
			db.KV.Commit(&writer)
			fmt.Println("Record inserted successfully.")
		} else {
			db.KV.Abort(&writer)
			fmt.Println("Failed to insert record.")
		}
	}
}

func HandleDelete(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	tableName := helper.GetTableName(scanner)
	rec := Record{
		Cols: []string{},
		Vals: []Value{},
	}

	var writer KVTX
	var reader KVReader
	db.KV.BeginRead(&reader)
	tdef := GetTableDef(db, tableName, &reader.Tree)
	db.KV.EndRead(&reader)
	if tdef == nil {
		fmt.Printf("Table '%s' not found.\n", tableName)
		return
	}

	for i, col := range tdef.Cols[:tdef.PKeys] {
		fmt.Printf("Enter value for %s (primary key): ", col)
		valStr, _ := scanner.ReadString('\n')
		valStr = strings.TrimSpace(valStr)

		var val Value
		if tdef.Types[i] == TYPE_BYTES {
			val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
		} else {
			var key int64
			fmt.Sscanf(valStr, "%d", &key)
			val = Value{Type: TYPE_INT64, I64: key}
		}
		rec.Cols = append(rec.Cols, col)
		rec.Vals = append(rec.Vals, val)
	}

	if currentTX != nil {
		if deleted, err := currentTX.Delete(tableName, rec); err != nil {
			fmt.Println("Failed to delete: ", err.Error())
		} else if deleted {
			fmt.Println("Record deleted successfully.")
		} else {
			fmt.Println("Failed to delete record.")
		}
	} else {
		db.KV.Begin(&writer)
		if deleted, err := db.Delete(tableName, rec, &writer); err != nil {
			fmt.Println("Failed to delete: ", err.Error())
		} else if deleted {
			db.KV.Commit(&writer)
			fmt.Println("Record deleted successfully.")
		} else {
			db.KV.Abort(&writer)
			fmt.Println("Failed to delete record.")
		}
	}
}

func HandleGet(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	tableName := helper.GetTableName(scanner)
	rec := Record{
		Cols: []string{},
		Vals: []Value{},
	}
	var reader KVReader
	db.KV.BeginRead(&reader)
	tdef := GetTableDef(db, tableName, &reader.Tree)
	db.KV.EndRead(&reader)
	if tdef == nil {
		fmt.Printf("Table '%s' not found.\n", tableName)
		return
	}
	fmt.Printf("Enter primary key or index col: ")
	colStr, _ := scanner.ReadString('\n')
	colStr = strings.TrimSpace(colStr)

	splitCol := strings.Split(colStr, ",")
	if len(splitCol) > 1 {
		for _, col := range splitCol {
			fmt.Printf("Enter value for col %s: ", col)
			valStr, _ := scanner.ReadString('\n')
			valStr = strings.TrimSpace(valStr)
			var val Value
			idx := ColIndex(tdef, colStr)
			if tdef.Types[idx] == TYPE_BYTES {
				val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
			} else {
				var key int64
				fmt.Sscanf(valStr, "%d", &key)
				val = Value{Type: TYPE_INT64, I64: key}
			}
			rec.Cols = append(rec.Cols, col)
			rec.Vals = append(rec.Vals, val)
		}
	} else {
		fmt.Printf("Enter value for col %s: ", colStr)
		valStr, _ := scanner.ReadString('\n')
		valStr = strings.TrimSpace(valStr)
		var val Value
		idx := ColIndex(tdef, colStr)
		if tdef.Types[idx] == TYPE_BYTES {
			val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
		} else {
			var key int64
			fmt.Sscanf(valStr, "%d", &key)
			val = Value{Type: TYPE_INT64, I64: key}
		}
		rec.Cols = append(rec.Cols, colStr)
		rec.Vals = append(rec.Vals, val)
	}
	resultChan := db.ConcurrentRead(tableName, rec)
	result := <-resultChan
	if result.Found {
		printRecord(result.Record)
	} else if result.Error != nil {
		fmt.Println("Error while retrieveing value: ", result.Error.Error())
	} else {
		fmt.Println("Record not found")
	}
}

func HandleUpdate(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	tableName := helper.GetTableName(scanner)

	rec := Record{
		Cols: []string{},
		Vals: []Value{},
	}

	var writer KVTX
	var reader KVReader
	db.KV.BeginRead(&reader)
	tdef := GetTableDef(db, tableName, &reader.Tree)
	db.KV.EndRead(&reader)

	if tdef == nil {
		fmt.Printf("Table '%s' not found.\n", tableName)
		return
	}

	for i, col := range tdef.Cols {
		if i == 0 {
			fmt.Printf("Enter primary key for %s: ", col)
		} else {
			fmt.Printf("Enter value for %s: ", col)
		}
		valStr, _ := scanner.ReadString('\n')
		valStr = strings.TrimSpace(valStr)

		var val Value
		if tdef.Types[i] == TYPE_BYTES {
			val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
		} else {
			var key int64
			fmt.Sscanf(valStr, "%d", &key)
			val = Value{Type: TYPE_INT64, I64: key}
		}
		rec.Cols = append(rec.Cols, col)
		rec.Vals = append(rec.Vals, val)
	}
	if currentTX != nil {
		if updated, err := currentTX.Set(tableName, rec, MODE_UPDATE_ONLY); err != nil {
			fmt.Println("Error while updating: ", err.Error())
		} else if updated {
			printRecord(rec)
		} else {
			fmt.Println("Failed to update record.")
		}
	} else {
		db.KV.Begin(&writer)
		if updated, err := db.Update(tableName, rec, &writer); err != nil {
			db.KV.Abort(&writer)
			fmt.Println("Error while updating: ", err.Error())
		} else if updated {
			db.KV.Commit(&writer)
			printRecord(rec)
		} else {
			db.KV.Abort(&writer)
			fmt.Println("Failed to update record.")
		}
	}
}

func HandleBegin(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	if currentTX != nil {
		fmt.Println("Transaction already in progress. Commit or abort the current transaction before starting a new one.")
		return
	}
	currentTX = &DBTX{}
	db.Begin(currentTX)
	fmt.Println("Transaction started.")
}

func HandleCommit(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	if currentTX == nil {
		fmt.Println("No active transaction to commit.")
		return
	}
	if err := db.Commit(currentTX); err != nil {
		fmt.Printf("Failed to commit transaction: %v\n", err)
	} else {
		fmt.Println("Transaction committed successfully.")
	}
	currentTX = nil
}

func HandleAbort(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	if currentTX == nil {
		fmt.Println("No active transaction to abort.")
		return
	}
	db.Abort(currentTX)
	fmt.Println("Transaction aborted.")
	currentTX = nil
}
