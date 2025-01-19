package database

import (
	"atomixDB/database/helper"
	"bufio"
	"fmt"
	"strings"
)

type Command func(scanner *bufio.Reader, db *DB, currentTX *DBTX)

type GetRequest struct {
	tableName string
	cols      []string
	vals      []string
	response  chan GetResponse
}

type GetResponse struct {
	record Record
	found  bool
	err    error
}

func RegisterCommands() map[string]Command {
	return map[string]Command{
		"create": HandleCreate,
		"insert": HandleInsert,
		"delete": HandleDelete,
		"get":    HandleGet,
		"update": HandleUpdate,
		"begin":  func(scanner *bufio.Reader, db *DB, currentTX *DBTX) {},
		"abort":  func(scanner *bufio.Reader, db *DB, currentTX *DBTX) {},
		"commit": func(scanner *bufio.Reader, db *DB, currentTX *DBTX) {},
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
	if currentTX != nil {
		if err := db.TableNew(tdef, &writer); err != nil {
			fmt.Println("Error creating table: ", err)
		} else {
			fmt.Printf("Table '%s' created successfully.\n", td.Name)
		}
	} else {
		db.kv.Begin(&writer)
		if err := db.TableNew(tdef, &writer); err != nil {
			db.kv.Abort(&writer)
			fmt.Println("Error creating table: ", err)
		} else {
			db.kv.Commit(&writer)
			fmt.Printf("Table '%s' created successfully.\n", td.Name)
		}
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
	db.kv.BeginRead(&reader)
	tdef := GetTableDef(db, tableName, &reader.Tree)
	db.kv.EndRead(&reader)
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
		db.kv.Begin(&writer)
		if inserted, err := db.Insert(tableName, rec, &writer); err != nil {
			db.kv.Abort(&writer)
			fmt.Println("Failed to insert: ", err.Error())
		} else if inserted {
			db.kv.Commit(&writer)
			fmt.Println("Record inserted successfully.")
		} else {
			db.kv.Abort(&writer)
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
	db.kv.BeginRead(&reader)
	tdef := GetTableDef(db, tableName, &reader.Tree)
	db.kv.EndRead(&reader)
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
		db.kv.Begin(&writer)
		if deleted, err := db.Delete(tableName, rec, &writer); err != nil {
			fmt.Println("Failed to delete: ", err.Error())
		} else if deleted {
			db.kv.Commit(&writer)
			fmt.Println("Record deleted successfully.")
		} else {
			db.kv.Abort(&writer)
			fmt.Println("Failed to delete record.")
		}
	}
}

func HandleGet(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	responseChan := make(chan GetResponse, 1)

	tableName := helper.GetTableName(scanner)

	fmt.Printf("Enter primary key or index col: ")
	colStr, _ := scanner.ReadString('\n')
	colStr = strings.TrimSpace(colStr)
	splitCol := strings.Split(colStr, ",")

	vals := make([]string, 0)
	for _, col := range splitCol {
		fmt.Printf("Enter value for col %s: ", col)
		valStr, _ := scanner.ReadString('\n')
		vals = append(vals, strings.TrimSpace(valStr))
	}

	db.pool.Submit(func() {
		req := GetRequest{
			tableName: tableName,
			cols:      splitCol,
			vals:      vals,
			response:  responseChan,
		}
		processGetRequest(req, db)
	})

	response := <-responseChan
	if response.err != nil {
		fmt.Println("Error:", response.err)
		return
	}
	if response.found {
		printRecord(response.record)
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
	db.kv.BeginRead(&reader)
	tdef := GetTableDef(db, tableName, &reader.Tree)
	db.kv.EndRead(&reader)

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
		db.kv.Begin(&writer)
		if updated, err := db.Update(tableName, rec, &writer); err != nil {
			db.kv.Abort(&writer)
			fmt.Println("Error while updating: ", err.Error())
		} else if updated {
			db.kv.Commit(&writer)
			printRecord(rec)
		} else {
			db.kv.Abort(&writer)
			fmt.Println("Failed to update record.")
		}
	}
}

func HandleBegin(scanner *bufio.Reader, db *DB, currentTX *DBTX) *DBTX {
	if currentTX != nil {
		fmt.Println("Transaction already in progress. Commit or abort the current transaction before starting a new one.")
		return currentTX
	}

	tx := &DBTX{}
	db.Begin(tx)
	fmt.Println("Transaction started.")
	return tx
}

func HandleCommit(scanner *bufio.Reader, db *DB, currentTX *DBTX) *DBTX {
	if currentTX == nil {
		fmt.Println("No active transaction to commit.")
		return nil
	}

	if err := db.Commit(currentTX); err != nil {
		fmt.Printf("Failed to commit transaction: %v\n", err)
		return currentTX
	}

	fmt.Println("Transaction committed successfully.")
	return nil
}

func HandleAbort(scanner *bufio.Reader, db *DB, currentTX *DBTX) *DBTX {
	if currentTX == nil {
		fmt.Println("No active transaction to abort.")
		return nil
	}

	db.Abort(currentTX)
	fmt.Println("Transaction aborted.")
	return nil
}

func processGetRequest(req GetRequest, db *DB) {
	var reader KVReader
	db.kv.BeginRead(&reader)
	tdef := GetTableDef(db, req.tableName, &reader.Tree)
	db.kv.EndRead(&reader)

	if tdef == nil {
		req.response <- GetResponse{err: fmt.Errorf("table '%s' not found", req.tableName)}
		return
	}

	if err := verifyColumns(tdef, req.cols); err != nil {
		req.response <- GetResponse{
			record: Record{},
			found:  false,
			err:    err,
		}
		return
	}
	rec := Record{
		Cols: make([]string, len(req.cols)),
		Vals: make([]Value, len(req.cols)),
	}

	for i, col := range req.cols {
		idx := ColIndex(tdef, col)
		if tdef.Types[idx] == TYPE_BYTES {
			rec.Vals[i] = Value{Type: TYPE_BYTES, Str: []byte(req.vals[i])}
		} else {
			var key int64
			fmt.Sscanf(req.vals[i], "%d", &key)
			rec.Vals[i] = Value{Type: TYPE_INT64, I64: key}
		}
		rec.Cols[i] = col
	}

	var kvReader KVReader
	defer db.kv.EndRead(&kvReader)
	db.kv.BeginRead(&kvReader)

	found, err := db.Get(req.tableName, &rec, &kvReader)
	req.response <- GetResponse{
		record: rec,
		found:  found,
		err:    err,
	}
}

func verifyColumns(tableDef *TableDef, userCols []string) error {
	colMap := make(map[string]bool)
	for _, col := range tableDef.Cols {
		colMap[col] = true
	}

	var invalidCols []string
	for _, col := range userCols {
		if !colMap[col] {
			invalidCols = append(invalidCols, col)
		}
	}

	if len(invalidCols) > 0 {
		return fmt.Errorf("invalid columns: %v", invalidCols)
	}
	return nil
}
