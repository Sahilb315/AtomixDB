package database

import (
	"atomixDB/database/helper"
	"bufio"
	"fmt"
	"strings"
)

type Command func(scanner *bufio.Reader, db *DB, currentTX *DBTX)

type QueryType int

const (
	SingleRecord QueryType = iota
	RangeQuery
	TableScan
)

type QueryRequest struct {
	tableName string
	cols      []string
	startVals []string
	endVals   []string
	queryType QueryType
	response  chan GetResponse
}

type GetResponse struct {
	records []*Record
	found   bool
	err     error
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
		"help": func(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
			helper.PrintWelcomeMessage(false)
		},
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
		var val Value
		isValidInput := false

		for !isValidInput {
			fmt.Printf("Enter value for %s: ", col)
			valStr, _ := scanner.ReadString('\n')
			valStr = strings.TrimSpace(valStr)

			if tdef.Types[i] == TYPE_BYTES {
				val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
				isValidInput = true
			} else if tdef.Types[i] == TYPE_INT64 {
				var key int64
				_, err := fmt.Sscanf(valStr, "%d", &key)
				if err == nil {
					val = Value{Type: TYPE_INT64, I64: key}
					isValidInput = true
				} else {
					fmt.Printf("Invalid input. Please enter again:\n")
				}
			}
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

func HandleGet(scanner *bufio.Reader, db *DB, currentTX *DBTX) {
	responseChan := make(chan GetResponse, 1)
	tableName := helper.GetTableName(scanner)

	fmt.Println("\nSelect query type:")
	fmt.Println("1. Index lookup (primary/secondary index)")
	fmt.Println("2. Range query")
	fmt.Println("3. Column filter")
	var choice string
	for {
		fmt.Print("Enter choice (1, 2 or 3): ")
		choice, _ = scanner.ReadString('\n')
		choice = strings.TrimSpace(choice)
		if choice != "" {
			break
		} else {
			fmt.Println("Please enter a valid choice!")
		}
	}

	queryType := SingleRecord
	switch strings.TrimSpace(choice) {
	case "2":
		queryType = RangeQuery
	case "3":
		queryType = TableScan
	}

	switch queryType {
	case RangeQuery:
		fmt.Print("\nEnter column name for range lookup(index col): ")
		colStr, _ := scanner.ReadString('\n')
		col := strings.TrimSpace(colStr)

		startVals := make([]string, 0, 1)
		endVals := make([]string, 0, 1)

		fmt.Print("\nEnter start range value: ")
		val, _ := scanner.ReadString('\n')
		startVals = append(startVals, strings.TrimSpace(val))

		fmt.Print("\nEnter end range value: ")
		val, _ = scanner.ReadString('\n')
		endVals = append(endVals, strings.TrimSpace(val))

		db.pool.Submit(func() {
			processQueryRequest(QueryRequest{
				tableName: tableName,
				cols:      []string{col},
				startVals: startVals,
				endVals:   endVals,
				queryType: queryType,
				response:  responseChan,
			}, db)
		})
	case SingleRecord:
		fmt.Print("\nEnter index column(s) (comma-separated for composite index): ")
		colStr, _ := scanner.ReadString('\n')
		cols := strings.Split(strings.TrimSpace(colStr), ",")
		for i := range cols {
			cols[i] = strings.TrimSpace(cols[i])
		}

		startVals := make([]string, 0, len(cols))
		for _, col := range cols {
			fmt.Printf("Enter value for %s: ", col)
			val, _ := scanner.ReadString('\n')
			startVals = append(startVals, strings.TrimSpace(val))
		}

		db.pool.Submit(func() {
			processQueryRequest(QueryRequest{
				tableName: tableName,
				cols:      cols,
				startVals: startVals,
				queryType: queryType,
				response:  responseChan,
			}, db)
		})
	default:
		fmt.Print("\nEnter column name for filter: ")
		colStr, _ := scanner.ReadString('\n')
		fmt.Print("Enter values(comma-separated for multiple values): ")
		valStr, _ := scanner.ReadString('\n')

		startVals := strings.Split(strings.TrimSpace(valStr), ",")
		startCols := make([]string, len(startVals))
		for i := range startVals {
			startVals[i] = strings.TrimSpace(startVals[i])
			startCols[i] = strings.TrimSpace(colStr)
		}

		db.pool.Submit(func() {
			processQueryRequest(QueryRequest{
				tableName: tableName,
				cols:      startCols,
				startVals: startVals,
				queryType: queryType,
				response:  responseChan,
			}, db)
		})
	}

	response := <-responseChan
	if response.err != nil {
		fmt.Println("\nError:", response.err)
		return
	}
	if !response.found {
		fmt.Println("\nNo records found")
		return
	}
	printRecords(response.records)
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

	for i, col := range tdef.Cols {
		var val Value
		isValidInput := false

		for !isValidInput {
			fmt.Printf("Enter value for %s: ", col)
			valStr, _ := scanner.ReadString('\n')
			valStr = strings.TrimSpace(valStr)

			if tdef.Types[i] == TYPE_BYTES {
				val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
				isValidInput = true
			} else if tdef.Types[i] == TYPE_INT64 {
				var key int64
				_, err := fmt.Sscanf(valStr, "%d", &key)
				if err == nil {
					val = Value{Type: TYPE_INT64, I64: key}
					isValidInput = true
				} else {
					fmt.Printf("Invalid input. Please enter again:\n")
				}
			}
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
		var val Value
		isValidInput := false

		for !isValidInput {
			valStr, _ := scanner.ReadString('\n')
			valStr = strings.TrimSpace(valStr)

			if tdef.Types[i] == TYPE_BYTES {
				val = Value{Type: TYPE_BYTES, Str: []byte(valStr)}
				isValidInput = true
			} else if tdef.Types[i] == TYPE_INT64 {
				var key int64
				_, err := fmt.Sscanf(valStr, "%d", &key)
				if err == nil {
					val = Value{Type: TYPE_INT64, I64: key}
					isValidInput = true
				} else {
					fmt.Printf("Invalid input. Please enter again: ")
				}
			}
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

func processQueryRequest(req QueryRequest, db *DB) {
	var reader KVReader
	db.kv.BeginRead(&reader)
	defer db.kv.EndRead(&reader)

	tdef := GetTableDef(db, req.tableName, &reader.Tree)
	if tdef == nil {
		req.response <- GetResponse{
			records: nil,
			found:   false,
			err:     fmt.Errorf("table '%s' not found", req.tableName),
		}
		return
	}

	if err := verifyColumns(tdef, req.cols); err != nil {
		req.response <- GetResponse{
			records: nil,
			found:   false,
			err:     err,
		}
		return
	}

	startRecord := Record{
		Cols: make([]string, len(req.cols)),
		Vals: make([]Value, len(req.cols)),
	}

	for i, col := range req.cols {
		idx := ColIndex(tdef, col)
		if tdef.Types[idx] == TYPE_BYTES {
			startRecord.Vals[i] = Value{Type: TYPE_BYTES, Str: []byte(req.startVals[i])}
		} else {
			var key int64
			fmt.Sscanf(req.startVals[i], "%d", &key)
			startRecord.Vals[i] = Value{Type: TYPE_INT64, I64: key}
		}
		startRecord.Cols[i] = col
	}

	if req.queryType == SingleRecord {
		found, err := db.Get(req.tableName, &startRecord, &reader)
		req.response <- GetResponse{
			records: []*Record{&startRecord},
			found:   found,
			err:     err,
		}
		return
	}

	if req.queryType == TableScan {
		results, err := db.QueryWithFilter(req.tableName, tdef, &startRecord)
		if err != nil {
			req.response <- GetResponse{
				records: nil,
				found:   false,
				err:     err,
			}

		}

		req.response <- GetResponse{
			records: results,
			found:   true,
			err:     nil,
		}
		return
	}

	endRecord := Record{
		Cols: make([]string, len(req.cols)),
		Vals: make([]Value, len(req.cols)),
	}
	for i, col := range req.cols {
		idx := ColIndex(tdef, col)
		if tdef.Types[idx] == TYPE_BYTES {
			endRecord.Vals[i] = Value{Type: TYPE_BYTES, Str: []byte(req.endVals[i])}
		} else {
			var key int64
			fmt.Sscanf(req.endVals[i], "%d", &key)
			endRecord.Vals[i] = Value{Type: TYPE_INT64, I64: key}
		}
		endRecord.Cols[i] = col
	}

	records, err := db.GetRange(req.tableName, &startRecord, &endRecord, &reader)
	req.response <- GetResponse{
		records: records,
		found:   len(records) > 0,
		err:     err,
	}
}

func verifyColumns(tdef *TableDef, cols []string) error {
	for _, col := range cols {
		found := false
		for _, tableCol := range tdef.Cols {
			if col == tableCol {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column '%s' not found in table", col)
		}
	}
	return nil
}

func formatValue(v Value) string {
	switch v.Type {
	case 1:
		return fmt.Sprintf("%d", v.I64)
	case 2:
		return string(v.Str)
	default:
		return "Unknown"
	}
}

func printRecord(record Record) {
	if len(record.Cols) == 0 || len(record.Vals) == 0 {
		fmt.Println("Empty record")
		return
	}

	colWidths := make([]int, len(record.Cols))
	for i, col := range record.Cols {
		colWidths[i] = len(col)
		valWidth := len(formatValue(record.Vals[i]))
		if valWidth > colWidths[i] {
			colWidths[i] = valWidth
		}
	}

	fmt.Println(strings.Repeat("-", calculateTotalWidth(colWidths)))
	for i, col := range record.Cols {
		fmt.Printf("| %-*s ", colWidths[i], col)
	}
	fmt.Println("|")
	fmt.Println(strings.Repeat("-", calculateTotalWidth(colWidths)))

	for i, val := range record.Vals {
		fmt.Printf("| %-*s ", colWidths[i], formatValue(val))
	}
	fmt.Println("|")
	fmt.Println(strings.Repeat("-", calculateTotalWidth(colWidths)))
}

func calculateTotalWidth(colWidths []int) int {
	total := 1
	for _, width := range colWidths {
		total += width + 3
	}
	return total
}

func printRecords(records []*Record) {
	if len(records) == 0 {
		fmt.Println("No records found")
		return
	}

	colWidths := make([]int, len(records[0].Cols))
	for _, record := range records {
		for i, col := range record.Cols {

			colWidths[i] = max(colWidths[i], len(col))

			valWidth := len(formatValue(record.Vals[i]))
			colWidths[i] = max(colWidths[i], valWidth)
		}
	}

	border := "+"
	for _, width := range colWidths {
		border += strings.Repeat("-", width+2) + "+"
	}

	fmt.Println(border)
	fmt.Print("|")
	for i, col := range records[0].Cols {
		fmt.Printf(" %-*s |", colWidths[i], col)
	}
	fmt.Println()
	fmt.Println(border)

	for _, record := range records {
		fmt.Print("|")
		for i, val := range record.Vals {
			fmt.Printf(" %-*s |", colWidths[i], formatValue(val))
		}
		fmt.Println()
	}
	fmt.Println(border)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
