package database

import (
	"atomixDB/database/helper"
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
)

type ReadResult struct {
	Record Record
	Error  error
	Found  bool
}

func newKV(filename string) *KV {
	return &KV{
		Path: filename,
	}
}

var fileName string = "database.db"

func newDB() *DB {
	return &DB{
		Path:   fileName,
		KV:     *newKV(fileName),
		Tables: make(map[string]*TableDef),
	}
}

const mutexLocked = 1

func MutexLocked(m *sync.Mutex) bool {
	state := reflect.ValueOf(m).Elem().FieldByName("state")
	return state.Int()&mutexLocked == mutexLocked
}

func initializeInternalTables(db *DB) error {
	var writer KVTX
	db.KV.Begin(&writer)
	if err := db.TableNew(TDEF_META, &writer); err != nil {
		db.KV.Abort(&writer)
		if !strings.Contains(err.Error(), "table exists") {
			return fmt.Errorf("failed to create TDEF_META: %v", err)
		}
		fmt.Println("TDEF_META already exists")
	}
	db.KV.Commit(&writer)
	fmt.Println("Mutex lock after meta table comp: ", MutexLocked(&db.KV.writer))
	db.KV.Begin(&writer)
	if err := db.TableNew(TDEF_TABLE, &writer); err != nil {
		db.KV.Abort(&writer)
		if !strings.Contains(err.Error(), "table exists") {
			return fmt.Errorf("failed to create TDEF_TABLE: %v", err)
		}
		fmt.Println("TDEF_TABLE already exists")
	}
	db.KV.Commit(&writer)
	fmt.Println("Mutex lock after def table comp: ", MutexLocked(&db.KV.writer))
	return nil
}

func StoreImpl() {
	scanner := bufio.NewReader(os.Stdin)
	db := newDB() // Initialize the database
	if err := db.KV.Open(); err != nil {
		log.Fatalf("Failed to open  %v", err)
	}
	err := initializeInternalTables(db)
	if err != nil {
		if !strings.Contains(err.Error(), "table already exists") {
			fmt.Println("Error while init table: ", err)
			os.Exit(0)
		}
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		db.KV.Close()
		os.Exit(0)
	}()

	commands := RegisterCommands()
	var currentTX *DBTX
	helper.PrintWelcomeMessage()

	for {
		fmt.Print("> ")
		line, _, err := scanner.ReadLine()
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		command := strings.TrimSpace(string(line))
		command = strings.ToLower(command)
		if handler, exists := commands[command]; exists {
			switch command {
			case "begin":
				currentTX = HandleBegin(scanner, db, currentTX)
			case "commit":
				currentTX = HandleCommit(scanner, db, currentTX)
			case "abort":
				currentTX = HandleAbort(scanner, db, currentTX)
			default:
				handler(scanner, db, currentTX)
			}
		} else if command == "exit" {
			db.KV.Close()
			fmt.Println("Exiting...")
			break
		} else {
			fmt.Println("Unknown command:", command)
		}
	}

}

func formatValue(v Value) string {
	switch v.Type {
	case 1:
		return fmt.Sprintf("%d", v.I64)
	case 2:
		return string(v.Str)
	default:
		return "Unknown Type"
	}
}

func printRecord(record Record) {
	headers := strings.Join(record.Cols, "\t")
	fmt.Println(headers)

	for i, val := range record.Vals {
		formattedValue := formatValue(val)
		if i == len(record.Vals)-1 {
			fmt.Print(formattedValue)
		} else {
			fmt.Print(formattedValue + "\t")
		}
	}
	fmt.Println()
}
