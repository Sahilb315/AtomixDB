package database

import (
	"atomixDB/database/helper"
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func newKV(filename string) *KV {
	return &KV{
		Path: filename,
	}
}

func newDB() *DB {
	return &DB{
		Path:   fileName,
		kv:     *newKV(fileName),
		tables: make(map[string]*TableDef),
		pool:   NewPool(3),
	}
}

const fileName string = "database.db"

func initializeInternalTables(db *DB) error {
	var writer KVTX
	db.kv.Begin(&writer)
	if err := db.TableNew(TDEF_META, &writer); err != nil {
		db.kv.Abort(&writer)
		if !strings.Contains(err.Error(), "table exists") {
			return fmt.Errorf("failed to create TDEF_META: %v", err)
		}
		fmt.Println("TDEF_META already exists")
	}
	db.kv.Commit(&writer)
	db.kv.Begin(&writer)
	if err := db.TableNew(TDEF_TABLE, &writer); err != nil {
		db.kv.Abort(&writer)
		if !strings.Contains(err.Error(), "table exists") {
			return fmt.Errorf("failed to create TDEF_TABLE: %v", err)
		}
		fmt.Println("TDEF_TABLE already exists")
	}
	db.kv.Commit(&writer)
	return nil
}

func StartDB() {
	scanner := bufio.NewReader(os.Stdin)
	db := newDB()
	if err := db.kv.Open(); err != nil {
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
		shutdownDB(db)
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

		command := strings.ToLower(strings.TrimSpace(string(line)))
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
			shutdownDB(db)
			break
		} else {
			fmt.Println("Unknown command:", command)
		}
	}
}

func shutdownDB(db *DB) {
	db.kv.Close()
	db.pool.Stop()
	fmt.Println("Exiting...")
	os.Exit(0)
}
