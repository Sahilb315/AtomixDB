package src

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

func StoreImpl() {
	scanner := bufio.NewReader(os.Stdin)
	db := newKV("database.db")

	if err := db.Open(); err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	fmt.Println("Welcome to AtomixDB")
	fmt.Println("Available Commands:")
	fmt.Println("  SET   - Add a key-value pair")
	fmt.Println("  DEL   - Delete a key")
	fmt.Println("  GET   - Retrieve the value for a key")
	fmt.Println("  EXIT  - Exit the program")
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
		case "SET":
			fmt.Print("Enter key: ")
			key, _ := scanner.ReadString('\n')
			key = strings.TrimSpace(key)

			fmt.Print("Enter value: ")
			val, _ := scanner.ReadString('\n')
			val = strings.TrimSpace(val)

			db.Set([]byte(key), []byte(val))

		case "DEL":
			fmt.Print("Enter key: ")
			key, _ := scanner.ReadString('\n')
			key = strings.TrimSpace(key)

			db.Delete([]byte(key))

		case "GET":
			fmt.Print("Enter key: ")
			key, _ := scanner.ReadString('\n')
			key = strings.TrimSpace(key)

			val, found := db.Get([]byte(key))
			if found {
				fmt.Println("Value:", string(val))
			} else {
				fmt.Println("Key not found")
			}

		case "EXIT":
			db.Close()
			fmt.Println("Exiting...")
			os.Exit(0)

		default:
			fmt.Println("Unknown command:", command)
		}
	}
}
