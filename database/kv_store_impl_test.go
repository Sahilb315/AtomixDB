package database

import (
	"os"
	"testing"
)

func TestKVOperations(t *testing.T) {
	// Test file path
	testDBPath := "test_database.db"

	// Clean up test database before and after tests
	defer os.Remove(testDBPath)

	// Create a new KV store
	db := newKV(testDBPath)

	// Open the database
	if err := db.Open(); err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test SET and GET operations
	t.Run("Set and Get Single Value", func(t *testing.T) {
		key := []byte("test_key")
		value := []byte("test_value")

		// Set the key-value pair
		err := db.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set key-value pair: %v", err)
		}

		// Retrieve the value
		retrievedValue, found := db.Get(key)
		if !found {
			t.Fatal("Key not found after setting")
		}

		if string(retrievedValue) != string(value) {
			t.Fatalf("Retrieved value does not match. Expected %s, got %s",
				string(value), string(retrievedValue))
		}
	})

	// Test overwriting an existing key
	t.Run("Overwrite Existing Key", func(t *testing.T) {
		key := []byte("overwrite_key")
		initialValue := []byte("initial_value")
		updatedValue := []byte("updated_value")

		// Set initial value
		err := db.Set(key, initialValue)
		if err != nil {
			t.Fatalf("Failed to set initial key-value pair: %v", err)
		}

		// Overwrite the key
		err = db.Set(key, updatedValue)
		if err != nil {
			t.Fatalf("Failed to overwrite key-value pair: %v", err)
		}

		// Retrieve the updated value
		retrievedValue, found := db.Get(key)
		if !found {
			t.Fatal("Key not found after overwriting")
		}

		if string(retrievedValue) != string(updatedValue) {
			t.Fatalf("Retrieved value does not match updated value. Expected %s, got %s",
				string(updatedValue), string(retrievedValue))
		}
	})

	// Test DELETE operation
	t.Run("Delete Existing Key", func(t *testing.T) {
		key := []byte("delete_key")
		value := []byte("delete_value")

		// Set a key-value pair
		err := db.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set key-value pair: %v", err)
		}

		// Delete the key
		deleted, err := db.Delete(&DeleteReq{Key: key})
		if err != nil {
			t.Fatalf("Failed to delete key: %v", err)
		}

		if !deleted {
			t.Fatal("Key was not deleted")
		}

		// Try to retrieve the deleted key
		_, found := db.Get(key)
		if found {
			t.Fatal("Key should not exist after deletion")
		}
	})

	// Test retrieving non-existent key
	t.Run("Get Non-Existent Key", func(t *testing.T) {
		key := []byte("non_existent_key")

		// Try to retrieve a non-existent key
		_, found := db.Get(key)
		if found {
			t.Fatal("Non-existent key should not be found")
		}
	})

	// Test multiple operations
	t.Run("Multiple Operations", func(t *testing.T) {
		// Set multiple key-value pairs
		testData := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for k, v := range testData {
			err := db.Set([]byte(k), []byte(v))
			if err != nil {
				t.Fatalf("Failed to set key %s: %v", k, err)
			}
		}

		// Verify each key
		for k, v := range testData {
			retrievedValue, found := db.Get([]byte(k))
			if !found {
				t.Fatalf("Key %s not found", k)
			}

			if string(retrievedValue) != v {
				t.Fatalf("Mismatched value for key %s. Expected %s, got %s",
					k, v, string(retrievedValue))
			}
		}

		// Delete a key and verify
		err := db.Set([]byte("key_to_delete"), []byte("temp_value"))
		if err != nil {
			t.Fatalf("Failed to set key for deletion: %v", err)
		}

		deleted, err := db.Delete(&DeleteReq{Key: []byte("key_to_delete")})
		if err != nil {
			t.Fatalf("Failed to delete key: %v", err)
		}

		if !deleted {
			t.Fatal("Key was not deleted")
		}

		_, found := db.Get([]byte("key_to_delete"))
		if found {
			t.Fatal("Deleted key should not exist")
		}
	})
}
