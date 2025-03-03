package database

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
)

func TestCreate(t *testing.T) {
	db := setupTestDB(t)
	defer db.kv.Close()
}

func TestCreateTable(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	tests := []struct {
		name        string
		tableDef    *TableDef
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid table creation",
			tableDef: &TableDef{
				Name:  "users",
				Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_BYTES},
				Cols:  []string{"id", "name", "email"},
				PKeys: 1,
			},
			expectError: false,
		},
		{
			name: "duplicate table",
			tableDef: &TableDef{
				Name:  "users",
				Types: []uint32{TYPE_INT64, TYPE_BYTES},
				Cols:  []string{"id", "name"},
				PKeys: 1,
			},
			expectError: true,
			errorMsg:    "table already exists",
		},
		{
			name: "empty table name",
			tableDef: &TableDef{
				Name:  "",
				Types: []uint32{TYPE_INT64},
				Cols:  []string{"id"},
				PKeys: 1,
			},
			expectError: true,
			errorMsg:    "table name cannot be empty",
		},
		{
			name: "mismatched columns and types",
			tableDef: &TableDef{
				Name:  "test",
				Types: []uint32{TYPE_INT64},
				Cols:  []string{"id", "name"},
				PKeys: 1,
			},
			expectError: true,
			errorMsg:    "length of columns & types do not match",
		},
		{
			name: "invalid column type",
			tableDef: &TableDef{
				Name:  "test",
				Types: []uint32{99},
				Cols:  []string{"id"},
				PKeys: 1,
			},
			expectError: true,
			errorMsg:    "invalid data type",
		},
		{
			name: "multiple primary keys",
			tableDef: &TableDef{
				Name:  "test",
				Types: []uint32{TYPE_INT64, TYPE_INT64},
				Cols:  []string{"id1", "id2"},
				PKeys: 2,
			},
			expectError: true,
			errorMsg:    "only one primary key is allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writer KVTX
			db.kv.Begin(&writer)
			err := db.TableNew(tt.tableDef, &writer)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorMsg)
				} else if !isEqual(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				db.kv.Abort(&writer)
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					db.kv.Abort(&writer)
				} else {
					db.kv.Commit(&writer)
				}
			}
		})
	}
}

func TestInsertRecord(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create a test table first
	setupTestTable(t, db)

	tests := []struct {
		name        string
		record      Record
		mode        int
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid insert",
			record: Record{
				Cols: []string{"id", "name", "email"},
				Vals: []Value{
					{Type: TYPE_INT64, I64: 1},
					{Type: TYPE_BYTES, Str: []byte("John")},
					{Type: TYPE_BYTES, Str: []byte("john@example.com")},
				},
			},
			mode:        MODE_INSERT_ONLY,
			expectError: false,
		},
		{
			name: "duplicate key",
			record: Record{
				Cols: []string{"id", "name", "email"},
				Vals: []Value{
					{Type: TYPE_INT64, I64: 1},
					{Type: TYPE_BYTES, Str: []byte("John")},
					{Type: TYPE_BYTES, Str: []byte("john@example.com")},
				},
			},
			mode:        MODE_INSERT_ONLY,
			expectError: true,
			errorMsg:    "record already exists",
		},
		{
			name: "missing primary key",
			record: Record{
				Cols: []string{"name", "email"},
				Vals: []Value{
					{Type: TYPE_BYTES, Str: []byte("John")},
					{Type: TYPE_BYTES, Str: []byte("john@example.com")},
				},
			},
			mode:        MODE_INSERT_ONLY,
			expectError: true,
			errorMsg:    "missing column",
		},
		{
			name: "type mismatch",
			record: Record{
				Cols: []string{"id", "name", "email"},
				Vals: []Value{
					{Type: TYPE_BYTES, Str: []byte("invalid")},
					{Type: TYPE_BYTES, Str: []byte("John")},
					{Type: TYPE_BYTES, Str: []byte("john@example.com")},
				},
			},
			mode:        MODE_INSERT_ONLY,
			expectError: true,
			errorMsg:    "invalid type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writer KVTX
			db.kv.Begin(&writer)
			inserted, err := db.Insert("users", tt.record, &writer)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorMsg)
				} else if !isEqual(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				db.kv.Abort(&writer)
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					db.kv.Abort(&writer)
				} else if !inserted {
					t.Error("expected record to be inserted")
					db.kv.Abort(&writer)
				} else {
					db.kv.Commit(&writer)
				}
			}
		})
	}
}

func TestDelete(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create a test table and insert a record first
	setupTestTable(t, db)
	insertTestRecord(t, db, 1)

	tests := []struct {
		name        string
		record      Record
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid delete",
			record: Record{
				Cols: []string{"id", "name", "email"},
				Vals: []Value{
					{Type: TYPE_INT64, I64: 1},
					{Type: TYPE_BYTES, Str: []byte("John")},
					{Type: TYPE_BYTES, Str: []byte("john@example.com")},
				},
			},
			expectError: false,
		},
		{
			name: "non-existent record",
			record: Record{
				Cols: []string{"id", "name", "email"},
				Vals: []Value{
					{Type: TYPE_INT64, I64: 999},
					{Type: TYPE_BYTES, Str: []byte("NonExistent")},
					{Type: TYPE_BYTES, Str: []byte("none@example.com")},
				},
			},
			expectError: true,
			errorMsg:    "record not found",
		},
		{
			name: "missing primary key",
			record: Record{
				Cols: []string{"name", "email"},
				Vals: []Value{
					{Type: TYPE_BYTES, Str: []byte("John")},
					{Type: TYPE_BYTES, Str: []byte("john@example.com")},
				},
			},
			expectError: true,
			errorMsg:    "missing primary key column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writer KVTX
			db.kv.Begin(&writer)
			deleted, err := db.Delete("users", tt.record, &writer)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorMsg)
				} else if !isEqual(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				db.kv.Abort(&writer)
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					db.kv.Abort(&writer)
				} else if !deleted {
					t.Error("expected record to be deleted")
					db.kv.Abort(&writer)
				} else {
					db.kv.Commit(&writer)
				}
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create a test table and insert a record first
	setupTestTable(t, db)
	insertTestRecord(t, db, 1)

	tests := []struct {
		name        string
		record      Record
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid update",
			record: Record{
				Cols: []string{"id", "name", "email"},
				Vals: []Value{
					{Type: TYPE_INT64, I64: 1},
					{Type: TYPE_BYTES, Str: []byte("John Updated")},
					{Type: TYPE_BYTES, Str: []byte("updated@example.com")},
				},
			},
			expectError: false,
		},
		{
			name: "non-existent record",
			record: Record{
				Cols: []string{"id", "name", "email"},
				Vals: []Value{
					{Type: TYPE_INT64, I64: 999},
					{Type: TYPE_BYTES, Str: []byte("NonExistent")},
					{Type: TYPE_BYTES, Str: []byte("none@example.com")},
				},
			},
			expectError: true,
			errorMsg:    "record not found",
		},
		{
			name: "type mismatch",
			record: Record{
				Cols: []string{"id", "name", "email"},
				Vals: []Value{
					{Type: TYPE_BYTES, Str: []byte("invalid")},
					{Type: TYPE_BYTES, Str: []byte("John")},
					{Type: TYPE_BYTES, Str: []byte("john@example.com")},
				},
			},
			expectError: true,
			errorMsg:    "invalid type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var writer KVTX
			db.kv.Begin(&writer)
			updated, err := db.Update("users", tt.record, &writer)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorMsg)
				} else if !isEqual(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				db.kv.Abort(&writer)
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					db.kv.Abort(&writer)
				} else if !updated {
					t.Error("expected record to be updated")
					db.kv.Abort(&writer)
				} else {
					db.kv.Commit(&writer)
				}
			}
		})
	}
}

func TestGet(t *testing.T) {
	db := setupTestDB(t)
	defer cleanupTestDB(t, db)

	// Create a test table and insert records
	setupTestTable(t, db)
	insertTestRecord(t, db, 1)
	insertTestRecord(t, db, 2)

	tests := []struct {
		name        string
		record      Record
		expectError bool
		errorMsg    string
		expectFound bool
	}{
		{
			name: "valid get",
			record: Record{
				Cols: []string{"id"},
				Vals: []Value{
					{Type: TYPE_INT64, I64: 1},
				},
			},
			expectError: false,
			expectFound: true,
		},
		{
			name: "non-existent record",
			record: Record{
				Cols: []string{"id"},
				Vals: []Value{
					{Type: TYPE_INT64, I64: 999},
				},
			},
			expectError: false,
			expectFound: false,
		},
		{
			name: "invalid column",
			record: Record{
				Cols: []string{"invalid_col"},
				Vals: []Value{
					{Type: TYPE_INT64, I64: 1},
				},
			},
			expectError: true,
			errorMsg:    "no index found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reader KVReader
			db.kv.BeginRead(&reader)
			found, err := db.Get("users", &tt.record, &reader)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorMsg)
				} else if !isEqual(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if found != tt.expectFound {
					t.Errorf("expected found=%v, got %v", tt.expectFound, found)
				}
			}
			db.kv.EndRead(&reader)
		})
	}
}

// Helper functions
func setupTestDB(t *testing.T) *DB {
	testPath := "test.db"

	testDB := &DB{
		Path:   testPath,
		kv:     *newKV(testPath),
		tables: make(map[string]*TableDef),
		pool:   NewPool(3),
	}

	if err := testDB.kv.Open(); err != nil {
		log.Fatalf("Failed to open  %v", err)
	}
	err := initializeInternalTables(testDB)
	if err != nil {
		if !errors.Is(err, ErrTableAlreadyExists) {
			fmt.Println("Error while init table: ", err)
			os.Exit(0)
		}
	}
	return testDB
}

func cleanupTestDB(t *testing.T, db *DB) {
	db.kv.Close()
	if err := os.Remove(db.Path); err != nil {
		t.Errorf("failed to cleanup test database: %v", err)
	}
}

func setupTestTable(t *testing.T, db *DB) {
	var writer KVTX
	db.kv.Begin(&writer)

	tdef := &TableDef{
		Name:  "users",
		Types: []uint32{TYPE_INT64, TYPE_BYTES, TYPE_BYTES},
		Cols:  []string{"id", "name", "email"},
		PKeys: 1,
	}

	if err := db.TableNew(tdef, &writer); err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}
	db.kv.Commit(&writer)
}

func insertTestRecord(t *testing.T, db *DB, id int64) {
	var writer KVTX
	db.kv.Begin(&writer)

	record := Record{
		Cols: []string{"id", "name", "email"},
		Vals: []Value{
			{Type: TYPE_INT64, I64: id},
			{Type: TYPE_BYTES, Str: []byte("John")},
			{Type: TYPE_BYTES, Str: []byte("john@example.com")},
		},
	}

	inserted, err := db.Insert("users", record, &writer)
	if err != nil {
		t.Fatalf("failed to insert test record: %v", err)
	}
	if !inserted {
		t.Fatal("failed to insert test record")
	}
	db.kv.Commit(&writer)
}

func isEqual(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
