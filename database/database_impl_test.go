package database

//
// import (
// 	"os"
// 	"testing"
// )
//
// func setupTestDB(t *testing.T) *DB {
// 	os.Remove("test.db")
//
// 	db := &DB{
// 		Path:   "test.db",
// 		kv:   *newKVTX("test.db"),
// 		tables: make(map[string]*TableDef),
// 	}
//
// 	if err := db.kv.kv.Open(); err != nil {
// 		t.Fatalf("Failed to open test database: %v", err)
// 	}
//
// 	if err := initializeInternalTables(db); err != nil {
// 		t.Fatalf("Failed to initialize internal tables: %v", err)
// 	}
//
// 	return db
// }
//
// func cleanupTestDB(t *testing.T, db *DB) {
// 	db.kv.kv.Close()
// 	os.Remove("test.db")
// }
//
// func TestTableCreation(t *testing.T) {
// 	db := setupTestDB(t)
// 	defer cleanupTestDB(t, db)
//
// 	tests := []struct {
// 		name    string
// 		tdef    *TableDef
// 		wantErr bool
// 	}{
// 		{
// 			name: "Valid table creation",
// 			tdef: &TableDef{
// 				Name:        "users",
// 				Cols:        []string{"id", "name", "age"},
// 				Types:       []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
// 				PKeys:       1,
// 				Indexes:     [][]string{{"name"}},
// 				IndexPrefix: make([]uint32, 0),
// 			},
// 			wantErr: false,
// 		},
// 		{
// 			name: "Duplicate table creation",
// 			tdef: &TableDef{
// 				Name:        "users",
// 				Cols:        []string{"id", "name"},
// 				Types:       []uint32{TYPE_INT64, TYPE_BYTES},
// 				PKeys:       1,
// 				IndexPrefix: make([]uint32, 0),
// 			},
// 			wantErr: true,
// 		},
// 		{
// 			name: "Invalid column type count",
// 			tdef: &TableDef{
// 				Name:        "invalid",
// 				Cols:        []string{"id", "name"},
// 				Types:       []uint32{TYPE_INT64},
// 				PKeys:       1,
// 				IndexPrefix: make([]uint32, 0),
// 			},
// 			wantErr: true,
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			err := db.TableNew(tt.tdef)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("TableNew() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 		})
// 	}
// }
//
// func TestInsert(t *testing.T) {
// 	db := setupTestDB(t)
// 	defer cleanupTestDB(t, db)
//
// 	tdef := &TableDef{
// 		Name:        "users",
// 		Cols:        []string{"id", "name", "age"},
// 		Types:       []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
// 		PKeys:       1,
// 		Indexes:     [][]string{{"name"}},
// 		IndexPrefix: make([]uint32, 0),
// 	}
// 	if err := db.TableNew(tdef); err != nil {
// 		t.Fatalf("Failed to create test table: %v", err)
// 	}
//
// 	tests := []struct {
// 		name    string
// 		record  Record
// 		wantErr bool
// 	}{
// 		{
// 			name: "Valid insertion",
// 			record: Record{
// 				Cols: []string{"id", "name", "age"},
// 				Vals: []Value{
// 					{Type: TYPE_INT64, I64: 1},
// 					{Type: TYPE_BYTES, Str: []byte("John")},
// 					{Type: TYPE_INT64, I64: 25},
// 				},
// 			},
// 			wantErr: false,
// 		},
// 		{
// 			name: "Duplicate primary key",
// 			record: Record{
// 				Cols: []string{"id", "name", "age"},
// 				Vals: []Value{
// 					{Type: TYPE_INT64, I64: 1},
// 					{Type: TYPE_BYTES, Str: []byte("Jane")},
// 					{Type: TYPE_INT64, I64: 30},
// 				},
// 			},
// 			wantErr: true,
// 		},
// 		{
// 			name: "Missing column",
// 			record: Record{
// 				Cols: []string{"id", "name"},
// 				Vals: []Value{
// 					{Type: TYPE_INT64, I64: 2},
// 					{Type: TYPE_BYTES, Str: []byte("Jane")},
// 				},
// 			},
// 			wantErr: true,
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			_, err := db.Insert("users", tt.record)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("Insert() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 		})
// 	}
// }
//
// func TestGet(t *testing.T) {
// 	db := setupTestDB(t)
// 	defer cleanupTestDB(t, db)
//
// 	tdef := &TableDef{
// 		Name:        "users",
// 		Cols:        []string{"id", "name", "age"},
// 		Types:       []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
// 		PKeys:       1,
// 		Indexes:     [][]string{{"name"}},
// 		IndexPrefix: make([]uint32, 0),
// 	}
// 	if err := db.TableNew(tdef); err != nil {
// 		t.Fatalf("Failed to create test table: %v", err)
// 	}
//
// 	testRecord := Record{
// 		Cols: []string{"id", "name", "age"},
// 		Vals: []Value{
// 			{Type: TYPE_INT64, I64: 1},
// 			{Type: TYPE_BYTES, Str: []byte("John")},
// 			{Type: TYPE_INT64, I64: 25},
// 		},
// 	}
// 	if _, err := db.Insert("users", testRecord); err != nil {
// 		t.Fatalf("Failed to insert test record: %v", err)
// 	}
//
// 	tests := []struct {
// 		name    string
// 		query   Record
// 		want    bool
// 		wantErr bool
// 	}{
// 		{
// 			name: "Get by primary key",
// 			query: Record{
// 				Cols: []string{"id"},
// 				Vals: []Value{{Type: TYPE_INT64, I64: 1}},
// 			},
// 			want:    true,
// 			wantErr: false,
// 		},
// 		{
// 			name: "Get by index",
// 			query: Record{
// 				Cols: []string{"name"},
// 				Vals: []Value{{Type: TYPE_BYTES, Str: []byte("John")}},
// 			},
// 			want:    true,
// 			wantErr: false,
// 		},
// 		{
// 			name: "Get non-existent record",
// 			query: Record{
// 				Cols: []string{"id"},
// 				Vals: []Value{{Type: TYPE_INT64, I64: 999}},
// 			},
// 			want:    false,
// 			wantErr: false,
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := db.Get("users", &tt.query)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 			if got != tt.want {
// 				t.Errorf("Get() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
//
// func TestUpdate(t *testing.T) {
// 	db := setupTestDB(t)
// 	defer cleanupTestDB(t, db)
//
// 	tdef := &TableDef{
// 		Name:        "users",
// 		Cols:        []string{"id", "name", "age"},
// 		Types:       []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
// 		PKeys:       1,
// 		Indexes:     [][]string{{"name"}},
// 		IndexPrefix: make([]uint32, 0),
// 	}
// 	if err := db.TableNew(tdef); err != nil {
// 		t.Fatalf("Failed to create test table: %v", err)
// 	}
//
// 	initRecord := Record{
// 		Cols: []string{"id", "name", "age"},
// 		Vals: []Value{
// 			{Type: TYPE_INT64, I64: 1},
// 			{Type: TYPE_BYTES, Str: []byte("John")},
// 			{Type: TYPE_INT64, I64: 25},
// 		},
// 	}
// 	if _, err := db.Insert("users", initRecord); err != nil {
// 		t.Fatalf("Failed to insert initial record: %v", err)
// 	}
//
// 	tests := []struct {
// 		name    string
// 		record  Record
// 		want    bool
// 		wantErr bool
// 	}{
// 		{
// 			name: "Valid update",
// 			record: Record{
// 				Cols: []string{"id", "name", "age"},
// 				Vals: []Value{
// 					{Type: TYPE_INT64, I64: 1},
// 					{Type: TYPE_BYTES, Str: []byte("John Doe")},
// 					{Type: TYPE_INT64, I64: 26},
// 				},
// 			},
// 			want:    true,
// 			wantErr: false,
// 		},
// 		{
// 			name: "Update non-existent record",
// 			record: Record{
// 				Cols: []string{"id", "name", "age"},
// 				Vals: []Value{
// 					{Type: TYPE_INT64, I64: 999},
// 					{Type: TYPE_BYTES, Str: []byte("Nobody")},
// 					{Type: TYPE_INT64, I64: 0},
// 				},
// 			},
// 			want:    false,
// 			wantErr: true,
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := db.Update("users", tt.record)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("Update() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 			if got != tt.want {
// 				t.Errorf("Update() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
//
// func TestDelete(t *testing.T) {
// 	db := setupTestDB(t)
// 	defer cleanupTestDB(t, db)
//
// 	tdef := &TableDef{
// 		Name:        "users",
// 		Cols:        []string{"id", "name", "age"},
// 		Types:       []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
// 		PKeys:       1,
// 		Indexes:     [][]string{{"name"}},
// 		IndexPrefix: make([]uint32, 0),
// 	}
// 	if err := db.TableNew(tdef); err != nil {
// 		t.Fatalf("Failed to create test table: %v", err)
// 	}
//
// 	testRecord := Record{
// 		Cols: []string{"id", "name", "age"},
// 		Vals: []Value{
// 			{Type: TYPE_INT64, I64: 1},
// 			{Type: TYPE_BYTES, Str: []byte("John")},
// 			{Type: TYPE_INT64, I64: 25},
// 		},
// 	}
// 	if _, err := db.Insert("users", testRecord); err != nil {
// 		t.Fatalf("Failed to insert test record: %v", err)
// 	}
//
// 	tests := []struct {
// 		name    string
// 		record  Record
// 		want    bool
// 		wantErr bool
// 	}{
// 		{
// 			name: "Delete non-existent record",
// 			record: Record{
// 				Cols: []string{"id"},
// 				Vals: []Value{{Type: TYPE_INT64, I64: 999}},
// 			},
// 			want:    false,
// 			wantErr: false,
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := db.Delete("users", tt.record)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 			if got != tt.want {
// 				t.Errorf("Delete() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
//
// func TestTransactions(t *testing.T) {
// 	db := setupTestDB(t)
// 	defer cleanupTestDB(t, db)
//
// 	tdef := &TableDef{
// 		Name:        "users",
// 		Cols:        []string{"id", "name", "age"},
// 		Types:       []uint32{TYPE_INT64, TYPE_BYTES, TYPE_INT64},
// 		PKeys:       1,
// 		Indexes:     [][]string{{"name"}},
// 		IndexPrefix: make([]uint32, 0),
// 	}
// 	if err := db.TableNew(tdef); err != nil {
// 		t.Fatalf("Failed to create test table: %v", err)
// 	}
//
// 	t.Run("Transaction commit", func(t *testing.T) {
// 		tx := &DBTX{}
// 		db.Begin(tx)
//
// 		record := Record{
// 			Cols: []string{"id", "name", "age"},
// 			Vals: []Value{
// 				{Type: TYPE_INT64, I64: 1},
// 				{Type: TYPE_BYTES, Str: []byte("John")},
// 				{Type: TYPE_INT64, I64: 25},
// 			},
// 		}
//
// 		added, err := tx.Set("users", record, MODE_INSERT_ONLY)
// 		if err != nil {
// 			t.Errorf("Transaction insert failed: %v", err)
// 		}
// 		if !added {
// 			t.Error("Transaction insert returned false")
// 		}
//
// 		if err := db.Commit(tx); err != nil {
// 			t.Errorf("Transaction commit failed: %v", err)
// 		}
//
// 		var queryRec Record
// 		queryRec.Cols = []string{"id"}
// 		queryRec.Vals = []Value{{Type: TYPE_INT64, I64: 1}}
// 		found, err := db.Get("users", &queryRec)
// 		if err != nil {
// 			t.Errorf("Failed to query record after commit: %v", err)
// 		}
// 		if !found {
// 			t.Error("Record not found after transaction commit")
// 		}
// 	})
// }
