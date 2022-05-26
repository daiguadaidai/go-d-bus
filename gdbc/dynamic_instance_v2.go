package gdbc

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

var instanceCache map[string]*sql.DB

func init() {
	instanceCache = make(map[string]*sql.DB)
}

func AddInstanceToCache(host string, port int64, db *sql.DB) {
	key := fmt.Sprintf("%v:%v", host, port)
	oldDB, ok := instanceCache[key]
	if ok {
		oldDB.Close()
	}
	instanceCache[key] = db
}

func GetDynamicDBByHostPort(host string, port int64) (*sql.DB, bool) {
	dynamicKey := fmt.Sprintf("%v:%v", host, port)

	return GetDynamicDB(dynamicKey)
}

// dynamicKey: 127.0.0.1:3306
func GetDynamicDB(dynamicKey string) (*sql.DB, bool) {
	db, ok := instanceCache[dynamicKey]
	if !ok {
		return nil, ok
	}

	return db, ok
}
