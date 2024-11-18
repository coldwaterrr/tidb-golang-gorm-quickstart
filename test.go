package main

import (
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func createDB_t() *gorm.DB {

	db, err := gorm.Open(mysql.Open(getDSN_t()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})

	fmt.Println("getDSN():", getDSN())

	if err != nil {
		panic(err)
	}

	return db
}

func getDSN_t() string {
	tidbHost := getEnvWithDefault("TIDB_HOST", "127.0.0.1")
	tidbPort := getEnvWithDefault("TIDB_PORT", "4000")
	//tidbPort := getEnvWithDefault("TIDB_PORT", "4000")
	tidbUser := getEnvWithDefault("TIDB_USER", "root")
	tidbPassword := getEnvWithDefault("TIDB_PASSWORD", "")
	tidbDBName := getEnvWithDefault("TIDB_DB_NAME", "player")
	useSSL := getEnvWithDefault("USE_SSL", "true")

	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&tls=%s",
		tidbUser, tidbPassword, tidbHost, tidbPort, tidbDBName, useSSL)
}

// func main() {
// 	createDB_t()
// }
