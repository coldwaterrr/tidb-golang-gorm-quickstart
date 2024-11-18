// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap-inc/tidb-example-golang/util"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"

	_ "github.com/joho/godotenv/autoload"
)

type Player struct {
	ID    string `gorm:"primaryKey;type:VARCHAR(36);column:id"`
	Coins int    `gorm:"column:coins"`
	Goods int    `gorm:"column:goods"`
}

func (*Player) TableName() string {
	return "player"
}

func main() {

	// 创建输出文件
	file, err := os.Create("output.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// 将标准输出重定向到文件
	log.SetOutput(file)

	// 1. Configure the example database connection.
	db := createDB()

	// path := "/home/zbw/imdb_pg_dataset/mssql/schema.sql"

	// //启用多语句模式
	// if err := db.Exec("SET GLOBAL tidb_multi_statement_mode='ON'").Error; err != nil {
	// 	panic(err)
	// }

	// //创建新的数据库
	// newDBName := "imdb"
	// if err := db.Exec("CREATE DATABASE IF NOT EXISTS " + newDBName).Error; err != nil {
	// 	panic(err)
	// }

	// //切换到新的数据库
	// if err := db.Exec("USE " + newDBName).Error; err != nil {
	// 	panic(err)
	// }

	// //导入数据集
	// importDataset(db, path)

	// // 查询所有数据库
	// var databases []string
	// db.Raw("SHOW DATABASES").Scan(&databases)

	// // 打印所有数据库
	// log.Println("Databases:")
	// for _, database := range databases {
	// 	log.Println(database)
	// 	log.Println("")
	// 	log.Println("")
	// 	// 切换到当前数据库
	// 	db.Exec("USE " + database)

	// 	// 查询当前数据库中的所有表
	// 	var tables []string
	// 	db.Raw("SHOW TABLES").Scan(&tables)

	// 	// 打印当前数据库中的所有表
	// 	fmt.Println("Tables in database", database, ":")
	// 	for _, table := range tables {
	// 		log.Println(table)

	// 		// 查询当前表的结构
	// 		var tableStructure []map[string]interface{}
	// 		db.Raw("SHOW COLUMNS FROM " + table).Scan(&tableStructure)

	// 		// 打印当前表的结构
	// 		log.Println("Structure of table", table, ":")
	// 		for _, column := range tableStructure {
	// 			log.Println(column)
	// 		}
	// 	}
	// }

	// // AutoMigrate for player table
	// db.AutoMigrate(&Player{})

	// // 2. Run some simple examples.
	// simpleExample(db)

	// // 3. Getting further.
	// tradeExample(db)

	// 4. Query tables in imdb database
	queryIMDBTables(db)
}

func importDataset(db *gorm.DB, filePath string) {
	// 读取 SQL 文件
	sqlBytes, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	sql := string(sqlBytes)

	// 执行 SQL 语句
	if err := db.Exec(sql).Error; err != nil {
		panic(err)
	}
}

func tradeExample(db *gorm.DB) {
	// Player 1: id is "1", has only 100 coins.
	// Player 2: id is "2", has 114514 coins, and 20 goods.
	player1 := &Player{ID: "1", Coins: 100}
	player2 := &Player{ID: "2", Coins: 114514, Goods: 20}

	// Create two players "by hand", using the INSERT statement on the backend.
	db.Clauses(clause.OnConflict{UpdateAll: true}).Create(player1)
	db.Clauses(clause.OnConflict{UpdateAll: true}).Create(player2)

	// Player 1 wants to buy 10 goods from player 2.
	// It will cost 500 coins, but player 1 cannot afford it.
	fmt.Println("\nbuyGoods:\n    => this trade will fail")
	if err := buyGoods(db, player2.ID, player1.ID, 10, 500); err == nil {
		panic("there shouldn't be success")
	}

	// So player 1 has to reduce the incoming quantity to two.
	fmt.Println("\nbuyGoods:\n    => this trade will success")
	if err := buyGoods(db, player2.ID, player1.ID, 2, 100); err != nil {
		panic(err)
	}
}

func simpleExample(db *gorm.DB) {
	// Create a player, who has a coin and a goods.
	if err := db.Clauses(clause.OnConflict{UpdateAll: true}).
		Create(&Player{ID: "test", Coins: 1, Goods: 1}).Error; err != nil {
		panic(err)
	}

	// Get a player.
	var testPlayer Player
	db.Find(&testPlayer, "id = ?", "test")
	fmt.Printf("getPlayer: %+v\n", testPlayer)

	// Create players with bulk inserts. Insert 1919 players totally, with 114 players per batch.
	bulkInsertPlayers := make([]Player, 1919, 1919)
	total, batch := 1919, 114
	for i := 0; i < total; i++ {
		bulkInsertPlayers[i] = Player{
			ID:    uuid.New().String(),
			Coins: rand.Intn(10000),
			Goods: rand.Intn(10000),
		}
	}

	if err := db.Session(&gorm.Session{Logger: db.Logger.LogMode(logger.Error)}).
		CreateInBatches(bulkInsertPlayers, batch).Error; err != nil {
		panic(err)
	}

	// Count players amount.
	playersCount := int64(0)
	db.Model(&Player{}).Count(&playersCount)
	fmt.Printf("countPlayers: %d\n", playersCount)

	// Print 3 players.
	threePlayers := make([]Player, 3, 3)
	db.Limit(3).Find(&threePlayers)
	for index, player := range threePlayers {
		fmt.Printf("print %d player: %+v\n", index+1, player)
	}
}

func createDB() *gorm.DB {

	db, err := gorm.Open(mysql.Open(getDSN()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})

	fmt.Println("getDSN():", getDSN())

	if err != nil {
		panic(err)
	}

	return db
}

func buyGoods(db *gorm.DB, sellID, buyID string, amount, price int) error {
	return util.TiDBGormBegin(db, true, func(tx *gorm.DB) error {
		var sellPlayer, buyPlayer Player
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Find(&sellPlayer, "id = ?", sellID).Error; err != nil {
			return err
		}

		if sellPlayer.ID != sellID || sellPlayer.Goods < amount {
			return fmt.Errorf("sell player %s goods not enough", sellID)
		}

		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Find(&buyPlayer, "id = ?", buyID).Error; err != nil {
			return err
		}

		if buyPlayer.ID != buyID || buyPlayer.Coins < price {
			return fmt.Errorf("buy player %s coins not enough", buyID)
		}

		updateSQL := "UPDATE player set goods = goods + ?, coins = coins + ? WHERE id = ?"
		if err := tx.Exec(updateSQL, -amount, price, sellID).Error; err != nil {
			return err
		}

		if err := tx.Exec(updateSQL, amount, -price, buyID).Error; err != nil {
			return err
		}

		fmt.Println("\n[buyGoods]:\n    'trade success'")
		return nil
	})
}

func getDSN() string {
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

func getEnvWithDefault(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func queryIMDBTables(db *gorm.DB) {
	// // 切换到imdb数据库
	// if err := db.Exec("USE imdb").Error; err != nil {
	// 	panic(err)
	// }

	// // 查询imdb数据库中的所有表
	// var tables []string
	// db.Raw("SHOW TABLES").Scan(&tables)

	// // 打印imdb数据库中的所有表
	// log.Println("Tables in imdb database:")
	// for _, table := range tables {
	// 	log.Println(table)

	// 	// 查询当前表的结构
	// 	var tableStructure []map[string]interface{}
	// 	db.Raw("SHOW COLUMNS FROM " + table).Scan(&tableStructure)

	// 	// 打印当前表的结构
	// 	log.Println("Structure of table", table, ":")
	// 	for _, column := range tableStructure {
	// 		log.Println(column)
	// 	}
	// }

	// // 切换到imdb数据库
	// if err := db.Exec("USE imdb").Error; err != nil {
	// 	panic(err)
	// }

	// // 查询aka_name表中的前50行数据
	// var akaNames []map[string]interface{}
	// db.Raw("SELECT * FROM title LIMIT 50").Scan(&akaNames)

	// // 打印aka_name表中的前50行数据
	// log.Println("First 50 rows in aka_name table:")
	// for _, row := range akaNames {
	// 	log.Println(row)
	// }

	// 切换到imdb数据库
	if err := db.Exec("USE imdb").Error; err != nil {
		panic(err)
	}

	// 	// 删除person_info和title表
	// 	if err := db.Exec("DROP TABLE IF EXISTS person_info").Error; err != nil {
	// 		panic(err)
	// 	}
	// 	if err := db.Exec("DROP TABLE IF EXISTS title").Error; err != nil {
	// 		panic(err)
	// 	}

	// 	// 重新创建person_info表
	// 	createPersonInfoTableSQL := `
	// 	CREATE TABLE person_info (
	//     id integer NOT NULL,
	//     person_id integer NOT NULL,
	//     info_type_id integer NOT NULL,
	//     info TEXT NOT NULL,
	//     note TEXT
	// );`
	// 	if err := db.Exec(createPersonInfoTableSQL).Error; err != nil {
	// 		panic(err)
	// 	}

	//		// 重新创建title表
	//		createTitleTableSQL := `
	//		CREATE TABLE title (
	//	    id integer NOT NULL,
	//	    title TEXT NOT NULL,
	//	    imdb_index nvarchar(5),
	//	    kind_id integer NOT NULL,
	//	    production_year integer,
	//	    imdb_id integer,
	//	    phonetic_code nvarchar(5),
	//	    episode_of_id integer,
	//	    season_nr integer,
	//	    episode_nr integer,
	//	    series_years nvarchar(49),
	//	    md5sum nvarchar(32)
	//
	// );`
	//
	//	if err := db.Exec(createTitleTableSQL).Error; err != nil {
	//		panic(err)
	//	}

	// // 查询movie_info表中的数据
	// var movieInfo []map[string]interface{}
	// db.Raw("SELECT * FROM movie_info WHERE id = 8582476").Scan(&movieInfo)

	// // 打印movie_info表中的数据到文件
	// log.Println("Data in movie_info table with id 8582476:")
	// for _, row := range movieInfo {
	// 	log.Println(row)
	// }
	directoryPath := "/home/zbw/imdb_pg_dataset/job"

	files, err := os.ReadDir(directoryPath)
	if err != nil {
		log.Fatal(err)
	}

	//fmt.Println("files:", files[0].Name())
	//fmt.Printf("fileType: %T\n", files[0].Name())

	for _, file := range files {
		filePath := "/home/zbw/imdb_pg_dataset/job/" + file.Name()

		if strings.Split(file.Name(), ".")[1] == "sql" {

			sql, err := os.ReadFile(filePath)
			if err != nil {
				panic(err)
			}

			// //fmt.Println("sql:", string(sql))

			// result := db.Raw("explain" + string(sql))

			// fmt.Println("result:", result)

			// 执行 SQL 查询并返回结果
			var result []map[string]interface{}
			db.Raw("explain analyze " + string(sql)).Scan(&result)

			// 将结果转换为字符串格式
			resultStr := fmt.Sprintf("Result for file: %s\n", file.Name())
			for _, row := range result {
				resultStr += fmt.Sprintf("%v\n", row)
			}

			// 将结果写入到 .txt 文件中
			outputFilePath := directoryPath + "/analyze" + file.Name() + ".txt"
			err = os.WriteFile(outputFilePath, []byte(resultStr), 0644)
			if err != nil {
				panic(err)
			}

			fmt.Printf("Result written to %s\n", outputFilePath)

		}

		// file, err := os.Open(filePath)
		// if err != nil {
		// 	return err
		// }
		// defer file.Close()

		//var testInfo []map[string]interface{}

		//db.Raw()

	}
}
