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
	"math/rand"
	"os"

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
	// 1. Configure the example database connection.
	db := createDB()

	// AutoMigrate for player table
	db.AutoMigrate(&Player{})

	// 2. Run some simple examples.
	simpleExample(db)

	// 3. Getting further.
	tradeExample(db)
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
	tidbUser := getEnvWithDefault("TIDB_USER", "root")
	tidbPassword := getEnvWithDefault("TIDB_PASSWORD", "")
	tidbDBName := getEnvWithDefault("TIDB_DB_NAME", "test")
	useSSL := getEnvWithDefault("USE_SSL", "false")

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
