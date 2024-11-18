package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

func printCSV(filePath string, leng int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.LazyQuotes = true    // 启用 LazyQuotes 选项
	reader.FieldsPerRecord = -1 // 允许每行有不同数量的字段

	//fmt.Println("")
	//fmt.Println(reader.Read())
	//fmt.Println("")

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return err
		}
		//fmt.Println(record)
		if record[0] == "2963574" {
			fmt.Println("len: ", len(record))
			for i := range record {
				fmt.Println(record[i])
				fmt.Println("")
			}
		}
		// if len(record) != leng {
		// 		fmt.Print("len: ", len(record))
		// 		fmt.Println("  line:", record[0])
		// }
	}

	fmt.Println("读取失败？")

	return nil
}

func main_1() {
	err := printCSV("/home/zbw/imdb_pg_dataset/imdb/imdb.person_info.csv", 5)
	if err != nil {
		panic(err)
	}

	// err = printCSV("/home/zbw/imdb_pg_dataset/imdb/imdb.title.csv", 12)
	// if err != nil {
	// 	panic(err)
	// }
}
