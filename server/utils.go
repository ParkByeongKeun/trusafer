package main

import (
	"bufio"
	"database/sql"
	"log"
	"os"
	"path"
	"strings"
)

// func getSqlNullStructValidValue(sqlNullStruct interface{}) interface{} {

// 	switch sqlNullStruct.(type) {
// 	case sql.NullString:
// 		if sqlNullStruct.(sql.NullString).Valid {
// 			return sqlNullStruct.(sql.NullString).String
// 		} else {
// 			return ""
// 		}
// 	case sql.NullInt16:
// 		if sqlNullStruct.(sql.NullInt16).Valid {
// 			return sqlNullStruct.(sql.NullInt16).Int16
// 		} else {
// 			return 0
// 		}
// 	case sql.NullInt32:
// 		if sqlNullStruct.(sql.NullInt32).Valid {
// 			return sqlNullStruct.(sql.NullInt32).Int32
// 		} else {
// 			return 0
// 		}
// 	case sql.NullInt64:
// 		if sqlNullStruct.(sql.NullInt64).Valid {
// 			return sqlNullStruct.(sql.NullInt64).Int64
// 		} else {
// 			return 0
// 		}
// 	case sql.NullFloat64:
// 		if sqlNullStruct.(sql.NullFloat64).Valid {
// 			return sqlNullStruct.(sql.NullFloat64).Float64
// 		} else {
// 			return 0
// 		}
// 	case sql.NullBool:
// 		if sqlNullStruct.(sql.NullBool).Valid {
// 			return sqlNullStruct.(sql.NullBool).Bool
// 		} else {
// 			return false
// 		}
// 	case sql.NullByte:
// 		if sqlNullStruct.(sql.NullByte).Valid {
// 			return sqlNullStruct.(sql.NullByte).Byte
// 		} else {
// 			return 0
// 		}
// 	case sql.NullTime:
// 		if sqlNullStruct.(sql.NullTime).Valid {
// 			return sqlNullStruct.(sql.NullTime).Time
// 		} else {
// 			return 0
// 		}
// 	default:
// 		return nil
// 	}
// }

func getNullStringValidValue(nullstring sql.NullString) string {
	if nullstring.Valid {
		return nullstring.String
	}
	return ""
}

func getOnlyNumbers(s string) string {
	return strings.ReplaceAll(s, "-", "")
}

func saveJpegBytesImage(imgByte []byte, savePath string) (bool, error) {
	baseDir := path.Dir(savePath)
	info, err := os.Stat(baseDir)
	if err != nil || info.IsDir() {
		err = os.MkdirAll(baseDir, 0755)
		if err != nil {
			return false, err
		}
	}

	fo, err := os.Create(savePath)
	if err != nil {
		log.Println("os Create err")
		log.Println(err)
		return false, err
	}
	defer fo.Close()

	fw := bufio.NewWriter(fo)
	_, err = fw.Write(imgByte)
	if err != nil {
		log.Println("fw.Write err")
		log.Println(err)
		return false, err
	}

	return true, nil
}
