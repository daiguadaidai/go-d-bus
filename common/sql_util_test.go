package common

import (
	"testing"
	"fmt"
)

func TestGetAnonymousTableName(t *testing.T) {
	tableName := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	anonymousName := GetAnonymousTableName(tableName)
	fmt.Println(anonymousName)

	tableName = "bbbbbb"
	anonymousName = GetAnonymousTableName(tableName)
	fmt.Println(anonymousName)
}

func TestCreateDropColumnSql(t *testing.T) {
	schemaName := "schema"
	tableName := "table"
	dropColumns := []string{"aa", "bb", "cc", "dd"}
	dropIndexes := []string{"idx_1", "idx_2", "idx_3", "idx_4"}

	alterSql := CreateDropColumnSql(schemaName, tableName, dropColumns, dropIndexes)
	fmt.Println(alterSql)
}

func TestFormatColumnNameStr(t *testing.T) {
	columnNames := []string{"col1", "col2", "col3"}
	fmt.Println(FormatColumnNameStr(columnNames))
}

func TestFormatOrderByStr(t *testing.T) {
	columnNames := []string{"col1", "col2", "col3"}
	ascDesc := "ASC"
	fmt.Println(FormatOrderByStr(columnNames, ascDesc))
}

func TestFormatTableName(t *testing.T) {
	schemaName := "schema"
	schenaTable := "table"

	fmt.Println(FormatTableName(schemaName, schenaTable, "`"))
}

func TestCreateWherePlaceholder(t *testing.T) {
	count := 4
	fmt.Println(CreatePlaceholderByCount(count))

	count = 1
	fmt.Println(CreatePlaceholderByCount(count))
}
