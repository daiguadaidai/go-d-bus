package matemap

import (
    "testing"
    "fmt"
)

func TestNewColumn(t *testing.T) {
    columnName_01 := "id"
    columnType_01 := "tinyint(3) unsigned zerofill"
    extra_01 := "auto_increment"
    ordinalPosition_01 := 1
    column := CreateColumn(columnName_01, columnType_01, extra_01, ordinalPosition_01)
    fmt.Println(column)

}
