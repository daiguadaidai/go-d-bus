package matemap

const(
    // 数字类型
    TYPE_BIT                         = iota + 1 // bit
    TYPE_TINYINT                                // tinyint
    TYPE_SMALLINT                               // smallint
    TYPE_MEDIUMINT                              // mediumint
    TYPE_INT                                    // int
    TYPE_BIGINT                                 // bigint
    TYPE_DECIMAL                                // decimal
    TYPE_FLOAT                                  // float
    TYPE_DOUBLE                                 // double

    // 字符串类型
    TYPE_CHAR            // char
    TYPE_VARCHAR         // varchar
    TYPE_BINARY          // binary
    TYPE_VARBINARY       // varbinary
    TYPE_ENUM            // enum
    TYPE_SET             // set
    TYPE_TINYBLOB        // tinyblob
    TYPE_BLOB            // blob
    TYPE_MEDIUMBLOB      // mediumblob
    TYPE_LONGBLOB        // longblob
    TYPE_TINYTEXT        // tinytext
    TYPE_TEXT            // text
    TYPE_MEDIUMTEXT      // mediumtext
    TYPE_LONGTEXT        // longtext

    // 日期类型
    TYPE_DATE            // date
    TYPE_TIME            // time
    TYPE_DATETIME        // datetime
    TYPE_TIMESTAMP       // timestamp
    TYPE_YEAR            // year

    // json 类型
    TYPE_JSON            // json

    // 地理位置类型
    TYPE_GEOMETRY             // geometry
    TYPE_POINT                // point
    TYPE_LINESTRING           // linestring
    TYPE_POLYGON              // polygon
    TYPE_GEOMETRYCOLLECTION   // geometrycollection
    TYPE_MULTIPOINT           // multipoint
    TYPE_MULTILINESTRING      // multilinestring
    TYPE_MULTIPOLYGON         // multipolygon
)
