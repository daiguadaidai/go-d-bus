package service

import (
    "github.com/daiguadaidai/go-d-bus/matemap"
    "github.com/daiguadaidai/go-d-bus/common"
    "fmt"
    "github.com/juju/errors"
    "strings"
	"github.com/outbrain/golib/log"
    "github.com/daiguadaidai/go-d-bus/gdbc"
)

// 获取需要的所有需要生成范围ID的表
func (this *RowCopy)GetNeedGetPrimaryRangeValueMap() (map[string]bool, error) {
    // 获取所有需要迁移的表
    migrationTableNameMap := matemap.FindAllMigrationTableNameMap()

    // 还需要生成 rowcopy 主键范围的表
    needGetPrimaryRangeValueMap := make(map[string]bool)

    for tableName, _ := range migrationTableNameMap {
    	if tableMap, ok := this.ConfigMap.TableMapMap[tableName]; ok { // 该表是确认要迁移的
    	    if tableMap.RowCopyComplete.Int64 == 1 { // 该表已经row copy 完成
    	        continue
            }

            // row copy 当前ID值范围为空 或则 为空字符串
            if !tableMap.CurrIDValue.Valid || strings.TrimSpace(tableMap.CurrIDValue.String) == "" {
                needGetPrimaryRangeValueMap[tableName] = true
                continue
            }

            // 没有row copy 截止的 ID 范围值
            if !tableMap.MaxIDValue.Valid || strings.TrimSpace(tableMap.MaxIDValue.String) == "" {
                needGetPrimaryRangeValueMap[tableName] = true
                continue
            }

            // 获取当前row copy到的id范围Map
            currPrimaryMap, err := common.Json2Map(tableMap.CurrIDValue.String)
            if err != nil {
            	log.Warningf("%v: 失败. 解析row copy 当前应用到的id 范围 json. value: %v. %v",
            	    common.CurrLine(), tableMap.CurrIDValue.String, err)
                needGetPrimaryRangeValueMap[tableName] = true
                continue
            }

            // 获取row copy 截止的id范围Map
            maxPrimaryMap, err := common.Json2Map(tableMap.MaxIDValue.String)
            if err != nil {
                log.Warningf("%v: 失败. 解析row copy 截止的id 范围 json. value: %v. %v",
                    common.CurrLine(), tableMap.MaxIDValue.String, err)
                needGetPrimaryRangeValueMap[tableName] = true
                continue
            }

            // 比较当前row copy 完成的id范围是否大于等于 需要截止的id范围
            if common.MapAGreaterOrEqualMapB(currPrimaryMap, maxPrimaryMap) { // 该表row copy完成
                continue
            } else { // 该表没完成 row copy
                needGetPrimaryRangeValueMap[tableName] = true
                continue
            }
        } else {
            errMSG := fmt.Sprintf("%v: 失败. 获取还需要进行生成row copy 范围ID失败. " +
                "在需要迁移的表中有, 但是在table_map表中没该表记录: %v", common.CurrLine(), tableName)

            return nil, errors.New(errMSG)
        }
    }

    return needGetPrimaryRangeValueMap, nil
}

// 获取需要迁移的表 当前row copy到的进度 id范围值
func (this *RowCopy) GetCurrentPrimaryRangeValueMap() (map[string]*matemap.PrimaryRangeValue, error) {

    currentPrimaryRangeValueMap := make(map[string]*matemap.PrimaryRangeValue)

    // 循环还需要进行row copy的表并且生成. 当前还需要的 ID 范围信息
    for tableName, _ := range this.NeedGetPrimaryRangeValueMap {
        if tableMap, ok := this.ConfigMap.TableMapMap[tableName]; ok { // 有数据. 获取表当前已经row copy 到的主键范围
            // 定义当前表的已经 row copy 到的ID范围
            currPrimaryMap := make(map[string]interface{})

            // 如果之前没有生成过 row copy 的主键范围, 需要去数据库中获取
            if !tableMap.CurrIDValue.Valid || strings.TrimSpace(tableMap.CurrIDValue.String) == "" {
            }
        } else {
            errMSG := fmt.Sprintf("%v: 失败. 初始化还需要进行row copy的已经他的ID范围. %v." +
                "在需要迁移的表变量中有, 在数据库table_map中没有",
                common.CurrLine(), tableName)
            return nil, errors.New(errMSG)
        }
    }

    return currentPrimaryRangeValueMap, nil
}

/* 获取指定表的第一条记录的 ID 值 map
Params:
    _host: 实例 host
    _port:  实例 port
    _schema: 数据库
    _table: 表
 */
func GetTableFirstPrimaryMap(_host string, _port int, _schema string,
    _table string) (map[string]interface{}, error) {

    firstPrimaryMap := make(map[string]interface{})

    migrationTable, err := matemap.GetMigrationTableBySchemaTable(_schema, _table)
    if err != nil {
        errMSG := fmt.Sprintf("%v: 失败. 获取迁移的表信息. %v.%v", _schema, _table)
        return nil, errors.New(errMSG)
    }

    instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
    if err != nil {
    	errMSG := fmt.Sprintf("%v: 失败. 查询表 %v.%v 第一条数据(获取实例). %v",
    	    common.CurrLine(), _schema, _table, err)
        return nil, errors.New(errMSG)
    }

    selectSql := migrationTable.GetSelFirstPKSqlTpl()

    row := instance.DB.QueryRow(selectSql)

    return firstPrimaryMap, nil
}