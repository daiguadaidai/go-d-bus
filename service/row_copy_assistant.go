package service

import (
    "github.com/daiguadaidai/go-d-bus/matemap"
    "github.com/daiguadaidai/go-d-bus/common"
    "fmt"
    "github.com/juju/errors"
    "strings"
	"github.com/outbrain/golib/log"
    "github.com/daiguadaidai/go-d-bus/gdbc"
    "github.com/daiguadaidai/go-d-bus/dao"
    "github.com/liudng/godump"
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
    	        log.Infof("%v: 完成. 该表已经完成row copy. %v",
    	            common.CurrLine(), tableName)
    	        continue
            }

            needGetPrimaryRangeValueMap[tableName] = true
        } else {
            errMSG := fmt.Sprintf("%v: 失败. 获取还需要进行生成row copy 范围ID失败. " +
                "在需要迁移的表中有, 但是在table_map表中没该表记录: %v", common.CurrLine(), tableName)

            return nil, errors.New(errMSG)
        }
    }

    godump.Dump(needGetPrimaryRangeValueMap)
    return needGetPrimaryRangeValueMap, nil
}

// 获取需要迁移的表的 row copy 截止的主键值
func (this *RowCopy) GetMaxPrimaryRangeValueMap() (map[string]*matemap.PrimaryRangeValue,
    map[string]bool, error) {

    // 每个表row copy截止的主键值
    maxPrimaryRangeValueMap := make(map[string]*matemap.PrimaryRangeValue)
    noDataTables := make(map[string]bool) // 没有数据的表, 代表已经完成

    // 循环获取 没个表 row copy 截止的ID值
    for tableName, _ := range this.NeedRowCopyTableMap {
        if tableMap, ok := this.ConfigMap.TableMapMap[tableName]; ok {
            // 该表没有 截止row copy id值的数据, 从数据库中获取
            if !tableMap.MaxIDValue.Valid || strings.TrimSpace(tableMap.MaxIDValue.String) == "" {
                maxPrimaryMap, err := GetTableLastPrimaryMap(
                    this.ConfigMap.Source.Host.String,
                    int(this.ConfigMap.Source.Port.Int64),
                    tableMap.Schema.String,
                    tableMap.Source.String,
                )
                if err != nil {
                    errMSG := fmt.Sprintf("%v: 失败. 初始化表:%v row copy截止id值. %v",
                        common.CurrLine(), tableName, err)
                    return nil, nil, errors.New(errMSG)
                }
                // 该表没有数据, 不处理, 进行下一个表的row copy 截止ID处理
                if len(maxPrimaryMap) < 1 {
                    noDataTables[tableName] = true
                    log.Warningf("%v: 警告. 该表没有数据, 无法获取到最大主键值. 将设置为row copy 完成. %v",
                        common.CurrLine(), tableName)
                    continue
                }

                // 将row copy截止的主键值保存到数据库中
                maxPrimaryJson, err := common.Map2Json(maxPrimaryMap)
                if err != nil {
                    errMSG := fmt.Sprintf("%v: 失败. 初始化row copy截止id值. map转化成json. %v %v. %v",
                        common.CurrLine(), tableName, maxPrimaryMap, err)
                    return nil, nil, errors.New(errMSG)
                }
                affected := UpdateTableMaxPrimaryValue(this.ConfigMap.TaskUUID, tableMap.Schema.String,
                    tableMap.Source.String, maxPrimaryJson)
                if affected < 1 { // 数据没有更新成功
                    errMSG := fmt.Sprintf("%v: 失败. 初始化表row copy截止主键值, 保存到数据库中 %v",
                        common.CurrLine(), tableName)
                    return nil, nil, errors.New(errMSG)
                }

                maxPrimaryRangeValueMap[tableName] = matemap.NewPrimaryRangeValue(
                    "-1", tableMap.Schema.String,
                    tableMap.Source.String, maxPrimaryMap, maxPrimaryMap)

                log.Infof("%v: 成功. 初始化row copy 最大主键值. 并保存到数据库中. %v",
                    common.CurrLine(), tableName)

            } else { // 在 table_map 表中已经有 row copy 截止主键值 max_id_value
                maxPrimaryMap, err := common.Json2Map(tableMap.MaxIDValue.String)
                if err != nil {
                    errMSG := fmt.Sprintf("%v: 失败. 初始化表: %v row copy 截止id值. json转化map. %v",
                        common.CurrLine(), tableName, err)
                    return nil, nil, errors.New(errMSG)
                }

                maxPrimaryRangeValueMap[tableName] = matemap.NewPrimaryRangeValue(
                    "-1", tableMap.Schema.String,
                    tableMap.Source.String, maxPrimaryMap, maxPrimaryMap)

                log.Infof("%v: 成功. 初始化row copy 最大主键值. 之前已经初始化过. %v",
                    common.CurrLine(), tableName)
            }
        } else { // 在需要row copy的表中有该表, 但是在配置映射信息中没有
            errMSG := fmt.Sprintf("%v: 失败. 初始化row copy截止的ID范围. %v." +
                "在需要迁移的表变量中有, 在数据库table_map中没有",
                common.CurrLine(), tableName)
            return nil, nil, errors.New(errMSG)
        }
    }

    return maxPrimaryRangeValueMap, noDataTables, nil
}

// 获取需要迁移的表 当前row copy到的进度 id范围值
func (this *RowCopy) GetCurrentPrimaryRangeValueMap() (map[string]*matemap.PrimaryRangeValue,
    map[string]bool, error) {

    currentPrimaryRangeValueMap := make(map[string]*matemap.PrimaryRangeValue)
    noDataTables := make(map[string]bool) // 没有数据的表, 代表已经完成

    // 循环还需要进行row copy的表并且生成. 当前还需要的 ID 范围信息
    for tableName, _ := range this.NeedRowCopyTableMap {
        if tableMap, ok := this.ConfigMap.TableMapMap[tableName]; ok { // 有数据. 获取表当前已经row copy 到的主键范围

            // 如果之前没有生成过 row copy 的主键范围, 需要去数据库中获取
            if !tableMap.CurrIDValue.Valid || strings.TrimSpace(tableMap.CurrIDValue.String) == "" {
                // 获取当前表的已经 row copy 到的ID范围
            	currPrimaryMap, err := GetTableFirstPrimaryMap(this.ConfigMap.Source.Host.String,
            	    int(this.ConfigMap.Source.Port.Int64), tableMap.Schema.String, tableMap.Source.String)
            	if err != nil {
                    errMSG := fmt.Sprintf("%v: 失败. 初始化还需要进行row copy的已经他的ID范围. " +
                        "获取该表的最小主键值 %v. %v",
                        common.CurrLine(), tableName, err)
                    return nil, nil, errors.New(errMSG)
                }
                // 数据库中没有该表中没有数据
                if len(currPrimaryMap) == 0 {
                    noDataTables[tableName] = true
                    log.Warningf("%v: 警告. 该表没有数据, 无法获取到最小主键值. 将设置为row copy 完成. %v",
                        common.CurrLine(), tableName)
                    continue
                }

                // 将当前row copy的主键值保存到数据库中
                currPrimaryJson, err := common.Map2Json(currPrimaryMap)
                if err != nil {
                    errMSG := fmt.Sprintf("%v: 失败. 初始化row copy最小id值. 转化称json. %v %v. %v",
                        common.CurrLine(), tableName, currPrimaryMap, err)
                    return nil, nil, errors.New(errMSG)
                }
                affected := UpdateTableCurrPrimaryValue(this.ConfigMap.TaskUUID, tableMap.Schema.String,
                    tableMap.Source.String, currPrimaryJson)
                if affected < 1 { // 数据没有更新成功
                    errMSG := fmt.Sprintf("%v: 失败. 初始化表当前row copy主键值, 保存到数据库中 %v",
                        common.CurrLine(), tableName)
                    return nil, nil, errors.New(errMSG)
                }

                // 生成但前已经rowcopy 到的范围
                currentPrimaryRangeValueMap[tableName] = matemap.NewPrimaryRangeValue(
                    "-1", tableMap.Schema.String,
                    tableMap.Source.String, currPrimaryMap, currPrimaryMap)

                log.Infof("%v: 成功. 初始化当前row copy主键值. 并保存到数据库中. %v",
                    common.CurrLine(), tableName)

            } else { // 在 table_map 表中已经有当前已经完成的 row copy 主键值 curr_min_value
                // 获取当前表的已经 row copy 到的ID范围
                currPrimaryMap, err := common.Json2Map(tableMap.CurrIDValue.String)
                if err != nil {
                    errMSG := fmt.Sprintf("%v: 失败. 转换json数据. 在初始化表已经完成" +
                        "row copy主键值的时候 %v. %v",
                        common.CurrLine(), tableName, err)
                    return nil, nil, errors.New(errMSG)
                }

                // 生成但前已经rowcopy 到的范围
                currentPrimaryRangeValueMap[tableName] = matemap.NewPrimaryRangeValue(
                    "-1", tableMap.Schema.String,
                    tableMap.Source.String, currPrimaryMap, currPrimaryMap)

                log.Infof("%v: 成功. 初始化当前row copy主键值. 数据库中已经有. %v",
                    common.CurrLine(), tableName)
            }
        } else {
            errMSG := fmt.Sprintf("%v: 失败. 初始化还需要进行row copy的已经他的ID范围. %v." +
                "在需要迁移的表变量中有, 在数据库table_map中没有",
                common.CurrLine(), tableName)
            return nil, nil, errors.New(errMSG)
        }
    }

    return currentPrimaryRangeValueMap, noDataTables, nil
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

    selectSql := migrationTable.GetSelFirstPKSqlTpl() // 获取查询表第一条数据的记录 SQL

    row := instance.DB.QueryRow(selectSql)
    firstPrimaryMap, err := common.Row2Map(row, migrationTable.FindSourcePKColumnNames(),
        migrationTable.FindSourcePKColumnTypes())

    return firstPrimaryMap, nil
}

/* 获取指定表的最大记录的 ID 值 map
Params:
    _host: 实例 host
    _port:  实例 port
    _schema: 数据库
    _table: 表
 */
func GetTableLastPrimaryMap(_host string, _port int, _schema string,
    _table string) (map[string]interface{}, error) {

    migrationTable, err := matemap.GetMigrationTableBySchemaTable(_schema, _table)
    if err != nil {
        errMSG := fmt.Sprintf("%v: 失败. 获取迁移的表信息. %v.%v", _schema, _table)
        return nil, errors.New(errMSG)
    }

    instance, err := gdbc.GetDynamicInstanceByHostPort(_host, _port)
    if err != nil {
        errMSG := fmt.Sprintf("%v: 失败. 查询表 %v.%v 最后一条数据(获取实例). %v",
            common.CurrLine(), _schema, _table, err)
        return nil, errors.New(errMSG)
    }

    selectSql := migrationTable.GetSelLastPKSqlTpl() // 获取查询表最后一条数据的记录 SQL

    row := instance.DB.QueryRow(selectSql)
    lastPrimaryMap, err := common.Row2Map(row, migrationTable.FindSourcePKColumnNames(),
        migrationTable.FindSourcePKColumnTypes())

    return lastPrimaryMap, nil
}

/* 该方法用于初始化row copy实例的时候使用. 标记和移除(从需要迁移的表中)表迁移完成
Params:
    _tables: 需要移除的表
 */
func (this *RowCopy) TagCompleteNeedRowCopyTables(_tables map[string]bool) {
    for tableName, _ := range _tables {
        // 标记表执行成功
        tableMapDao := new(dao.TableMapDao)
        _ = tableMapDao.TagTableRowCopyComplete(this.ConfigMap.TaskUUID,
            this.ConfigMap.TableMapMap[tableName].Schema.String,
            this.ConfigMap.TableMapMap[tableName].Source.String)
        log.Warningf("%v: 表: %v, 标记表row copy已经完成.",
            common.CurrLine(), tableName)

        delete(this.NeedRowCopyTableMap, tableName)
        log.Warningf("%v: 表: %v, 从还需要进行row copy的列表中删除成功.",
            common.CurrLine(), tableName)
    }
}

// 获取需要迁移的表中当前row copy 主键值 大于 row copy 截止的主键ID值
func (this *RowCopy) FindCurrGreaterMaxPrimaryTables() map[string]bool {
    greaterTables := make(map[string]bool)

    for tableName, _ := range this.NeedRowCopyTableMap {
        log.Warningf("%v: %v - %v", common.CurrLine(),
            this.CurrentPrimaryRangeValueMap[tableName].MaxValue,
            this.MaxPrimaryRangeValueMap[tableName].MaxValue)

        if common.MapAGreaterOrEqualMapB(
            this.CurrentPrimaryRangeValueMap[tableName].MaxValue,
            this.MaxPrimaryRangeValueMap[tableName].MaxValue,
        ) {
        	log.Warningf("%v: 警告. 表: %v 当前row copy 值 大于等于 截止row copy值. %v >= %v",
        	    common.CurrLine(), tableName,
        	    this.CurrentPrimaryRangeValueMap[tableName].MaxValue,
                this.MaxPrimaryRangeValueMap[tableName].MaxValue)

            greaterTables[tableName] = true
        }
    }

    return greaterTables
}

/* 跟新表当前row copy 到的主键值
Params:
    _taskUUID: 任务ID
    _schema: 数据库名
    _table: 表名
    _jsonData: 需要更新的数据
 */
func UpdateTableCurrPrimaryValue(_taskUUID , _schema, _table, _jsonData string) int {
    tableMapDao := new(dao.TableMapDao)
    affected := tableMapDao.UpdateCurrIDValue(_taskUUID, _schema, _table, _jsonData)
    return affected
}

/* 跟新表row copy 截止的主键值
Params:
    _taskUUID: 任务ID
    _schema: 数据库名
    _table: 表名
    _jsonData: 需要更新的数据
 */
func UpdateTableMaxPrimaryValue(_taskUUID , _schema, _table, _jsonData string) int {
    tableMapDao := new(dao.TableMapDao)
    affected := tableMapDao.UpdateMaxIDValue(_taskUUID, _schema, _table, _jsonData)
    return affected
}
