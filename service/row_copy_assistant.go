package service

import (
    "github.com/daiguadaidai/go-d-bus/matemap"
    "github.com/daiguadaidai/go-d-bus/model"
    "github.com/daiguadaidai/go-d-bus/common"
    "fmt"
    "github.com/juju/errors"
    "strings"
	"github.com/outbrain/golib/log"
)

// 获取需要的所有需要生成范围ID的表
func GetNeedGetPrimaryRangeValueMap(_tableMapMap map[string]model.TableMap) (map[string]bool, error) {
    // 获取所有需要迁移的表
    migrationTableNameMap := matemap.FindAllMigrationTableNameMap()

    // 还需要生成 rowcopy 主键范围的表
    needGetPrimaryRangeValueMap := make(map[string]bool)

    for tableName, _ := range migrationTableNameMap {
    	if tableMap, ok := _tableMapMap[tableName]; ok { // 该表是确认要迁移的
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
            currPrimaryRangeMap, err := common.Json2Map(tableMap.CurrIDValue.String)
            if err != nil {
            	log.Warningf("%v: 失败. 解析row copy 当前应用到的id 范围 json. value: %v. %v",
            	    common.CurrLine(), tableMap.CurrIDValue.String, err)
                needGetPrimaryRangeValueMap[tableName] = true
                continue
            }

            // 获取row copy 截止的id范围Map
            maxPrimaryRangeMap, err := common.Json2Map(tableMap.MaxIDValue.String)
            if err != nil {
                log.Warningf("%v: 失败. 解析row copy 截止的id 范围 json. value: %v. %v",
                    common.CurrLine(), tableMap.MaxIDValue.String, err)
                needGetPrimaryRangeValueMap[tableName] = true
                continue
            }

            // 比较当前row copy 完成的id范围是否大于等于 需要截止的id范围
            if common.MapAGreaterOrEqualMapB(currPrimaryRangeMap, maxPrimaryRangeMap) { // 该表row copy完成
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

/* 获取需要迁移的表 当前row copy到的进度 id范围值
Params:
    _needRowCopyTables: 未完成row copy的表
    _tableMapMap: 数据库中所有需要迁移的表
 */
func GetCurrentPrimaryRangeValueMap(_needRowCopyTables map[string]bool,
    _tableMapMap map[string]model.TableMap) (map[string]*matemap.PrimaryRangeValue, error) {

    currentPrimaryRangeValueMap := make(map[string]*matemap.PrimaryRangeValue)

    return currentPrimaryRangeValueMap, nil
}