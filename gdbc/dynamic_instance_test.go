package gdbc

import (
	"fmt"
	"github.com/daiguadaidai/go-d-bus/setting"
	"github.com/outbrain/golib/log"
	"sync"
	"testing"
	"time"
)

func TestSetAndGetDynamicConfig(t *testing.T) {
	// 创建一个DBConfig
	dbConfig := new(setting.DBConfig)
	dbConfig.Username = "HH"
	dbConfig.Password = "oracle"
	dbConfig.Host = "10.10.10.12"
	dbConfig.Port = 3306
	dbConfig.Database = "test"
	dbConfig.AutoCommit = true
	dbConfig.AllowOldPasswords = 1
	dbConfig.CharSet = "utf8,utf8mb4"
	dbConfig.MaxOpenConns = 500
	dbConfig.MaxIdelConns = 250

	fmt.Println(dbConfig)

	// 设置DBConfig
	err := SetDynamicConfig(dbConfig)
	if err != nil {
		t.Fatalf("设置数据库文件失败: %v", err)
	}

	// 获取数据库配置文件
	dbConfig2, ok := GetDynamicConifgByHostPort(dbConfig.Host, dbConfig.Port)
	if !ok {
		t.Fatalf("获取不到指定配置信息. %v:%v", dbConfig.Host, dbConfig.Port)
	}
	fmt.Println(dbConfig2)
}

func TestGetDynamicInstanceByHostPort(t *testing.T) {
	dynamicConfig := setConfig()

	wg := new(sync.WaitGroup)

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func(_wg *sync.WaitGroup) {
			defer wg.Done()

			dynamicInstance, err := GetDynamicInstanceByHostPort(dynamicConfig.Host, dynamicConfig.Port)
			if err != nil {
				t.Fatal(err)
			}

			log.Infof("%v", dynamicInstance)

			tx, err := dynamicInstance.DB.Begin()

			update_sql := "update test.t2 set actor_id=555 where id=40011"
			result, err := tx.Exec(update_sql)

			sql := "SELECT SLEEP(60)"
			result, err = tx.Exec(sql)

			update_sql = "update test.t2 set actor_id=666 where id=40011"
			result, err = tx.Exec(update_sql)

			tx.Commit()

			log.Infof("%v", result)

		}(wg)
	}

	wg.Wait()
}

func TestGetDynamicInstanceByHostPort2(t *testing.T) {
	dynamicConfig := setConfig()

	wg := new(sync.WaitGroup)

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func(_wg *sync.WaitGroup) {
			defer wg.Done()

			dynamicInstance, err := GetDynamicInstanceByHostPort(dynamicConfig.Host, dynamicConfig.Port)
			if err != nil {
				t.Fatal(err)
			}

			log.Infof("%v", dynamicInstance)

			sql := "SELECT SLEEP(1)"
			result, err := dynamicInstance.DB.Query(sql)
			result.Close()

			log.Infof("%v", result)

		}(wg)
	}

	wg.Wait()

	time.Sleep(time.Second * 10)

	query(dynamicConfig.Host, dynamicConfig.Port)
}

func query(_host string, _port int) {
	dynamicInstance, err := GetDynamicInstanceByHostPort(_host, _port)
	if err != nil {
		log.Fatalf("query: %v", err)
	}

	log.Infof("query: %v", dynamicInstance)

	sql := "SELECT SLEEP(60)"
	result, err := dynamicInstance.DB.Query(sql)
	if err != nil {
		log.Fatalf("query: %v", err)
	}

	log.Infof("query: %v", result)

}

func setConfig() *setting.DBConfig {
	dbConfig := new(setting.DBConfig)
	dbConfig.Username = "HH"
	dbConfig.Password = "oracle"
	dbConfig.Host = "10.10.10.12"
	dbConfig.Port = 3306
	dbConfig.Database = "test"
	dbConfig.AutoCommit = true
	dbConfig.AllowOldPasswords = 1
	dbConfig.CharSet = "utf8,utf8mb4"
	dbConfig.MaxOpenConns = 1
	dbConfig.MaxIdelConns = 1

	// 设置DBConfig
	err := SetDynamicConfig(dbConfig)
	if err != nil {
		log.Fatalf("设置数据库文件失败: %v", err)
	}

	log.Infof("设置数据库配置成功! %v", dbConfig)

	return dbConfig
}
