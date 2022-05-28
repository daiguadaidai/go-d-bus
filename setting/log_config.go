package setting

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DefaultLogFilename   = "./go-d-bus.log"
	DefaultLogMaxSize    = 512
	DefaultLogMaxBackups = 50
	DefaultLogMaxAge     = 7
	DefaultLogCompress   = false
	DefaultLogConsole    = false
	DefaultLogLevel      = DEBUG_LEVEL_STR
)

const (
	DEBUG_LEVEL_STR  = "debug"
	INFO_LEVEL_STR   = "info"
	WARN_LEVEL_STR   = "warn"
	ERROR_LEVEL_STR  = "error"
	DPANIC_LEVEL_STR = "dpanic"
	PANIC_LEVEL_STR  = "panic"
	FATAL_LEVEL_STR  = "fatal"
)

var logLevelStrToIntMap = map[string]zapcore.Level{
	DEBUG_LEVEL_STR:  zap.DebugLevel,
	INFO_LEVEL_STR:   zap.InfoLevel,
	WARN_LEVEL_STR:   zap.WarnLevel,
	ERROR_LEVEL_STR:  zap.ErrorLevel,
	DPANIC_LEVEL_STR: zap.DPanicLevel,
	PANIC_LEVEL_STR:  zap.PanicLevel,
	FATAL_LEVEL_STR:  zap.FatalLevel,
}

type LogConfig struct {
	LogFilename   string `json:"log_filename" toml:"log_filename"`       // 日志文件
	LogLevel      string `json:"log_level" toml:"log_level"`             // 日志级别
	LogMaxSize    int    `json:"log_max_size" toml:"log_max_size"`       // 文件最大大小(单位: M)
	LogMaxBackups int    `json:"log_max_backups" toml:"log_max_backups"` // 日志文件最多保存多少个备份
	LogMaxAge     int    `json:"log_max_age" toml:"log_max_age"`         // 文件最多保存多少天
	LogCompress   bool   `json:"log_compress" toml:"log_compress"`       // 是否压缩
	LogConsole    bool   `json:"log_console" toml:"log_console"`         // 是否打印到控制台
}

func (this *LogConfig) DeepClone() (*LogConfig, error) {
	raw, err := json.Marshal(this)
	if err != nil {
		return nil, fmt.Errorf("日志配置DeepClone出错, LogConfig -> Json: %v", err.Error())
	}

	var logConfig LogConfig
	err = json.Unmarshal(raw, &logConfig)
	if err != nil {
		return nil, fmt.Errorf("启动配置DeepClone出错, Json -> LogConfig: %v", err.Error())
	}

	return &logConfig, nil
}

func (this *LogConfig) GetLogLevel() string {
	_, ok := logLevelStrToIntMap[this.LogLevel]
	if !ok {
		return INFO_LEVEL_STR
	}

	return this.LogLevel
}

func (this *LogConfig) GetLogLevelZap() zapcore.Level {
	return logLevelStrToIntMap[this.GetLogLevel()]
}
