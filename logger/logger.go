package logger

import (
	"github.com/daiguadaidai/go-d-bus/setting"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"strings"
)

// 使用
// logger.M.Info("打印日志 Info")
// logger.M.Debug("打印日志 Debug")
// logger.M.Error("打印日志 Error")
var M *zap.SugaredLogger

func InitLogger(logConfig *setting.LogConfig) {
	writeSyncers := getLoggerSyncers(logConfig)

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// 设置日志级别
	atomicLevel := zap.NewAtomicLevel()
	atomicLevel.SetLevel(logConfig.GetLogLevelZap())

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),     // 编码器配置
		zapcore.NewMultiWriteSyncer(writeSyncers...), // 打印到控制台和文件
		atomicLevel, // 日志级别
	)

	// 开启开发模式，堆栈跟踪
	caller := zap.AddCaller()
	// 开启文件及行号
	development := zap.Development()
	// 构造日志
	tmpLogger := zap.New(core, caller, development)
	M = tmpLogger.Sugar()
}

func getLoggerSyncers(logConfig *setting.LogConfig) []zapcore.WriteSyncer {
	syncers := make([]zapcore.WriteSyncer, 0, 1)

	// 添加文件输出日志
	if strings.TrimSpace(logConfig.LogFilename) != "" {
		fileSyncer := &lumberjack.Logger{
			Filename:   logConfig.LogFilename,   // 日志文件路径
			MaxSize:    logConfig.LogMaxSize,    // 每个日志文件保存的最大尺寸 单位：M
			MaxBackups: logConfig.LogMaxBackups, // 日志文件最多保存多少个备份
			MaxAge:     logConfig.LogMaxAge,     // 文件最多保存多少天
			Compress:   logConfig.LogCompress,   // 是否压缩
		}
		syncers = append(syncers, zapcore.AddSync(fileSyncer))
	}

	// 没有指定文件 或 指定了控制台输出, 则添加控制台输出日志
	if logConfig.LogConsole || strings.TrimSpace(logConfig.LogFilename) == "" {
		syncers = append(syncers, zapcore.AddSync(os.Stdout))
	}

	return syncers
}
