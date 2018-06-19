package service

import (
    "github.com/daiguadaidai/go-d-bus/parser"
    "github.com/daiguadaidai/go-d-bus/config"
    "sync"
)

type ApplyBinlog struct {
    WG *sync.WaitGroup
}

/* 创建一个应用binlog
Params:
    _parser: 命令行解析的信息
    _configMap: 配置信息
    _wg: 并发控制参数
 */
func NewApplyBinlog(_parser *parser.RunParser, _configMap *config.ConfigMap,
    _wg *sync.WaitGroup) (*ApplyBinlog, error){

    applyBinlog := new(ApplyBinlog)

    applyBinlog.WG = _wg

    return applyBinlog, nil
}

func (this *ApplyBinlog) Start() {
    defer this.WG.Done()
}


