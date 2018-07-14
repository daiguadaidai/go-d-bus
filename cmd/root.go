// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/daiguadaidai/go-d-bus/parser"
	"github.com/outbrain/golib/log"
	"github.com/daiguadaidai/go-d-bus/service"
	"github.com/liudng/godump"
)

var runParser *parser.RunParser

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "go-d-bus",
	Short: "MySQL异构数据迁移工具",
	Long: `
    一款基于 Go 开发的 MySQL 异构数据迁移一工具.
    该迁移工具模拟了 MySQL Slave 行为 对数据进行迁移.
    `,
}

// 启动一个迁移任务, runCmd 是 rootCmd 的一个子命令
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "启动一个迁移任务",
	Long: `用于启动一个迁移任务:

./go-d-bus run --task-uuid=20180204151900nb6VqFhl

./go-d-bus run \
    --task-uuid=20180204151900nb6VqFhl \
    --start-log-file=mysql-bin.0000001 \
    --start-log-pos=120 \
    --stop-log-file=mysql-bin.0000002 \
    --stop-log-pos=0 \
    --enable-apply-binlog=true \
    --enable-row-copy=true \
    --apply-binlog-paraller=8 \
    --row-copy-paraller=8 \
    --binlog-apply-water-mark=10000 \
    --row-copy-water-mark=100 \
    --row-copy-limit=1000 \
    --heartbeat-schema=dbmonitor \
    --heartbeat-table=heartbeat_table
    
    `,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		// 检测命令行输入的参数
		err := runParser.Parse()
		if err != nil {
			log.Fatalf("%v", err)
		}

		godump.Dump(runParser)

		// 开始迁移
		service.StartMigration(runParser)

	},
}

// 用于回滚数据, rollbackCmd 是 rootCmd 的一个子命令
var rollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "回滚数据",
	Long: ` 
    回滚数据是基于binlog进行的, 数据流向: (目标 -> 源):
    go-d-bus rollback
    `,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {

	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// 添加 run, rollabck 子命令
	rootCmd.AddCommand(runCmd, rollbackCmd)

    // 接收 run 命令 flags
    runParser = new(parser.RunParser)
    runCmd.Flags().StringVar(&runParser.TaskUUID, "task-uuid", "",
    	"需要运行的任务 UUID")

	runCmd.Flags().StringVar(&runParser.StartLogFile, "start-log-file", "",
		"运行任务开始应用 binlog 的文件")
	runCmd.Flags().IntVar(&runParser.StartLogPos, "start-log-pos", -1,
		"运行任务开始应用 binlog 的位点")

	runCmd.Flags().StringVar(&runParser.StopLogFile, "stop-log-file", "",
		"任务停止应用 binlog 的文件")
	runCmd.Flags().IntVar(&runParser.StopLogPos, "stop-log-pos", -1,
		"任务停止应用 binlog 的位点")

	runCmd.Flags().IntVar(&runParser.ApplyBinlogParaller, "apply-binlog-paraller",
		-1, "应用binglog的并发数")
	runCmd.Flags().IntVar(&runParser.RowCopyParaller, "row-copy-paraller",
		-1, "进行数据拷贝(row copy)的并发数")

	runCmd.Flags().BoolVar(&runParser.EnableApplyBinlog, "enable-apply-binlog",
		true, "是否进行应用binlog")
	runCmd.Flags().BoolVar(&runParser.EnableRowCopy, "enable-row-copy",
		true, "是否进行数据拷贝(row copy)")

	runCmd.Flags().IntVar(&runParser.ApplyBinlogHighWaterMark, "binlog-apply-water-mark",
		-1, "应用binlog队列缓存最大个数")
	runCmd.Flags().IntVar(&runParser.RowCopyHighWaterMark, "row-copy-water-mark",
		-1, "数据拷贝(row copy)队列缓存最大个数")

	runCmd.Flags().IntVar(&runParser.RowCopyLimit, "row-copy-limit",
		-1, "每次数据拷贝(row copy)的行数")

	runCmd.Flags().StringVar(&runParser.HeartbeatSchema, "heartbeat-schema", "",
		"心跳数据库")
	runCmd.Flags().StringVar(&runParser.HeartbeatTable, "heartbeat-table", "",
		"心跳表 该表的数据不会被应用, 主要是为了解析的位点能不段变, 应用的位点有可能不变")


    // 接收 rollback 命令 flags
}
