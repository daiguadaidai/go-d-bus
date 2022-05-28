# go-d-bus
MySQL传输数据工具

### 使用方法

在项目中`doc`目录下有需要的表结构

1. 写入需要迁移的源数据
```
-- truncate table d_bus.task;
-- truncate table d_bus.source;
-- truncate table d_bus.target;
-- truncate table d_bus.schema_map;
-- truncate table d_bus.table_map;

INSERT INTO d_bus.task VALUES
(NULL, 
 '20180204151900nb6VqFhl', -- 任务UUID
 1, '迁移测试', 'dbmonitor', 'heartbeat_table', NULL, 4, 0, NOW(), NOW(),
 100, -- 每次row copy行数, 实际row copy会比这个数字少1行
 NULL,
 0, -- 需要迁移的表是否都完成了row copy
 20000, 4000,
 NULL,
 4, -- row copy 并发数
 4, -- 应用binlog并发数
 1, -- 数据校验并发数
 1 -- 修复不一致数据并发数
);

INSERT INTO d_bus.source VALUES
(NULL, '20180204151900nb6VqFhl',
 '127.0.0.1', -- 源数据库ip
 3306, -- 源数据库端口
 'HH', -- 源数据库用户名
 'oracle12', -- 源数据库密码
 NOW(), NOW(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO d_bus.target VALUES
(NULL, '20180204151900nb6VqFhl',
 '127.0.0.1', -- 目标数据库ip
 3306, -- 目标数据库端口
 'HH', -- 目标数据库用户名
 'oracle12', -- 目标数据库密码
 NOW(), NOW(), NULL, NULL);

INSERT INTO d_bus.schema_map VALUES(NULL, '20180204151900nb6VqFhl',
 'employees', -- 需要迁移的源数据库名
 'test', -- 需要迁移的目标数据库名
 NOW(), NOW());

INSERT INTO d_bus.table_map VALUES
(NULL, '20180204151900nb6VqFhl',
 'employees', -- 源数据库数据库名
 'employees_bak', -- 源数据库需要迁移的表名
 'employees', -- 目标数据库的表名
 0, -- 该表的row copy是否完成
 NULL, NULL, NOW(), NOW());
```

2. 运行命令

**编译**

```
go build
```

**运行**

```
# 迁移的元数据库
--mysql-host=127.0.0.1 
--mysql-port=3306
--mysql-username="HH"
--mysql-password="oracle12"
--mysql-database="d_bus"

--task-uuid=20180204151900nb6VqFhl # 需要迁移的 任务UUID
--enable-checksum=false # 是否进行checksum
--enable-row-copy=true # 是否进行row copy
--enable-apply-binlog=true # 是否进行应用binlog

./go-d-bus run \
    --mysql-host=127.0.0.1 \
    --mysql-port=3306 \
    --mysql-username="HH" \
    --mysql-password="oracle12" \
    --mysql-database="d_bus" \
    --task-uuid=20180204151900nb6VqFhl \
    --enable-checksum=false \
    --enable-row-copy=true \
    --enable-apply-binlog=true
```
