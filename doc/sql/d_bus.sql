--
-- Table structure for table `column_map`
--

DROP TABLE IF EXISTS `column_map`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `column_map` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `task_uuid` varchar(22) NOT NULL COMMENT '迁移任务UUID',
  `schema` varchar(100) NOT NULL COMMENT '源 schema 名称',
  `table` varchar(100) NOT NULL COMMENT '源 table 名称',
  `source` varchar(100) NOT NULL COMMENT '源 column 名称',
  `target` varchar(100) NOT NULL COMMENT '目标 column 名称',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_uuid_tbl_che_src` (`task_uuid`,`schema`,`table`,`source`),
  KEY `idx_uuid_tbl_che_std` (`task_uuid`,`schema`,`table`,`target`),
  KEY `created_at` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `data_checksum`
--

DROP TABLE IF EXISTS `data_checksum`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `data_checksum` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `task_uuid` varchar(22) NOT NULL COMMENT '迁移任务UUID',
  `source_schema` varchar(100) NOT NULL COMMENT '源 schema 名称',
  `source_table` varchar(100) NOT NULL COMMENT '源 table 名称',
  `target_schema` varchar(100) NOT NULL COMMENT '目标 schema 名称',
  `target_table` varchar(100) NOT NULL COMMENT '目标 table 名称',
  `min_id_value` varchar(200) DEFAULT NULL COMMENT 'id范围最小值',
  `max_id_value` varchar(200) DEFAULT NULL COMMENT 'id范围最大值',
  `is_fix` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否修复',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_task_uuid` (`task_uuid`),
  KEY `idx_uuid_source_schema_table` (`task_uuid`,`source_schema`,`source_table`),
  KEY `idx_uuid_target_schema_table` (`task_uuid`,`target_schema`,`target_table`),
  KEY `idx_updated_at` (`updated_at`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `schema_map`
--

DROP TABLE IF EXISTS `schema_map`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `schema_map` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `task_uuid` varchar(22) NOT NULL COMMENT '迁移任务UUID',
  `source` varchar(100) NOT NULL COMMENT '源schema名称',
  `target` varchar(100) NOT NULL COMMENT '目标schema名称',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_uuid_source` (`task_uuid`,`source`),
  KEY `idx_uuid_target` (`task_uuid`,`target`),
  KEY `created_at` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `source`
--

DROP TABLE IF EXISTS `source`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `source` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `task_uuid` varchar(22) NOT NULL COMMENT '迁移任务UUID',
  `host` varchar(15) NOT NULL COMMENT '链接数据库 host',
  `port` smallint(6) NOT NULL COMMENT '链接数据库 port',
  `user` varchar(30) NOT NULL COMMENT '链接数据库 user',
  `passwd` varchar(64) NOT NULL COMMENT '链接数据库 passwd',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `log_file` varchar(20) DEFAULT NULL COMMENT '当前binlog应用位点',
  `log_pos` bigint(20) DEFAULT NULL COMMENT '当前binlog应用位点',
  `start_log_file` varchar(20) DEFAULT NULL COMMENT '开始binlog应用位点',
  `start_log_pos` bigint(20) DEFAULT NULL COMMENT '开始binlog应用位点',
  `parse_log_file` varchar(20) DEFAULT NULL COMMENT '解析到binlog应用位点',
  `parse_log_pos` bigint(20) DEFAULT NULL COMMENT '解析到binlog应用位点',
  `stop_log_file` varchar(20) DEFAULT NULL COMMENT '停止binlog应用位点',
  `stop_log_pos` bigint(20) DEFAULT NULL COMMENT '停止binlog应用位点',
  PRIMARY KEY (`id`),
  KEY `idx_task_uuid` (`task_uuid`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `table_map`
--

DROP TABLE IF EXISTS `table_map`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `table_map` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `task_uuid` varchar(22) NOT NULL COMMENT '迁移任务UUID',
  `schema` varchar(100) NOT NULL COMMENT '源 schema 名称',
  `source` varchar(100) NOT NULL COMMENT '源 table 名称',
  `target` varchar(100) NOT NULL COMMENT '目标 table 名称',
  `row_copy_complete` tinyint(4) NOT NULL DEFAULT '0' COMMENT '表 row copy 是否完成',
  `max_id_value` varchar(200) DEFAULT NULL COMMENT '表需要row copy 到哪一行',
  `curr_id_value` varchar(200) DEFAULT NULL COMMENT '表当前row copy到哪一行',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_uuid_schema_source` (`task_uuid`,`schema`,`source`),
  KEY `idx_uuid_schema_target` (`task_uuid`,`schema`,`target`),
  KEY `created_at` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=185 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `target`
--

DROP TABLE IF EXISTS `target`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `target` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `task_uuid` varchar(22) NOT NULL COMMENT '迁移任务UUID',
  `host` varchar(15) NOT NULL COMMENT '链接数据库 host',
  `port` smallint(6) NOT NULL COMMENT '链接数据库 port',
  `user` varchar(30) NOT NULL COMMENT '链接数据库 user',
  `passwd` varchar(64) NOT NULL COMMENT '链接数据库 passwd',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `log_file` varchar(20) DEFAULT NULL COMMENT '当前binlog应用位点',
  `log_pos` bigint(20) DEFAULT NULL COMMENT '当前binlog应用位点',
  PRIMARY KEY (`id`),
  KEY `idx_task_uuid` (`task_uuid`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_uuid_host_port` (`task_uuid`,`host`,`port`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `task`
--

DROP TABLE IF EXISTS `task`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `task_uuid` varchar(22) NOT NULL COMMENT '迁移任务UUID',
  `type` tinyint(4) NOT NULL DEFAULT '1' COMMENT '任务类型: 1.普通迁移, 2.sharding_o2m, 3.sharding_m2m',
  `name` varchar(30) DEFAULT NULL COMMENT '迁移名称, 用来描述一个迁移任务',
  `heartbeat_schema` varchar(30) NOT NULL DEFAULT 'dbmonitor' COMMENT '心跳检测数据库',
  `heartbeat_table` varchar(30) NOT NULL DEFAULT 'dbmonitor' COMMENT '心跳检测表',
  `pause` varchar(10) DEFAULT NULL COMMENT '暂停: NULL/immediate/normal',
  `run_status` tinyint(4) NOT NULL DEFAULT '4' COMMENT '1.receive(刚接收到), 2.ready, 3.running, 4.stop, 11.停滞接收, 12.停滞准备',
  `is_complete` tinyint(4) NOT NULL DEFAULT '0' COMMENT '迁移是否完成: 0:否, 1:是',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `row_copy_limit` int(11) NOT NULL DEFAULT '1000' COMMENT 'row copy每批应用的行数',
  `run_host` varchar(15) DEFAULT NULL COMMENT '任务运行在哪个机器上',
  `row_copy_complete` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'row copy 是否完成:0否 1是',
  `row_high_water_mark` int(11) NOT NULL DEFAULT '10000' COMMENT '队列中超过多少数据, 进行等待, 默认 1w',
  `row_low_water_mark` int(11) NOT NULL DEFAULT '2000' COMMENT '队列中少于2000, 进行开始继续解析, 默认 2k',
  `start_time` datetime DEFAULT NULL COMMENT '任务开始时间',
  `row_copy_paraller` tinyint(3) unsigned NOT NULL DEFAULT '10' COMMENT 'row copy 并发数',
  `binlog_paraller` tinyint(3) unsigned NOT NULL DEFAULT '15' COMMENT '应用binlog 并发数',
  `checksum_paraller` tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT 'checksum 并发数',
  `checksum_fix_paraller` tinyint(3) unsigned NOT NULL DEFAULT '1' COMMENT 'checksum 修复数据并发数',
  PRIMARY KEY (`id`),
  UNIQUE KEY `udx_task_uuid` (`task_uuid`),
  KEY `idx_name` (`name`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `task_host`
--

DROP TABLE IF EXISTS `task_host`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task_host` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `host` varchar(15) DEFAULT NULL COMMENT '可用的服务器',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `curr_process_cnt` smallint(6) NOT NULL DEFAULT '0' COMMENT '当前进程数',
  `is_available` tinyint(4) NOT NULL DEFAULT '1' COMMENT '是否可用:0.否, 1.是',
  `idc` varchar(3) NOT NULL DEFAULT '' COMMENT 'IDC',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COMMENT='bid可用的任务机器';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `task_run_history`
--

DROP TABLE IF EXISTS `task_run_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task_run_history` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `task_uuid` varchar(22) NOT NULL COMMENT '迁移任务UUID',
  `type` tinyint(4) NOT NULL DEFAULT '1' COMMENT '任务类型: 1.普通迁移, 2.sharding_o2m, 3.sharding_m2m',
  `name` varchar(30) DEFAULT NULL COMMENT '迁移名称, 用来描述一个迁移任务',
  `heartbeat_schema` varchar(30) NOT NULL DEFAULT 'dbmonitor' COMMENT '心跳检测数据库',
  `heartbeat_table` varchar(30) NOT NULL DEFAULT 'dbmonitor' COMMENT '心跳检测表',
  `pause` varchar(10) DEFAULT NULL COMMENT '暂停: NULL/immediate/normal',
  `run_status` tinyint(4) NOT NULL DEFAULT '4' COMMENT '1.receive(刚接收到), 2.ready, 3.running, 4.stop, 11.停滞接收, 12.停滞准备',
  `is_complete` tinyint(4) NOT NULL DEFAULT '0' COMMENT '迁移是否完成: 0:否, 1:是',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `row_copy_limit` int(11) NOT NULL DEFAULT '1000' COMMENT 'row copy每批应用的行数',
  `run_host` varchar(15) DEFAULT NULL COMMENT '任务运行在哪个机器上',
  `row_copy_complete` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'row copy 是否完成:0否 1是',
  `row_high_water_mark` int(11) NOT NULL DEFAULT '10000' COMMENT '队列中超过多少数据, 进行等待, 默认 1w',
  `row_low_water_mark` int(11) NOT NULL DEFAULT '2000' COMMENT '队列中少于2000, 进行开始继续解析, 默认 2k',
  `start_time` datetime DEFAULT NULL COMMENT '任务开始时间',
  `row_copy_paraller` tinyint(3) unsigned NOT NULL DEFAULT '10' COMMENT 'row copy 并发数',
  `binlog_paraller` tinyint(3) unsigned NOT NULL DEFAULT '15' COMMENT '应用binlog 并发数',
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_task_uuid` (`task_uuid`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8mb4 COMMENT='任务启动记录';
