truncate table d_bus.task;
truncate table d_bus.source;
truncate table d_bus.target;
truncate table d_bus.schema_map;
truncate table d_bus.table_map;

INSERT INTO d_bus.task VALUES
(NULL, '20180204151900nb6VqFhl', 1, '迁移测试', 'dbmonitor', 'heartbeat_table', NULL, 4, 0, NOW(), NOW(), 100, NULL, 0, 20000, 4000, NULL, 4, 4, 1, 1);
INSERT INTO d_bus.source VALUES
(NULL, '20180204151900nb6VqFhl', '127.0.0.1', 3306, 'HH', 'oracle12', NOW(), NOW(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO d_bus.target VALUES
(NULL, '20180204151900nb6VqFhl', '127.0.0.1', 3306, 'HH', 'oracle12', NOW(), NOW(), NULL, NULL);
INSERT INTO d_bus.schema_map VALUES(NULL, '20180204151900nb6VqFhl', 'employees', 'test', NOW(), NOW());
INSERT INTO d_bus.table_map VALUES
(NULL, '20180204151900nb6VqFhl', 'employees', 'employees_bak', 'employees', 0, NULL, NULL, NOW(), NOW());