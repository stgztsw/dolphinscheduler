CREATE TABLE t_ds_process_dependent (
                                        `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
                                        `dependent_id` int(11) NOT NULL COMMENT 'dependent process id',
                                        `process_id` int(11) NOT NULL COMMENT 'process id',
                                        `create_time` datetime DEFAULT NULL COMMENT 'create time',
                                        `update_time` datetime DEFAULT NULL COMMENT 'update time',
                                        PRIMARY KEY (`id`),
                                        UNIQUE KEY `t_ds_process_dependent_id_process_id_IDX` (`dependent_id`,`process_id`) USING BTREE,
                                        KEY `t_ds_process_dependent_dependent_id_IDX` (`dependent_id`) USING BTREE,
                                        KEY `t_ds_process_dependent_process_id_IDX` (`process_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

alter table t_ds_command
    add scheduler_interval tinyint default 9 null comment 'scheduler interval';
alter table t_ds_command
    add scheduler_batch_no int default 0 null comment 'scheduler batch no';

alter table t_ds_process_instance
    add scheduler_interval tinyint default 9 null comment 'scheduler interval';
alter table t_ds_process_instance
    add scheduler_batch_no int default 0 null comment 'scheduler batch no';

alter table t_ds_process_definition
    add process_type tinyint default 0 null comment 'process type';

alter table t_ds_process_instance
    add process_type tinyint default 0 null comment 'process type';

alter table t_ds_process_instance
    add dependent_scheduler_flag tinyint default 0 null comment 'dependent scheduler flag';

alter table t_ds_command
    add dependent_scheduler_flag tinyint default 0 null comment 'dependent scheduler flag';

-- 调度重跑功能
alter table t_ds_command
    add scheduler_start_id int(11) default 0 null comment 'scheduler start id';

alter table t_ds_process_instance
    add scheduler_start_id int(11) default 0 null comment 'scheduler start id';

alter table t_ds_command
    add scheduler_rerun_no varchar(64) comment 'scheduler rerun no';

alter table t_ds_process_instance
    add scheduler_rerun_no varchar(64) comment 'scheduler rerun no';

alter table t_ds_command
    add rerun_scheduler_flag tinyint default 0 null comment 'rerun scheduler flag';

alter table t_ds_process_instance
    add rerun_scheduler_flag tinyint default 0 null comment 'rerun scheduler flag';

-- normal节点设置定时
alter table t_ds_process_instance
    add dependent_scheduler_type tinyint default 0 null comment 'dependent scheduler type';

alter table t_ds_command
    add dependent_scheduler_type tinyint default 0 null comment 'dependent scheduler type';
-- 历史数据修正
update t_ds_process_instance set dependent_scheduler_type = 1 where dependent_scheduler_flag = true