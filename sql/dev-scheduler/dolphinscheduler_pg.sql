CREATE TABLE t_ds_process_dependent (
    id int NOT NULL ,
    dependent_id int NOT NULL ,
    process_id int NOT NULL ,
    create_time timestamp DEFAULT NULL ,
    update_time timestamp DEFAULT NULL ,
    PRIMARY KEY (id)
);
create unique index t_ds_process_dependent_id_process_id_IDX on t_ds_process_dependent (dependent_id, process_id);
create index t_ds_process_dependent_dependent_id_IDX on t_ds_process_dependent (dependent_id);
create index t_ds_process_dependent_process_id_IDX on t_ds_process_dependent (process_id);

CREATE SEQUENCE  t_ds_process_dependent_id_sequence;
ALTER TABLE t_ds_process_dependent ALTER COLUMN id SET DEFAULT NEXTVAL('t_ds_process_dependent_id_sequence');

ALTER TABLE t_ds_command ADD COLUMN scheduler_interval int default 9;
ALTER TABLE t_ds_command ADD COLUMN scheduler_batch_no int default 0;
ALTER TABLE t_ds_command ADD COLUMN dependent_scheduler_flag boolean default false;

ALTER TABLE t_ds_process_instance ADD COLUMN scheduler_interval int default 9;
ALTER TABLE t_ds_process_instance ADD COLUMN scheduler_batch_no int default 0;
ALTER TABLE t_ds_process_instance ADD COLUMN process_type int default 0;
ALTER TABLE t_ds_process_instance ADD COLUMN dependent_scheduler_flag boolean default false;

ALTER TABLE t_ds_process_definition ADD COLUMN process_type int default 0;

-- 调度重跑功能
ALTER TABLE t_ds_command ADD COLUMN scheduler_start_id int default 0;

ALTER TABLE t_ds_process_instance ADD COLUMN scheduler_start_id int default 0;

ALTER TABLE t_ds_command ADD COLUMN scheduler_rerun_no varchar(64) DEFAULT NULL;

ALTER TABLE t_ds_process_instance ADD COLUMN scheduler_rerun_no varchar(64) DEFAULT NULL;

ALTER TABLE t_ds_command ADD COLUMN rerun_scheduler_flag boolean default false;

ALTER TABLE t_ds_process_instance ADD COLUMN rerun_scheduler_flag boolean default false;
