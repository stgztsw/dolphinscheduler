#!/usr/bin/env python
# -*- coding:utf-8 -*-

import datetime
import json as js
import os
import timedelta
import dateutil
import sys, getopt
import re
import sqlparse

import cx_Oracle
from pyhive import hive

# -------------------配置项---------------------

partition = "pt"
env = "prd"

# reader 支持ftp, oracle, hive
reader_db_type = "ftp"
reader_db_table_name = "dingxiangclinic.ods_f_dxy_clinic_supplier"  # 注意oracle的识别大小写，请确保库表名的大小写和实际一致
mysql_file_path = "/alidata1/admin/jar/createTable/dingxiangClinic"  # table schema 文件夹路径
# writer 支持hive, dorisdb
writer_db_type = "hive"
writer_db_name = "dingxiangclinic"
writer_hive_table_prefix = "ods_f_dxy_"
writer_table_name = None  # 默认策略: hive的场景会在表名前加ods_; dorisdb的场景会在表前加 doris_

haveKerberos = "false"
dorisdb_dynamic_partition_time_unit = "MONTH"  # 可以指定为: DAY/WEEK/MONTH


# -------------------配置项---------------------

def usage():
    print('请输入如下格式的参数：\r')
    print('-e --env=  环境配置\r')
    print('-i --readertype=  输入数据库类型\r')
    print('-t --readertable=  输入数据库和表\r')
    print('-o --writertype=  输出数据库类型\r')
    print('-x --writertable=  输出数据库和表\r')


try:
    options, args = getopt.getopt(sys.argv[1:], '-e:-i:-t:-o:-x:',
                                  ['env=', 'readertype=', 'readertable=', 'writertype=', 'writertable='])
    for name, value in options:
        if name in ('-e', '--env'):
            env = value
        elif name in ('-i', '--readertype'):
            reader_db_type = value
        elif name in ('-t', '--readertable'):
            reader_db_table_name = value
        elif name in ('-o', '--writertype'):
            writer_db_type = value
        elif name in ('-x', '--writertable'):
            writer_table_name = value
except:
    usage()

print("任务配置:")
print("env=%s, reader_db_type=%s, reader_db_table_name=%s, writer_db_type=%s, writer_table_name=%s"
      % (env, reader_db_type, reader_db_table_name, writer_db_type, writer_table_name))
print("如果writer_table_name为None, 会按默认策略生成: hive的场景会在表名前加ods_; dorisdb的场景会在表前加 doris_")

db_infos = {
    "oracle": {
        "uat": {
            "user": "JHBI",
            "password": "JHBIwd",
            "listener": "10.2.13.45:1521/orcl12"
        },
        "prd": {
            "user": "JHBI",
            "password": "JHBIlwd",
            "listener": "10.2.12.131:1558/emrdg"
        }
    },
    "hive": {
        "uat": {
            "host": "10.2.13.3",
            "user": "hive",
            "password": "Byscq123!@#",
            "port": "10000"
        },
        "prd": {
            "host": "10.2.240.243",
            "user": "chacha.tang",
            "password": "Accenture1",
            "port": "10000"
        }
    },
    "dorisdb": {
        "uat": {
            "host": "10.2.13.6",
            "user": "root",
            "password": "root",
            "port": "9030",
            "loadUrl": ["10.2.13.6:8030"],
            "loadProps": {
                "column_separator": "\\x01",
                "row_delimiter": "\\x02"
            }
        },
        "prd": {
            "host": "10.2.240.241",
            "user": "chachatang",
            "password": "Accenture1",
            "port": "9130",
            "loadUrl": ["10.2.240.241:8130"],
            "loadProps": {
                "column_separator": "\\x01",
                "row_delimiter": "\\x02"
            }
        }
    },
    "ftp": {
        "prd": {
            "host": "10.2.25.221",
            "port": 22,
            "username": "user02",
            "password": "JIH@202109",
        }
    }
}

hdfs_infos = {
    "uat": {
        "defaultFS": "hdfs://nameservice1",
        "hadoopConfig": {
            "dfs.nameservices": "nameservice1",
            "dfs.ha.namenodes.nameservice1": "namenode105,namenode80",
            "dfs.namenode.rpc-address.nameservice1.namenode105": "big-data-test003:8020",
            "dfs.namenode.rpc-address.nameservice1.namenode80": "big-data-test004:8020",
            "dfs.client.failover.proxy.provider.nameservice1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        }
    },
    "prd": {
        "defaultFS": "hdfs://nameservice1",
        "hadoopConfig": {
            "dfs.nameservices": "nameservice1",
            "dfs.ha.namenodes.nameservice1": "namenode41,namenode65",
            "dfs.namenode.rpc-address.nameservice1.namenode41": "jh-bigdata-001:8020",
            "dfs.namenode.rpc-address.nameservice1.namenode65": "jh-bigdata-002:8020",
            "dfs.client.failover.proxy.provider.nameservice1": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        }
    }
}

reader_db_info = db_infos.get(reader_db_type).get(env)
writer_db_info = db_infos.get(writer_db_type).get(env)
hdfs_info = hdfs_infos.get(env)
# reader字段信息缓存，writer处理中复用到
column_type_list = []


def create_setting_json(channel=1):
    setting = {"speed": {"channel": channel}, "errorLimit": {"record": 0}}
    return setting


def create_oracle_reader_json():
    global column_type_list
    conn = cx_Oracle.connect(reader_db_info.get("user"), reader_db_info.get("password"),
                             reader_db_info.get("listener"))  # type: Connection
    cursor = conn.cursor()  # type: cx_Oracle.Cursor
    db_name = get_reader_db_table_name()[0].upper()
    table_name = get_reader_db_table_name()[1].upper()
    sql = "select col.COLUMN_NAME, col.DATA_TYPE, com.COMMENTS " \
          "from all_tab_columns col " \
          "left join ALL_COL_COMMENTS com on col.OWNER = com.OWNER and col.TABLE_NAME = com.TABLE_NAME and col.COLUMN_NAME = com.COLUMN_NAME " \
          "where col.OWNER='%s' and col.TABLE_NAME='%s' order by col.COLUMN_ID" % (db_name, table_name)

    print("oracle schema sql = %s" % sql)
    cursor.execute(sql)
    count = 0
    for x in cursor.fetchall():
        s = (count, x[0], x[1], x[2])
        count = count + 1
        column_type_list.append(s)
    if len(column_type_list) == 0:
        raise Exception("can not found the column info in %s, please check if the config of is right",
                        reader_db_table_name)
    column_list = list(map(lambda x: x[1], column_type_list))
    connection = [
        {"table": [reader_db_table_name], "jdbcUrl": ["jdbc:oracle:thin:@//" + reader_db_info.get("listener")]}]
    oracle_reader = {"name": "oraclereader", "parameter": {"username": reader_db_info.get("user"),
                                                           "password": reader_db_info.get("password"),
                                                           "column": column_list, "splitPk": column_list[0],
                                                           "connection": connection}}
    return oracle_reader


def reader_type_mapping(data_type: str):
    global reader_db_type
    data_type = data_type.upper()
    index = data_type.find("(")
    rtv_data_type = None
    data_len = None
    if index > 0:
        data_len = data_type[index+1:len(data_type)-1]
        data_type = data_type[0:index]
    if reader_db_type.lower() == "hive":
        if data_type in ("TINYINT", "SMALLINT", "INT", "BIGINT"):
            rtv_data_type = "Long"
        elif data_type in ("FLOAT", "DOUBLE"):
            rtv_data_type = "Double"
        elif data_type in ("STRING", "CHAR", "VARCHAR", "STRUCT", "MAP", "ARRAY", "UNION", "BINARY"):
            rtv_data_type = "String"
        elif data_type == "BOOLEAN":
            rtv_data_type = "Boolean"
        elif data_type in ("DATE", "TIMESTAMP"):
            rtv_data_type = "Date"
    elif reader_db_type.lower() == "oracle":
        if data_type in ("NUMBER", "INTEGER", "INT", "SMALLINT"):
            rtv_data_type = "Long"
        elif data_type in ("LONG", "CHAR""NCHAR""VARCHAR", "VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB", "CHARACTER",
                           "CHARACTER VARYING", "CHAR VARYING", "NATIONAL CHARACTER", "NATIONAL CHAR",
                           "NATIONAL CHARACTER",
                           "VARYING", "NATIONAL CHAR VARYING", "NCHAR VARYING"):
            rtv_data_type = "String"
        elif data_type in ("TIMESTAMP", "DATE"):
            rtv_data_type = "Date"
        elif data_type in ("BIT", "BOOL"):
            rtv_data_type = "Boolean"
        elif data_type in ("BLOB", "BFILE", "RAW", "LONG RAW"):
            rtv_data_type = "Bytes"
    elif reader_db_type.lower() == "ftp":
        if data_type in ("INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT"):
            rtv_data_type = "Long"
        elif data_type in ("FLOAT", "DOUBLE",  "DECIMAL"):
            rtv_data_type = "Double"
        elif data_type in ("VARCHAR", "CHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "YEAR"):
            rtv_data_type = "String"
        elif data_type in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
            rtv_data_type = "Date"
        elif data_type in ("BIT", "BOOL"):
            rtv_data_type = "Boolean"
    # if rtv_data_type is None:
    #     print(rtv_data_type)
    return rtv_data_type, data_len


def writer_type_mapping(reader_data_type: str):
    global writer_db_type
    datax_data = reader_type_mapping(reader_data_type)
    datax_type = datax_data[0]
    datax_len = datax_data[1]
    if writer_db_type.lower() == "hive":
        if datax_type == "Long":
            return "BIGINT"
        if datax_type == "Double":
            return "DOUBLE"
        if datax_type == "String":
            return "STRING"
        if datax_type == "Boolean":
            return "BOOLEAN"
        if datax_type == "Date":
            return "DATE"
    elif writer_db_type.lower() == "dorisdb":
        if datax_type == "Long":
            return "BIGINT"
        if datax_type == "Double":
            return "DOUBLE"
        if datax_type == "String":
            if datax_len is None:
                return "VARCHAR(50)"
            else:
                return "VARCHAR(%s)" % datax_len
        if datax_type == "Boolean":
            return "BOOLEAN"
        if datax_type == "Date":
            return "DATE"


def get_reader_db_table_name():
    index = reader_db_table_name.find(".")
    if index > 0:
        return reader_db_table_name[0:index].lower(), reader_db_table_name[index + 1:].lower()
    else:
        raise Exception('reader_db_table_name is not right, it must bu set as db_name.table_name format')


def get_writer_db_table_name():
    global writer_table_name, writer_db_name, writer_hive_table_prefix
    db_name = get_reader_db_table_name()[0]
    table_name = get_reader_db_table_name()[1]
    if writer_db_name is not None:
        db_name = writer_db_name

    if writer_table_name is not None:
        table_name = writer_table_name
    elif writer_db_type == "hive":
        table_name = writer_hive_table_prefix + table_name
    elif writer_db_type == "dorisdb":
        table_name = table_name
    return db_name.lower(), table_name.lower(), db_name.lower() + "." + table_name.lower()


def create_hive_reader_json():
    conn = hive.Connection(host=reader_db_info.get("host"), port=reader_db_info.get("port"),
                           username=reader_db_info.get("user"),
                           auth="CUSTOM", password=reader_db_info.get("password"))  # type: hive.Connection
    global partition, haveKerberos, column_type_list
    cursor = conn.cursor()  # type: hive.Cursor
    cursor.execute("desc " + reader_db_table_name)
    count = 0
    # column json
    for x in cursor.fetchall():
        if x[0] == partition or x[1] is None or x[0].startswith("#"):
            continue
        data_type = x[1]
        s = (count, x[0], data_type, x[2])
        count = count + 1
        column_type_list.append(s)
    column_list = list(map(lambda y: {"index": y[0], "type": reader_type_mapping(y[2])[0]}, column_type_list))
    db_name = get_reader_db_table_name()[0]
    table_name = get_reader_db_table_name()[1]
    path = os.path.join("/user/hive/warehouse", db_name + ".db", table_name, partition + "=${biz_date}")
    hive_reader = {"name": "hdfsreader", "parameter": {"path": path, "column": column_list, "fileType": "orc",
                                                       "encoding": "UTF-8", "fieldDelimiter": "\t",
                                                       "haveKerberos": haveKerberos,
                                                       "defaultFS": hdfs_info.get("defaultFS"),
                                                       "hadoopConfig": hdfs_info.get("hadoopConfig")}}
    return hive_reader


def process_col_name(col_name):
    col_name = col_name.strip()
    if col_name.startswith("`"):
        col_name = col_name[1:]
    if col_name.endswith("`"):
        col_name = col_name[:len(col_name)-1]
    return col_name


def parse_table_schema(table_schema_file_path):
    global column_type_list
    with open(table_schema_file_path, 'r') as file_to_read:
        context = file_to_read.readlines()
        count = 0
        for line in context:
            if re.search("create\s+table\s+", line.lower()) or re.search("primary\s+key\s+", line.lower()) \
                    or re.search("unique\s+key\s+", line.lower()) or re.search("key\s+", line.lower()) \
                    or re.search("engine\s*=innodb\s+", line.lower()):
                continue
            if line.upper().find(' COMMENT ') > -1:
                match_obj = re.match(r'(.*?)\s+(.*?)\s+.*(COMMENT\s+\'(.*?)\')', line.upper().strip())
                s = (count, process_col_name(match_obj.group(1).lower()), match_obj.group(2).lower(), match_obj.group(4))
            else:
                match_obj = re.match(r'(.*?)\s+(.*?)\s+.*', line.upper().strip())
                s = (count, process_col_name(match_obj.group(1).lower()), match_obj.group(2).lower(), None)
            count = count + 1
            column_type_list.append(s)


def create_ftp_reader_json(table_schema_file_path, file_name):
    global column_type_list
    column_type_list = []
    parse_table_schema(table_schema_file_path)
    column_list = []
    for x in column_type_list:
        if reader_type_mapping(x[2])[0] == "Date":
            s = {"index": x[0], "type": reader_type_mapping(x[2])[0], "format": "请输入时间格式"}
        elif reader_type_mapping(x[2])[0] == "Long" or reader_type_mapping(x[2])[0] == "Double":
            # 数据库里数值型的数据有肯能为null, 转datax内部类型的时候，null转long会报错，所以要转成String类型，但是为了建表保证数据类型一致，所以保留原类型
            s = {"index": x[0], "type": "String"}
        else:
            s = {"index": x[0], "type": reader_type_mapping(x[2])[0]}
        column_list.append(s)
    ftp_host = reader_db_info.get("host")
    ftp_port = reader_db_info.get("port")
    ftp_username = reader_db_info.get("username")
    ftp_password = reader_db_info.get("password")
    db_name = file_name[:file_name.find(".")]
    table_name = file_name[file_name.find(".")+1:]
    ftp_reader = {"name": "ftpreader", "parameter": {"protocol": "sftp", "host": ftp_host, "port": ftp_port,
                                                     "username": ftp_username, "password": ftp_password, "path": ["/upload/dingxiangClinic/yesterday/%s/%s" % (db_name, table_name)],
                                                     "column": column_list, "encoding": "UTF-8", "fieldDelimiter": ","}}
    return ftp_reader


def create_reader_json(reader_type: str, table_schema_file_path, file_name):
    if reader_type.lower() == "oracle":
        return create_oracle_reader_json()
    elif reader_type.lower() == "hive":
        return create_hive_reader_json()
    elif reader_type.lower() == "ftp":
        return create_ftp_reader_json(table_schema_file_path, file_name)


def create_hive_writer_json():
    global column_type_list, haveKerberos
    column_list = list(map(lambda x: {"name": x[1], "type": writer_type_mapping(x[2])}, column_type_list))
    db_name = get_writer_db_table_name()[0].lower()
    table_name = get_writer_db_table_name()[1].lower()
    path = os.path.join("/user/hive/warehouse", db_name + ".db", table_name, partition + "=${biz_date}")
    hive_writer = {"name": "hdfswriter", "parameter": {"compress": "NONE", "fileType": "orc", "writeMode": "truncate",
                                                       "fieldDelimiter": "\t", "path": path, "fileName": table_name,
                                                       "column": column_list, "haveKerberos": haveKerberos,
                                                       "defaultFS": hdfs_info.get("defaultFS"),
                                                       "hadoopConfig": hdfs_info.get("hadoopConfig")}}
    return hive_writer


def create_dorisdb_writer_json():
    global column_type_list
    column_list = list(map(lambda x: x[1], column_type_list))
    jdbc_url = "jdbc:mysql://%s:%s" % (reader_db_info.get("host"), reader_db_info.get("port"))
    db_name = get_writer_db_table_name()[0]
    table_name = get_writer_db_table_name()[1]
    dorisdb_writer = {"name": "doriswriter",
                      "parameter": {"username": writer_db_info.get("user"), "password": writer_db_info.get("password"),
                                    "database": db_name, "table": table_name, "column": column_list,
                                    "jdbcUrl": jdbc_url, "loadUrl": writer_db_info.get("loadUrl"),
                                    "loadProps": writer_db_info.get("loadProps")}}
    return dorisdb_writer


def create_writer_json(writer_type):
    if writer_type == "hive":
        return create_hive_writer_json()
    elif writer_type == "dorisdb":
        return create_dorisdb_writer_json()


def create_datax_json(table_schema_file_path=None, file_name=None):
    global reader_db_type, writer_db_type
    setting_json = create_setting_json()
    reader_json = create_reader_json(reader_type=reader_db_type, table_schema_file_path=table_schema_file_path, file_name=file_name)
    writer_json = create_writer_json(writer_db_type)
    datax_json = {"job": {"content": [{"reader": reader_json, "writer": writer_json}], "setting": setting_json}}
    datax_json = js.dumps(datax_json, indent=4, ensure_ascii=False)
    with open(os.path.join(os.getcwd(), reader_db_type + "2" + writer_db_type + "_" + reader_db_table_name + ".json"), "w+", encoding="utf-8") as f:
        f.write(datax_json)


def strip(s: str):
    if s is not None:
        return "COMMENT '%s'" % s.strip()
    return ""


def create_writer_hive_table_sql():
    global column_type_list
    db_table_name = get_writer_db_table_name()[2]
    with open(os.path.join(os.getcwd(), "create_table_%s_sql" % db_table_name), "w+", encoding="utf-8") as f:
        f.write("create table IF NOT EXISTS %s (\r" % db_table_name)
        length = len(column_type_list)
        count = 1
        for x in column_type_list:
            if count == length:
                f.write("%s %s %s\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            else:
                f.write("%s %s %s,\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            count = count + 1
        f.write(") partitioned by (pt string)\r")
        f.write("row format delimited fields terminated by '\\t'\r")
        f.write("STORED AS ORC\r")


def dorisdb_dynamic_partition(f):
    f.write("PARTITION BY RANGE(请在括号内填写分区字段)(\r")
    curr_time = datetime.datetime.now()
    if dorisdb_dynamic_partition_time_unit.lower() == "day":
        for x in range(-3, 3):
            delta = datetime.timedelta(days=x)
            n_day = curr_time + delta
            n_day_plus_1 = n_day.strftime('%Y-%m-%d')
            delta = datetime.timedelta(days=-1)
            n_day = n_day + delta
            n_day_ = n_day.strftime('%Y%m%d')
            if x == 2:
                f.write("PARTITION p%s VALUES LESS THAN (\"%s\")\r" % (n_day_, n_day_plus_1))
            else:
                f.write("PARTITION p%s VALUES LESS THAN (\"%s\"),\r" % (n_day_, n_day_plus_1))
    elif dorisdb_dynamic_partition_time_unit.lower() == "month":
        for x in range(-3, 3):
            n_day = curr_time + dateutil.relativedelta.relativedelta(months=x)
            the_month_start_day = datetime.datetime(n_day.year, n_day.month, 1)
            the_month_start_day_str = the_month_start_day.strftime('%Y-%m-%d')
            month_before = the_month_start_day + dateutil.relativedelta.relativedelta(months=-1)
            month_before_str = month_before.strftime('%Y%m')
            if x == 2:
                f.write("PARTITION p%s VALUES LESS THAN (\"%s\")" % (month_before_str, the_month_start_day_str))
            else:
                f.write("PARTITION p%s VALUES LESS THAN (\"%s\"),\r" % (month_before_str, the_month_start_day_str))
    f.write(") DISTRIBUTED BY HASH(请在括号内填写分桶字段) BUCKETS 5\r")


def create_writer_doris_table_sql():
    global column_type_list
    db_table_name = get_writer_db_table_name()[2]
    with open(os.path.join(os.getcwd(), "create_table_%s_sql" % db_table_name), "w+", encoding="utf-8") as f:
        f.write("CREATE TABLE IF NOT EXISTS %s (\r" % db_table_name)
        length = len(column_type_list)
        count = 1
        for x in column_type_list:
            if count == length:
                f.write("%s %s %s\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            else:
                f.write("%s %s %s,\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            count = count + 1
        f.write(") DUPLICATE KEY(请在括号内填写key)\r")
        dorisdb_dynamic_partition(f)
        f.write("PROPERTIES(\r")
        f.write("\"replication_num\" = \"1\",\r")
        f.write("\"dynamic_partition.enable\" = \"true\",\r")
        f.write("\"dynamic_partition.time_unit\" = \"DAY\",\r")
        f.write("\"dynamic_partition.start\" = \"-10\",\r")
        f.write("\"dynamic_partition.end\" = \"10\",\r")
        f.write("\"dynamic_partition.prefix\" = \"p\",\r")
        f.write("\"dynamic_partition.buckets\" = \"5\"\r")
        f.write(")")


def create_writer_table_sql():
    if writer_db_type == "hive":
        create_writer_hive_table_sql()
    elif writer_db_type == "dorisdb":
        create_writer_doris_table_sql()


def do_batch():
    global mysql_file_path, reader_db_table_name
    for root, dirs, files in os.walk(mysql_file_path):
        for file in files:
            path = os.path.join(root, file)
            reader_db_table_name = file
            create_datax_json(table_schema_file_path=path, file_name=file)
            create_writer_table_sql()


if __name__ == '__main__':
    if reader_db_type == "ftp":
        do_batch()
    else:
        create_datax_json()
        create_writer_table_sql()
