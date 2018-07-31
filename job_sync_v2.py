import hashlib
import json
import os
from datetime import datetime
from os import DirEntry

from os.path import join, basename
from typing import List, Dict
import pymysql


class Task(object):
    def __init__(self, path, org_code, mode):
        self.path = path
        self.org_code = org_code
        self.mode = mode
        self.task_name = basename(self.path)
        self.fetch_size = 0
        self.task_priority = 0
        self.target_columns = []
        self.target_tables = []
        self.pre_sqls = []
        self.session_sqls = []
        self.query_sqls = []
        self.increment_config = []

        self._load_meta()
        self._load_pre_sqls()
        self._load_query_sqls()
        self._load_session_sqls()

    def _load_meta(self):
        with open(join(self.path, "meta.json"), 'r', encoding="utf-8") as meta_config:
            meta_config = json.load(meta_config)  # type: dict
            self.target_columns = meta_config.get("target_columns", [])
            self.target_tables = meta_config.get("target_tables", [])
            self.fetch_size = meta_config.get("fetch_size", 1024)
            self.task_priority = meta_config.get("task_priority", 0)
            self.increment_config = meta_config.get("increment_config", {})

    def _load_pre_sqls(self):
        self.pre_sqls.extend(self._load_sqls("pre_sql"))

    def _load_query_sqls(self):
        self.query_sqls.extend(self._load_sqls("query_sql"))

    def _load_session_sqls(self):
        self.session_sqls.extend(self._load_sqls("session_sql"))

    def _load_sqls(self, prefix_name):
        all_sqls = []
        for sql_file in os.scandir(self.path):  # type: DirEntry
            sql_with_prefix = sql_file.name.startswith(prefix_name + ".")
            file_end_with_sql = sql_file.name.endswith(".sql")

            if sql_with_prefix and file_end_with_sql:
                content = open(join(self.path, sql_file.name), "r", encoding="utf-8").read()
                index = sql_file.name.split(".")[1]
                all_sqls.append([content, index])

        all_sqls = sorted(all_sqls, key=lambda sql: sql[1])
        all_sql_contents = []
        for c in all_sqls:
            all_sql_contents.append(c[0])

        return all_sql_contents


class SQLScanner(object):
    def __init__(self, org_code, path):
        self.path = path
        self.org_code = org_code

    def scan_sqls(self, connection):

        query_sql = "SELECT * FROM retailer_sqls WHERE org_code = %s AND file_name = %s"
        update_sql = "UPDATE retailer_sqls SET sql_content = %s, sql_sha1 = %s where org_code = %s AND file_name = %s"
        insert_sql = "INSERT INTO retailer_sqls(org_code, file_name, sql_content, sql_sha1) values(%s, %s, %s, %s);"
        for sql_file in os.scandir(self.path):  # type: DirEntry
            ends_with_sql = sql_file.name.endswith(".sql")  # type: bool
            if all([sql_file.is_file(), ends_with_sql]):
                sql_file_name = sql_file.name
                sql_content = open(join(self.path, sql_file.name), 'r', encoding="utf-8").read()
                sql_sha1 = hashlib.sha1(sql_content.encode('utf-8')).hexdigest()
                connection.begin()
                cursor = connection.cursor()
                cursor.execute(query_sql, [self.org_code, sql_file_name])
                exists = list(cursor.fetchall())
                # print(self.org_code)
                if len(exists) > 0:
                    cursor.execute(update_sql, [sql_content, sql_sha1, self.org_code, sql_file_name])
                else:
                    cursor.execute(insert_sql, [ self.org_code, sql_file_name, sql_content, sql_sha1])
                connection.commit()


class TaskScanner(object):
    def __init__(self, org_code, mode, path):
        self.org_code = org_code
        self.mode = mode
        self.path = path

    def scan_tasks(self, connection):
        #  type: (Connection) -> List[Task]
        tasks = []
        for task_dir in os.scandir(self.path):  # type: DirEntry
            dir_start_with_dot = task_dir.name.startswith(".")  # type: bool
            if all([task_dir.is_dir(), not dir_start_with_dot]):
                task = Task(path=join(self.path, task_dir.name), org_code=self.org_code, mode=self.mode)
                tasks.append(task)

        self._update_task_with_connection(tasks, connection)
        return tasks

    def _update_task_with_connection(self, tasks, connection):
        # type: (List[Task], Connection) -> None
        grouped_tasks = dict()  # type: Dict[str, Task]
        for task in tasks:
            grouped_tasks[task.task_name] = task

        if len(tasks) == 0:
            just_clear_sql = """
DELETE FROM retailer_sync_jobs 
WHERE retailer_sync_jobs.org_code = %s
  AND retailer_sync_jobs.mode = %s
                    """
            connection.begin()
            cursor = connection.cursor()
            cursor.execute(just_clear_sql, [self.org_code, self.mode])
            connection.commit()
            return

        all_task_names = list(map(lambda task: task.task_name, tasks))
        connection.begin()
        cursor = connection.cursor()  # type: Cursor

        cursor.execute("SELECT * FROM retailer_configs where org_code = %s", [self.org_code])
        data = cursor.fetchall()
        if len(data) == 0:
            insert_org_sql = """
INSERT INTO `retailer_configs` (`org_code`, `database_type`, `database_config`, `created_at`, `updated_at`, `activated`, `online_dw_name`)
VALUES (%s, 'sql_server', %s, %s, %s, 0, %s);    
            """
            cursor.execute(insert_org_sql, [self.org_code, json.dumps(dict()),
                                            datetime.now(), datetime.now(), "{0}DW".format(self.org_code)])

        clear_sql = """
DELETE FROM retailer_sync_jobs 
WHERE retailer_sync_jobs.org_code = %s
  AND retailer_sync_jobs.name NOT IN ({0})
  AND retailer_sync_jobs.mode = %s
        """.format(', '.join(list(map(lambda x: '%s', all_task_names))))
        query_args = [self.org_code]
        query_args.extend(all_task_names)
        query_args.extend([self.mode])
        cursor.execute(clear_sql, query_args)
        data = cursor.fetchall()

        query_existed_sql = """
SELECT * FROM retailer_sync_jobs 
WHERE retailer_sync_jobs.org_code = %s
  AND retailer_sync_jobs.name IN ({0})
  AND retailer_sync_jobs.mode = %s
        """.format(', '.join(list(map(lambda x: '%s', all_task_names))))

        update_sql = """
UPDATE retailer_sync_jobs SET
  retailer_sync_jobs.updated_at = %s, 
  retailer_sync_jobs.query_sql = %s,
  retailer_sync_jobs.pre_sql = %s,
  retailer_sync_jobs.target_tables = %s,
  retailer_sync_jobs.target_columns = %s,
  retailer_sync_jobs.fetch_size = %s,
  retailer_sync_jobs.session_sqls = %s,
  retailer_sync_jobs.task_priority = %s,
  retailer_sync_jobs.increment_config = %s
WHERE retailer_sync_jobs.id = %s
        """
        cursor.execute(query_existed_sql, query_args)
        data = cursor.fetchall()

        if len(data) > 0:
            print(data)

        for database_task in data:
            task_id = database_task[0]
            task_name = database_task[5]

            task = grouped_tasks.get(task_name)
            update_args = [
                datetime.now(),
                json.dumps(task.query_sqls, indent=4),
                json.dumps(task.pre_sqls, indent=4),
                json.dumps(task.target_tables, indent=4),
                json.dumps(task.target_columns, indent=4),
                task.fetch_size,
                json.dumps(task.session_sqls, indent=4),
                json.dumps(task.task_priority, indent=4),
                json.dumps(task.increment_config, indent=4),
                task_id
            ]
            cursor.execute(update_sql, update_args)
            grouped_tasks.pop(task_name, None)

        insert_sql = """
INSERT INTO `retailer_sync_jobs` (
  `org_code`, `created_at`, `updated_at`, `name`, 
  `query_sql`, `pre_sql`, `target_tables`, `target_columns`, 
  `manual_create_json`, `fetch_size`, `session_sqls`, `mode`, `task_priority`, `increment_config`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        for task_name in grouped_tasks.keys():

            task = grouped_tasks[task_name]
            insert_args = [
                task.org_code,
                datetime.now(),
                datetime.now(),
                task.task_name,
                json.dumps(task.query_sqls, indent=4),
                json.dumps(task.pre_sqls, indent=4),
                json.dumps(task.target_tables, indent=4),
                json.dumps(task.target_columns, indent=4),
                False,
                task.fetch_size,
                json.dumps(task.session_sqls, indent=4),
                task.mode,
                json.dumps(task.task_priority, indent=4),
                json.dumps(task.increment_config, indent=4)
            ]
            cursor.execute(insert_sql, insert_args)

        connection.commit()


class EnvSyncDirectory(object):
    def __init__(self, path):
        # type: (str) -> EnvSyncDirectory

        self.path = path
        self.config = None  # type: dict
        self.connection = None  # type: Connection
        self.tasks = []  # type: List[Task]

    def sync(self):
        self._init_config()
        self._init_connection()
        print(u"初始化环境{0}的配置: {1}".format(basename(self.path), self.config))
        self._scan_orgs()
        pass

    def _init_config(self):
        with open(join(self.path, 'config.json'), 'r', encoding="utf-8") as config_content:
            self.config = json.load(config_content)

    def _init_connection(self):
        self.connection = pymysql.connect(host=self.config['host'], port=self.config["port"],
                                          user=self.config["username"], password=self.config["password"],
                                          database=self.config["database_name"], charset="utf8")

    def _scan_orgs(self):
        self.tasks.clear()
        for org_dir in os.scandir(self.path):  # type: DirEntry
            dir_start_with_dot = entry.name.startswith(".")  # type: bool
            if all([org_dir.is_dir(), not dir_start_with_dot]):
                org_code = basename(org_dir.name)
                if os.path.isdir(join(self.path, org_dir.name, "dr")):
                    dr_tasks = TaskScanner(org_code=org_code, mode="dr",
                                           path=join(self.path, org_dir.name, "dr")).scan_tasks(self.connection)
                    self.tasks.extend(dr_tasks)

                if os.path.isdir(join(self.path, org_dir.name, "ss")):
                    ss_tasks = TaskScanner(org_code=org_code, mode="ss",
                                           path=join(self.path, org_dir.name, "ss")).scan_tasks(self.connection)
                    self.tasks.extend(ss_tasks)

                if os.path.isdir(join(self.path, org_dir.name, "stock")):
                    ss_tasks = TaskScanner(org_code=org_code, mode="stock",
                                           path=join(self.path, org_dir.name, "stock")).scan_tasks(self.connection)
                    self.tasks.extend(ss_tasks)

                if os.path.isdir(join(self.path, org_dir.name, "sqls")):
                    sql_scanner = SQLScanner(org_code=org_code, path=join(self.path, org_dir.name, "sqls"))
                    sql_scanner.scan_sqls(self.connection)


def process_dir_name(path):
    # type: (str) -> None
    if os.path.isfile(join(path, "config.json")):
        EnvSyncDirectory(path).sync()
    else:
        print(u"环境{0}找不到数据的配置文件[config.json]".format(basename(path)))


def get_dir_status() -> bool:
    from subprocess import Popen, PIPE

    online_status = False

    resp = Popen("git status --porcelain | find \"online\"", shell=True, stdout=PIPE, stderr=PIPE).stdout.readlines()
    if len(resp) != 0:
        print("本地目录有改动,请先处理再发布到airflow")
        print(resp)
        exit()
    else:
        online_status = True
        print("本地目录正常")
        return online_status


dir_path = os.path.dirname(os.path.realpath(__file__))
print(dir_path)
online_status = get_dir_status()
if online_status is True:
    for entry in os.scandir("."):  # type: DirEntry
        start_with_dot = entry.name.startswith(".")  # type: bool
        if all([entry.is_dir(), not start_with_dot]):
            print(entry.name)
            process_dir_name(join(dir_path, entry.name))