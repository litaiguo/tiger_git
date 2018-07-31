import datetime
from pymongo import MongoClient


def create_src(host, org_code, username, password, port, database):
    database_agent = {
        'org_code': org_code,
        'updated_at': datetime.datetime.now(),
        'created_at': datetime.datetime.now(),
        'status': 'active',
        'sqls': {},
        'properties': {},
        'database_config': {
            'default': {
                'username': username,
                'password': password,
                'host': host,
                'port': port,
                'database': database,
                'timeout': 60,
                'login_timeout': 10,
                'encoding': "utf-8",
                'database_type': "sql_server",
            }
        }
    }
    collection.save(database_agent)


# 测试mongo
# mc = MongoClient('192.168.10.202', 27017)
# 正式mongo
mc = MongoClient('222.73.36.205', 40000)
db = mc.crm_production
collection = db.database_agents
host = "171.92.207.74"
org_code = "pzyingyuansu"
username = "pzyys"
password = "pzyys"
port = 4076
database = "kldyytrs20171207"
agent = collection.find_one({'org_code': org_code})
if (agent is None):
    create_src(host, org_code, username, password, port, database)
    print("商户{0}创建成功！".format(org_code))
else:
    print("该商户已配置,请手动去查看或者修改")
