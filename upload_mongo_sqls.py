from  pymongo import MongoClient
import re
import datetime
import codecs


def insert_all_sqls(file_name):
    with codecs.open(file_name, "r", encoding='utf-8') as the_file:
        content = the_file.read()

    interface_names = re.findall(r"\[\"(.+?)\"\]", content)

    interface_sqls = re.findall('```sql\s+(.+?)\s+```', content, re.S)
    print(interface_names)
    titles_cnt = len(interface_names)
    sqls_cnt = len(interface_sqls)
    if (titles_cnt!=sqls_cnt):
        print("注意:接口个数不一致!!!")
        print(titles_cnt, sqls_cnt)
        exit()
    else:
        print("接口个数一致!!!!!接口个数:%d" %titles_cnt)
        agent = collection.find_one({'org_code': org_code})
        if (agent is None):
            print("请先调用create_src创建一个商户壳%s再上传sqls!!!" %org_code)
            collection.save(database_agent)
            agent = collection.find_one({'org_code': org_code})
            sqls = agent['sqls']
            for i, el in enumerate(interface_names):
                print(el)
                sqls[el] = interface_sqls[i]
            collection.save(agent)
            print("选择org_code = "+org_code+"商户")
        else:
            sqls = agent['sqls']
            for i, el in enumerate(interface_names):
                print(el)
                sqls[el] = interface_sqls[i]
            collection.save(agent)


def insert_one_sql(org_code, file_name, interface_names):

    with codecs.open(file_name, "r", encoding='utf-8') as the_file:
        content = the_file.read()

    interface_names = re.findall(r"\[\"(.+?)\"\]", content)
    interface_sqls = re.findall('```sql\s+(.+?)\s+```', content, re.S)

    titles_cnt = len(interface_names)
    sqls_cnt = len(interface_sqls)

    if (titles_cnt != sqls_cnt):
        print("注意:接口个数不一致!!!")
        print(titles_cnt, sqls_cnt)
        exit()
    else:
        print("接口个数一致!!!!!接口个数:%d" % titles_cnt)
        agent = collection.find_one({'org_code': org_code})
        if (agent is None):
            print("请先调用create_src创建一个商户壳%s再上传sqls!!!" %org_code)
            collection.save(database_agent)
            print("创建成功")
            agent = collection.find_one({'org_code': org_code})
        sqls = agent['sqls']
        print(interface_names)
        index = interface_names.index(interface_names)
        print(index)
        print(interface_sqls[index])
        sqls[interface_names] = interface_sqls[index]
        collection.save(agent)


# 测试mongo
# mc = MongoClient('192.168.10.202', 27017)
# 线上mongo
mc = MongoClient('222.73.36.205', 40000)
db = mc.crm_production
collection = db.database_agents

org_code = "cddiaodiao"
username = "qqsmy"
password = "qqsmy"
host = "cvpn.joowing.com"
port = 4072
database = "kldyytrs20180104"

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

file_name = "C://Users//Administrator//Desktop//数据导入//调调_cddiaodiao_sqls.md"
# 单条插入sqls
# interface4updated = 'find_prices'
# insert_one_sql(org_code, file_name, interface4updated)

# 插入all sqls
insert_all_sqls(file_name)
print("上传成功！")