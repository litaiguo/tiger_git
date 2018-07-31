import hashlib
import json
import os
from datetime import datetime
from os import DirEntry

from os.path import join, basename, abspath
from typing import List, Dict

import pymysql
import re


def read_precedures(host, port, user, password, db_name, dir_name):
    connection = pymysql.connect(host=host, port=port,
                                 user=user, password=password,
                                 database=db_name, charset="utf8")
    cursor = connection.cursor()
    precedure_query = """
SHOW PROCEDURE STATUS where Db = %s;
    """
    cursor.execute('SET NAMES utf8;')
    cursor.fetchall()
    cursor.execute(precedure_query, [database_name])
    precedures = list(cursor.fetchall())
    print(precedures)
    regex = re.compile(r"DEFINER(.*)PROCEDURE")
    for precedure_status in precedures:
        precedure_name = precedure_status[1]
        print("{0}:{1}".format(precedure_status[0], precedure_status[1]))
        cursor.execute("SHOW CREATE PROCEDURE {0};".format(precedure_name))
        precedures = list(cursor.fetchall())
        code = precedures[0][2]  # type: bytes or str
        # code = code.encode("utf-8")
        if type(code) is bytes:
            code = code.decode("utf-8")
        # print(code)
        code = regex.sub("PROCEDURE", code)
        # print(code)
        with open(join(dir_name, "{0}.sql".format(precedure_name)), "w", encoding="utf-8") as sql_file:
            sql_file.write("DROP PROCEDURE IF EXISTS `{0}`;\r\n".format(precedure_name))
            sql_file.write(code)
    cursor.close()

    # create_al(connection)


def create_al(connection):
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS `alembic_version`;")
    cursor.fetchall()
    sql2 = """
CREATE TABLE `alembic_version` (
  `version_num` varchar(32) NOT NULL,
  PRIMARY KEY (`version_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    cursor.execute(sql2)
    cursor.fetchall()

    sql3 = """
 INSERT INTO `alembic_version` (`version_num`)
 VALUES
	('084d11487a87');
    """
    cursor.execute(sql3)
    cursor.fetchall()
    connection.commit()
    cursor.close()

org_codes_all = [
    "aier",
    "aitiantian",
    "aiwa",
    "aiying",
    "aiyingfang",
    "babycare",
    "babycountry",
    "babyfocus",
    "bearhouse",
    "beibei",
    "byjy",
    "chenbaby",
    "clbabytiandi",
    "congcong",
    "czqcyy",
    "dd",
    "dreamstart",
    "dtaiyingfang",
    "dychouchou",
    "eastbaby",
    "fabeiniu",
    "fqyaya",
    "fzabd",
    "fzjiabeiai",
    "glhuangjiayy",
    "growgarden",
    "haoshijie",
    "happyxybb",
    "harneybaby",
    "hdguzi",
    "heartlove",
    "jdb",
    "jiajia",
    "jialibaby",
    "jinbaby",
    "jinyaolan",
    "jodafengche",
    "jzaibaby",
    "keai",
    "kkqq",
    "lebaby",
    "lebao",
    "leyaya",
    "loveangel",
    "loveheart",
    "lsdsyy",
    "lshibaby",
    "lyjy",
    "lzgoodboy",
    "lzyyb",
    "mamabb",
    "mamalove",
    "mamiai",
    "mamibaobei",
    "muyingfang",
    "nbmamibaby",
    "ndabd",
    "ndbeiniu",
    "ptbabyplan",
    "pxayf",
    "pyfujiababy",
    "qwmykj",
    "rsxyzj",
    "rxwayy",
    "shengyi",
    "smdafengche",
    "smhybb",
    "sqbaobaole",
    "sunnybaby",
    "sxqzf",
    "tianyibaby",
    "wanpf",
    "wlmami",
    "wxmyf",
    "xatonghuiyy",
    "xinrenlei",
    "xzbym",
    "xzguzi",
    "xzmmbb",
    "yaerbaby",
    "ybfcbb",
    "ygyj",
    "yingyuan",
    "yingyuansu",
    "youyimy",
    "yqmamalove",
    "yujiababy",
    "yybb",
    "yyplan",
    "zgprettybaby",
    "zyyibaby"
]

org_codes18 = [
    "hefeijyl"
]
root_dir = abspath("./online")

#for org_code in org_codes_all:
for org_code in org_codes18:
    database_name = "{0}DW".format(org_code)
    sqls_dir = join(root_dir, org_code, "sqls")
    os.makedirs(sqls_dir, exist_ok=True)
    read_precedures(host="222.73.36.230", port=3336,
                    user="datadev", password="datadev!33.6",
                    db_name=database_name, dir_name=sqls_dir)


