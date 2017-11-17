#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/17 15:05
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : conn_sql_server.py
# @Software: PyCharm

import pymssql
import settings


def save_to_sql_server():
    """
    存数据至sql——server
    :return:
    """
    conn = pymssql.connect(host=settings.host, user=settings.user, password=settings.password,
                           database=settings.database, charset=settings.charset)
    cur = conn.cursor()

    if not cur:
        raise (NameError, "数据库连接失败")

    for i in range(100):
        cur.execute("insert into Company(Industry,Name,Introduction) values('chanye%d','公司名称%d','gongsi介绍%d')" % (i, i, i))
        conn.commit()

    conn.close()


if __name__ == '__main__':
    save_to_sql_server()
