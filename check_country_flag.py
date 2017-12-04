#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/12/4 15:20
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : check_country_flag.py
# @Software: PyCharm

# 检查国家&国旗

import pymssql
import settings
import os


def get_server_country_set():
    """
    获取服务端国家集合
    :return:
    """
    conn = pymssql.connect(host=settings.host, user=settings.user, password=settings.password,
                           database=settings.database, charset=settings.charset)
    cur = conn.cursor()

    if not cur:
        raise (NameError, "数据库连接失败")

    cur.execute("select Name from Country")
    country_name_list = cur.fetchall()
    country_name_set = set([x[0].lower() for x in country_name_list])

    conn.close()
    return country_name_set


def get_country_flag_set():
    """
    获取本地国旗集合
    :return:
    """
    flag_set = set()
    for item_flag in os.listdir(r'C:\Users\topeasecpb\Desktop\country(1)\country'):
        # print(item_flag)
        if item_flag.endswith('.png'):
            flag_set.add(item_flag.lower().replace('.png', ''))
    return flag_set


def get_no_flag_country_set():
    """
    获取没有国旗的国家集合
    :return:
    """
    flag_set = get_country_flag_set()
    # print(flag_set)
    country_set = get_server_country_set()
    # print(country_set)
    no_flag_country_set = country_set - flag_set
    no_country_flag_set = flag_set - country_set

    print(no_country_flag_set)
    print(len(no_country_flag_set))

    print(no_flag_country_set)
    print(len(no_flag_country_set))


if __name__ == '__main__':
    get_no_flag_country_set()
