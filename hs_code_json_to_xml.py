#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/1/10 15:56
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : hs_code_json_to_xml.py
# @Software: PyCharm

# hs编码的json文件，转成xml格式，并存储在SqlServer

import dicttoxml
import json
import pymssql
import settings


def read_hs_code_json():
    """
    读取hs编码的json文件
    :return: hs编码字典
    """
    with open(r'E:\work_all\topease\hs_spider_data\hs_code_desc\spider_hs_code_3.json', 'r', encoding='utf8') as fp:
        hs_code_json_dict = json.loads(fp.read())
    return hs_code_json_dict


def hs_code_json_to_xml(json_dict):
    """
    hs编码的json字典，转成xml字符串
    :param json_dict: json字典
    :return: xml字符串
    """
    return str(dicttoxml.dicttoxml(json_dict, attr_type=False, root=False), encoding='utf8')


def start():
    """
    程序入口
    :return:
    """
    hs_code_json_dict = read_hs_code_json()

    conn = pymssql.connect(host=settings.host, user=settings.user, password=settings.password,
                           database=settings.database_hs_code, charset=settings.charset)
    cur = conn.cursor()

    if not cur:
        raise (NameError, "数据库连接失败")

    for item_dict in hs_code_json_dict:
        item_xml = hs_code_json_to_xml(item_dict)

        try:
            sql_str = "insert into hscode([content]) values(N'%s')" % (item_xml,)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
        except Exception as e:
            print(sql_str)
            print(e)

    conn.close()


if __name__ == '__main__':
    start()
