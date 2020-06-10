#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/4/1 11:31
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : area.py
# @Software: PyCharm


import pymssql
import settings


def readAreaToText():
    """
    读取省市区到文本
    :return:
    """
    conn = pymssql.connect(host='116.247.118.146', user='sa', password='sa2017!@#',
                           database='oa_test', charset=settings.charset)
    cur = conn.cursor()

    if not cur:
        raise (NameError, "数据库连接失败")
    else:
        print('数据库连接成功')

    sql_str = 'select name from sys_area'
    cur.execute(sql_str.encode('utf8'))
    area_data = cur.fetchall()

    area_stop_words_set = set()
    for item in area_data:
        if len(item) > 0:
            area_name = str(item[0])
            # 直辖市、自治区、特别行政区、地区、新区、自治县、自治州、省、市、区、县、（筛去后一个字时不过滤）、☆
            area_alias = clean_area_name(area_name)
            area_stop_words_set.add(area_name)
            area_stop_words_set.add(area_alias)

    conn.close()

    with open(r'C:\Users\topeasecpb\Desktop\area_stop_words.txt', 'w', encoding='utf8') as fp:
        for area_name in sorted(area_stop_words_set):
            fp.write(area_name + '\n')


def clean_area_name(area_name: str):
    """
    清洗区域名
    :param area_name:
    :return:
    """
    per_clean_list = ['☆', ' ']
    clean_list = ['直辖市', '自治区', '特别行政区', '地区', '新区', '自治县', '自治州', '省', '市', '区', '县']

    for per_clean_char in per_clean_list:
        area_name = area_name.strip(per_clean_char)

    for clean_char in clean_list:
        if area_name.endswith(clean_char):
            area_alias = area_name.rstrip(clean_char)
            if len(area_alias) > 1:
                return area_alias
    return area_name


if __name__ == '__main__':
    readAreaToText()
