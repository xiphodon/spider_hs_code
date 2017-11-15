#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/15 9:14
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : check_all_spider_keys.py
# @Software: PyCharm

import json
import company_spider_3
import company_spider_4
import company_spider_5
import company_spider_6


def check_spider_3_all_keys():
    """
    检查并搜集当前spider的结果的所有的key
    :return:
    """
    check_spider_all_keys(company_spider_3.read_company_desc_list_has_web_json(),
                          company_spider_3.all_keys_set_json_path)


def check_spider_4_all_keys():
    """
    检查并搜集当前spider的结果的所有的key
    :return:
    """
    check_spider_all_keys(company_spider_4.read_company_desc_has_phone_str_json(),
                          company_spider_4.company_desc_all_keys_json_path_2)


def check_spider_5_all_keys():
    """
    检查并搜集当前spider的结果的所有的key
    :return:
    """
    check_spider_all_keys(company_spider_5.read_company_desc_list_json(),
                          company_spider_5.all_keys_json_path)


def check_spider_6_all_keys():
    """
    检查并搜集当前spider的结果的所有的key
    :return:
    """
    check_spider_all_keys(company_spider_6.read_company_desc_list_json(),
                          company_spider_6.all_keys_list_json_path)


def check_spider_all_keys(json_list, write_file_path):
    """
    检查并收集所有key
    :return:
    """
    all_keys_set = set()

    count = 0
    all_count = len(json_list)

    for item_dict in json_list:
        count += 1
        print('\r%.4f%%' % (count / all_count * 100), end='')
        for key in item_dict:
            all_keys_set.add(key)

    with open(write_file_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(list(all_keys_set)))


if __name__ == '__main__':
    # check_spider_3_all_keys()
    # check_spider_4_all_keys()
    # check_spider_5_all_keys()
    check_spider_6_all_keys()
