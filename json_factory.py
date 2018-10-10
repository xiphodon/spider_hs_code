#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/27 11:47
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : json_factory.py
# @Software: PyCharm

import json


def split_json():
    """
    拆分json
    拆分（有网址、无网址）并去重（网址一致）
    :return:
    """
    with open(r'E:\workspace_all\workspace_py\yellowpages_spider\yellowpages_spider\data\germany_spider.json', 'r', encoding='utf8') as fp:
        data = json.load(fp)

    web_set = set()
    company_has_web_list = list()
    company_no_web_list = list()
    for item in data:
        company_website = dict(item).get('company_website', None)
        if company_website is not None and company_website != '':
            # 有网址
            if company_website not in web_set:
                # 没有收集此网址
                web_set.add(company_website)
                company_has_web_list.append(item)
        else:
            # 无网址
            company_no_web_list.append(item)

    with open(r'C:\Users\topeasecpb\Desktop\germany_spider_has_website.json', 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_has_web_list))

    with open(r'C:\Users\topeasecpb\Desktop\germany_spider_no_website.json', 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_no_web_list))


if __name__ == '__main__':
    split_json()
