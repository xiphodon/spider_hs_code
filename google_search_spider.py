#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/10/28 9:37
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : google_search_spider.py
# @Software: PyCharm

import pandas as pd
import json


def save_ali_product():
    """
    转存阿里产品列表
    :return:
    """
    df = pd.DataFrame(pd.read_excel(r'C:\Users\topeasecpb\Desktop\ali产品类目.xlsx'))
    data = df['Name'].values.tolist()
    print(data)

    with open('./ali_product_list.json', 'w', encoding='utf8') as fp:
        json.dump(data, fp)


def export_google_search_log():
    """
    输出google查询日志结果
    :return:
    """
    result_dict = dict()
    with open(r'E:\work_all\topease\特易数据中心\ali_google_search\log2.txt',
              'r', encoding='utf8') as fp:
        for i, line in enumerate(fp):
            # if i > 1500:
            #     break
            line = str(line).strip()
            line_json = json.loads(line)

            key = line_json.get('key', '').replace('&amp;', '&').replace('&#39;', '`').strip()
            url = line_json.get('url', '').replace('广告', '').strip()

            if key in result_dict:
                result_dict[key].add(url)
            else:
                temp_set = set()
                temp_set.add(url)
                result_dict[key] = temp_set

    # url_count = 0
    # for k, v in result_dict.items():
    #     url_count += len(v)
    # print(url_count)

    result_list = list()
    for k, v in result_dict.items():
        for u in v:
            result_list.append([k, u])

    df = pd.DataFrame(result_list, columns=['关键字', '广告链接'])
    df = df.reset_index()
    df['index'] = df['index'].apply(lambda x: int(x) + 1)
    print(df.head())
    df = df.set_index(['关键字', 'index'])
    df.to_excel(r'E:\work_all\topease\特易数据中心\ali_google_search\ali_google_products_ads2.xlsx')


def start():
    """
    入口
    :return:
    """
    # save_ali_product()
    export_google_search_log()


if __name__ == '__main__':
    start()
