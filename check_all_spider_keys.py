#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/15 9:14
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : check_all_spider_keys.py
# @Software: PyCharm

import matplotlib.pyplot as plt
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


def check_spider_3_all_len_value():
    """
    检查并搜集当前spider的结果的所有的key
    :return:
    """
    check_spider_all_len_value_stat(company_spider_3.read_company_desc_list_has_web_json(),
                                    company_spider_3.all_keys_set_json_path)


def check_spider_4_all_len_value():
    """
    检查并搜集当前spider的结果的所有的key
    :return:
    """
    check_spider_all_len_value_stat(company_spider_4.read_company_desc_has_phone_str_json(),
                                    company_spider_4.company_desc_all_keys_json_path_2)


def check_spider_5_all_len_value():
    """
    检查并搜集当前spider的结果的所有的key
    :return:
    """
    check_spider_all_len_value_stat(company_spider_5.read_company_desc_list_json(),
                                    company_spider_5.all_keys_json_path)


def check_spider_6_all_len_value():
    """
    检查并搜集当前spider的结果的所有的key
    :return:
    """
    check_spider_all_len_value_stat(company_spider_6.read_company_desc_list_json(),
                                    company_spider_6.all_keys_list_json_path)


def check_spider_all_keys(json_list, write_file_path):
    """
    检查并收集所有key
    :return:
    """
    all_keys_dict = dict()

    count = 0
    all_count = len(json_list)

    for item_dict in json_list:
        for key, value in dict(item_dict).items():
            try:
                len_value = len(value)
            except TypeError:
                len_value = len(str(int(value)))

            if key not in all_keys_dict:
                all_keys_dict[key] = [len_value, value]
            else:
                if len_value > all_keys_dict[key][0]:
                    all_keys_dict[key] = [len_value, value]

        count += 1
        print('\r%.4f%%' % (count / all_count * 100), end='')

    with open(write_file_path.replace('.json', '_dict.json'), 'w', encoding='utf8') as fp:
        fp.write(json.dumps(all_keys_dict))


def check_spider_all_len_value_stat(json_list, write_file_path):
    """
    检查并收集各个字段长度的统计
    :return:
    """
    all_keys_dict = dict()

    count = 0
    all_count = len(json_list)

    for item_dict in json_list:
        for key, value in dict(item_dict).items():
            try:
                len_value = len(value)
            except TypeError:
                len_value = len(str(int(value)))

            if key not in all_keys_dict:
                all_keys_dict[key] = [len_value]
            else:
                all_keys_dict[key].append(len_value)

        count += 1
        print('\r%.4f%%' % (count / all_count * 100), end='')

    with open(write_file_path.replace('.json', '_stat_dict.json'), 'w', encoding='utf8') as fp:
        fp.write(json.dumps(all_keys_dict))


def show_len_value(read_file_path):
    """
    可视化数据集合的value的长度
    :return:
    """
    # 加载数据
    # reviews = pd.read_csv("fandango_scores.csv")

    with open(read_file_path.replace('.json', '_stat_dict.json'), 'r', encoding='utf8') as fp:
        temp_dict = json.loads(fp.read())

    for key, value in temp_dict.items():
        fig, ax = plt.subplots()
        # 绘制散点图
        ax.scatter(range(len(value)), value)
        ax.set_xlabel('scatter id')
        ax.set_ylabel('len({})'.format(key))
        plt.show()
        # break

    # # 绘制散点图（翻转x，y坐标对比）
    # fig = plt.figure(figsize=(12, 6))
    # ax1 = fig.add_subplot(1, 2, 1)
    # ax2 = fig.add_subplot(1, 2, 2)
    #
    # # 绘制散点图
    # ax1.scatter(reviews["Fandango_Ratingvalue"], reviews["RT_user_norm"])
    # ax1.set_xlabel("Fandango")
    # ax1.set_ylabel("Rotten Tomatoes")
    # ax1.set_title("图1")  # 子图标题
    # # 绘制散点图
    # ax2.scatter(reviews["RT_user_norm"], reviews["Fandango_Ratingvalue"])
    # ax2.set_xlabel("Rotten Tomatoes")
    # ax2.set_ylabel("Fandango")
    # ax2.set_title("图2")  # 子图标题
    #
    # plt.show()


if __name__ == '__main__':
    # check_spider_3_all_keys()
    # check_spider_4_all_keys()
    # check_spider_5_all_keys()
    # check_spider_6_all_keys()
    # check_spider_3_all_len_value()
    # check_spider_4_all_len_value()
    # check_spider_5_all_len_value()
    # check_spider_6_all_len_value()
    # show_len_value(company_spider_3.all_keys_set_json_path)
    # show_len_value(company_spider_4.company_desc_all_keys_json_path_2)
    # show_len_value(company_spider_5.all_keys_json_path)
    show_len_value(company_spider_6.all_keys_list_json_path)
