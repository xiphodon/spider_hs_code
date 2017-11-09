#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/1 15:36
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : count_files.py
# @Software: PyCharm

# 统计文件数量

import company_spider_4
import company_spider_3
import os
import time


def count_dir_files(dir_path):
    """
    统计某文件夹内文件数
    :return:
    """
    last_count = 0
    while True:
        files_count = len(os.listdir(dir_path))
        print('\r' + str(files_count) + '  +' + str(files_count - last_count) + '  ' +
              time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), end='')
        last_count = files_count
        time.sleep(15)


def count_spider4_desc_files():
    """
    统计spider4的公司详情页数量
    :return:
    """
    count_dir_files(company_spider_4.company_desc_list_dir_path)


def count_spider3_desc_files():
    """
    统计spider3的公司详情页数量
    :return:
    """
    count_dir_files(company_spider_3.company_desc_has_web_list_dir_path)


def count_spider4_img_to_str_json_files():
    """
    统计spider4的公司手机号json文件数量
    :return:
    """
    count_dir_files(company_spider_4.phone_str_json_list_dir_path)


def count_spider4_phone_img_dirs():
    """
    统计spider4的手机图片文件夹数量
    :return:
    """
    count_dir_files(company_spider_4.phone_img_list_dir_path)


def count_spider4_phone_img_files(root_dir_path):
    """
    统计spider4的手机图片文件数量
    :return:
    """
    last_count = 0
    while True:
        files_count = 0
        for item_dir in os.listdir(root_dir_path):
            item_dir_path = os.path.join(root_dir_path, item_dir)
            temp_files_count = len(os.listdir(item_dir_path))
            files_count += temp_files_count
            print('\r' + str(last_count) + '---------' + str(files_count), end='')
        print('\r' + str(files_count) + '  +' + str(files_count - last_count) + '  ' +
              time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), end='')
        last_count = files_count
        time.sleep(60 * 10)


if __name__ == '__main__':
    # count_spider4_desc_files()
    # count_spider3_desc_files()
    # count_spider4_phone_img_dirs()
    # count_spider4_phone_img_files(company_spider_4.phone_img_list_dir_path)
    count_spider4_img_to_str_json_files()
