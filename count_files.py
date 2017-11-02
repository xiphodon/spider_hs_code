#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/1 15:36
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : count_files.py
# @Software: PyCharm

# 统计文件数量

import company_spider_4
import os
import time


def count_spider4_desc_files():
    """
    统计spider4的公司详情页数量
    :return:
    """
    last_count = 0
    while True:
        files_count = len(os.listdir(company_spider_4.company_desc_list_dir_path))
        print('\r' + str(files_count) + '  +' + str(files_count - last_count) + '  ' +
              time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), end='')
        last_count = files_count
        time.sleep(15)


if __name__ == '__main__':
    count_spider4_desc_files()
