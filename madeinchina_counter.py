#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/21 16:33
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : madeinchina_counter.py
# @Software: PyCharm

import os
import threading


def count_download_files():
    """
    统计下载文件数量
    :return:
    """
    dir_path = r'E:\mic\company_page_list'

    count = 0

    for item_dir in os.listdir(dir_path):
        # print(item_dir)
        item_dir_path = os.path.join(dir_path, item_dir)
        for _ in os.listdir(item_dir_path):
            count += 1

    data_progress = round(count / 2300000 * 100, 4)
    data_progress = min(data_progress, 100)
    progress_bar_str = draw_data_progress(data_progress)

    if count >= 10000:
        count_list = list()
        count_list.insert(0, str(count // 10000))
        count_list.append(str(count % 10000))

        count_unit_list = ['个']
        count_unit_list.insert(0, '万')

        count_str = f'{count_list[0]} {count_unit_list[0]} {count_list[1]} {count_unit_list[1]}'
    else:
        count_str = f'{count} 个'

    return data_progress, progress_bar_str, count_str


def draw_data_progress(data_progress):
    """
    绘制数据进度条
    :return:
    """
    data_progress_len = 50
    draw_progress_ratio = round(data_progress / 100 * data_progress_len)
    progress_1 = ['='] * draw_progress_ratio
    if len(progress_1) > 1:
        progress_1 = progress_1[:-1]
        progress_1.append('>')
    progress_2 = ['·'] * (50 - draw_progress_ratio)
    progress_bar = progress_1 + progress_2
    progress_bar_str = ''.join(progress_bar)
    # print(f'\n{data_progress} %  |{progress_bar_str}|')
    return progress_bar_str


def print_download_stat():
    """
    打印下载统计
    :return:
    """
    data_progress, progress_bar_str, count_str = count_download_files()
    print(f'\r|{progress_bar_str}|  {data_progress} %  ({count_str})', end='')


def while_count_download_files():
    """
    循环统计下载文件量
    :return:
    """
    print_download_stat()
    timer = threading.Timer(10, while_count_download_files)
    timer.start()


def start():
    """
    入口
    :return:
    """
    # while_count_download_files()
    print_download_stat()


if __name__ == '__main__':
    start()
