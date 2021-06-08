#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/4/15 14:12
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : base_spider.py
# @Software: PyCharm
import os
import random
import re
import time
from typing import Union, Optional


class BaseSpider:
    """
    爬虫基类
    """

    def __init__(self):
        """
        初始化
        """
        self.data_progress = DataProgress()

    @staticmethod
    def mkdir(dir_path):
        """
        创建文件夹
        :param dir_path:
        :return:
        """
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

    @staticmethod
    def create_file(file_path, content: str):
        """
        创建文件
        :param file_path: 标记文件路径
        :param content: 内容
        :return:
        """
        with open(file_path, 'w', encoding='utf8') as fp:
            fp.write(content)

    @staticmethod
    def random_float(number_1: Union[float, int], number_2: Union[float, int]) -> float:
        """
        随机一个范围内的小数
        :param number_1:
        :param number_2:
        :return:
        """
        if number_2 > number_1:
            number_1, number_2 = number_2, number_1
        delta = number_2 - number_1
        return random.random() * delta + number_1

    @staticmethod
    def data_list_get_first(data_list: list, default=''):
        """
        数据列表获取第一个数据
        :param data_list:
        :param default:
        :return:
        """
        return data_list[0] if len(data_list) > 0 else default

    @staticmethod
    def clean_text(text):
        """
        清洗文字，去除空白符
        :param text:
        :return:
        """
        return re.sub(r'\s{2,}|(\r\n)+|(\r)+|(\n)+|(<br>)+|(<br/>)+|(\u200b)+', ' ', text).strip()


class DataProgress:
    """
    数据进度
    """
    def __init__(self, last_data_used_time_size=50):
        """
        初始化
        """
        self.last_data_time: Optional[int] = None
        self.last_data_used_time_list: list = list()
        self.last_data_used_time_size = last_data_used_time_size

    def print_data_progress(self, current_value: int, total_value: int, data_progress_display_len=50):
        """
        打印进度条
        :param current_value:
        :param total_value:
        :param data_progress_display_len:
        :return:
        """
        data_progress_display_str = self.draw_data_progress(current_value, total_value, data_progress_display_len)
        time_progress_display_str = self.draw_time_progress(current_value, total_value)
        print(f'\r{data_progress_display_str} {time_progress_display_str}', end='')

    def draw_time_progress(self, current_value: int, total_value: int):
        """
        绘制时间相关进度
        :param current_value:
        :param total_value:
        :return:
        """
        current_time = time.time()
        speed = 0
        time_unit = 's'
        if len(self.last_data_used_time_list) < self.last_data_used_time_size:
            self.last_data_used_time_list.append(current_time)
        else:
            self.last_data_used_time_list.pop(0)
            self.last_data_used_time_list.append(current_time)
            speed = len(self.last_data_used_time_list) / (
                        self.last_data_used_time_list[-1] - self.last_data_used_time_list[0])
            if speed < 1:
                speed *= 60
                time_unit = 'min'
        speed_str = f'{round(speed, 2):.2f}/{time_unit}'

        remain_time = '∞'
        if speed > 0:
            remain_time = (total_value - current_value) / speed
            # print(remain_time, time_unit)
            if time_unit == 's' and remain_time >= 60:
                remain_time /= 60
                time_unit = 'min'
            if time_unit == 'min' and remain_time >= 60:
                remain_time /= 60
                time_unit = 'hour'
            remain_time = round(remain_time, 2)
        remain_time_str = f'{remain_time}{time_unit}'
        return f'{speed_str} {remain_time_str}'

    @staticmethod
    def draw_data_progress(current_value: int, total_value: int, data_progress_display_len=50):
        """
        绘制数据进度条
        :param current_value: 当前进度数字
        :param total_value: 总计数字
        :param data_progress_display_len: 进度条显示长度
        :return:
        """
        data_progress = current_value / total_value
        draw_progress_size = max(round(data_progress * data_progress_display_len), 1)
        progress_1 = ['='] * (draw_progress_size - 1)
        progress_1.append('>')
        progress_2 = ['·'] * (data_progress_display_len - draw_progress_size)
        progress_bar = progress_1 + progress_2
        progress_bar_str = ''.join(progress_bar)
        return f'|{progress_bar_str}| {current_value}/{total_value} {round(data_progress * 100, 2):.2f}%'
