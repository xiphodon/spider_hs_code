#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/22 8:49
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : check_my_ip.py
# @Software: PyCharm


import requests
import time
import datetime
from lxml import etree

# 睡眠时间
sleep_time = 5

used_ip_dict = dict()
last_used_ip = ''

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}


def while_requests_get(page_url, times=50000):
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            result = requests.get(page_url, headers=headers, timeout=5)
            # result = requests.get(page_url, headers=headers, proxies=proxies, timeout=5)
        except Exception as e:
            if while_times < times:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url)
                continue
            else:
                raise e
        else:
            return result


def get_my_ip_page():
    """
    获取我的外部ip显示的页面
    :return:
    """
    url = 'http://www.baidu.com/baidu?wd=ip'
    result = while_requests_get(url)
    return result


def parse_show_ip_page(content):
    """
    解析显示ip的页面
    :param content:页面内容
    :return:
    """
    selector = etree.HTML(content)
    ip_str = selector.xpath('//div[@id="content_left"]/div/div/div/div/table//tr/td/span/text()')
    # print(ip_str)
    if (ip_str is not None) and (len(ip_str) > 0):
        return ip_str[0]
    else:
        return 'none'


def clear_ip_str(ip_str):
    """
    清洗ip_str
    :param ip_str:
    :return:
    """
    return str(ip_str).strip().split('\xa0')[-1]


def collect_ip(ip_str):
    """
    收集ip
    :param ip_str:
    :return:
    """
    if ip_str != last_used_ip:
        if ip_str in used_ip_dict:
            used_ip_dict[ip_str] += 1
        else:
            used_ip_dict[ip_str] = 1

    return used_ip_dict


def format_print(_ip_str, _collect_ip_dict):
    """
    格式化输出
    :param _ip_str: 当前ip
    :param _collect_ip_dict: 所收集使用过的ip情况
    :return:
    """
    print(datetime.datetime.now(), '=========================')
    print('\t当前IP:\t' + _ip_str)

    if _ip_str != 'none':
        print('\t使用过的ip数:\t' + str(len(_collect_ip_dict)))
        # print('\t', _collect_ip_dict)

        often_used_ip_dict = dict()
        oftener_used_ip_dict = dict()
        oftenest_used_ip_dict = dict()
        for key, value in _collect_ip_dict.items():
            if value > 10:
                oftenest_used_ip_dict[key] = value
            if value > 5:
                oftener_used_ip_dict[key] = value
            if value > 1:
                often_used_ip_dict[key] = value

        print('\t重复使用过的ip数(>1)：' + str(len(often_used_ip_dict)))
        print('\t多次使用过的ip数(>5)：' + str(len(often_used_ip_dict)))
        print('\t高频使用过的ip数(>10)：' + str(len(often_used_ip_dict)))
        # print('\t', often_used_ip_dict)


def while_print_ip():
    """
    循环打印当前ip
    :return:
    """
    while True:
        result = get_my_ip_page()
        ip_str = parse_show_ip_page(result.text)
        ip_str = clear_ip_str(ip_str)
        collect_ip_dict = collect_ip(ip_str)
        format_print(ip_str, collect_ip_dict)
        global last_used_ip
        last_used_ip = ip_str
        time.sleep(sleep_time)


if __name__ == '__main__':
    while_print_ip()
