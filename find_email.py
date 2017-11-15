#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/10 10:39
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : find_email.py
# @Software: PyCharm

import requests
import re


def find_email_yingyanso(target_url):
    """
    查找邮箱（通过yingyanso）
    :return:
    """
    url = r'https://www.yingyanso.cn/getemail'
    headers = {
        'Host': 'www.yingyanso.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://www.yingyanso.cn/',
        'Connection': 'keep-alive'
    }
    form_data = {
        'weburl': target_url,
        'homesspd': '1'
    }
    result = requests.post(url, data=form_data, timeout=60, headers=headers)
    if result.status_code == 200:
        print(result.text)


def check_url(target_url):
    """
    检查目标url格式
    :param target_url:
    :return:
    """
    return target_url


def find_email_from_domain(target_url):
    """
    通过域名查找邮箱
    :param target_url:
    :return:
    """
    target_url = check_url(target_url)
    result = requests.get(target_url)
    if result.status_code == 200:
        content = result.text
        # print(content)
        re_result = re.search(r'([a-zA-Z0-9_.-]?)+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+', content)
        if re_result:
            print(re_result.group())
        else:
            print('匹配失败')
    else:
        print('请求异常')


if __name__ == '__main__':
    url_1 = r'http://www.festo.com'
    url_2 = r'http://www.closeoutdistributors.com'
    find_email_yingyanso(url_2)
    # find_email_from_domain(url_2)