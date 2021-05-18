#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/5/11 14:01
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : proxies_test.py
# @Software: PyCharm
import time

import requests

username = 'test'
password = 'fdiuiwu'
ip = '107.167.89.234'
port = '5555'

proxies = {
    'http': f'http://{username}:{password}@{ip}:{port}',
    'https': f'http://{username}:{password}@{ip}:{port}'
}

now = time.time()
for i in range(10):
    result = requests.get('https://www.europages.co.uk/')
    # result = requests.get('https://www.indotrading.com/companycatalog/', proxies=proxies)
    print(i)
print(time.time() - now)
# print(result.text)