#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/12 13:39
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : rekuten_test.py
# @Software: PyCharm

import requests

# requests.adapters.DEFAULT_RETRIES = 5

headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
            'Connection': 'keep-alive'
        }

url = 'https://search.rakuten.co.jp/search/mall/モップ/?p=1'
# url = 'https://www.baidu.com'
# sess = requests.session()

result = requests.get(url, headers=headers, timeout=10)
# result = sess.get(url, headers=headers, timeout=10)
print(result.text)
