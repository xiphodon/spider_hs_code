#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/11 9:50
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : firmseeker.py
# @Software: PyCharm

import requests
import json

# 引入WebDriver包
from selenium import webdriver
# 引入WebDriver Keys包
from selenium.webdriver.common.keys import Keys
import time

# 进入浏览器设置
options = webdriver.ChromeOptions()
# 设置中文
options.add_argument('lang=zh_CN.UTF-8')
# 更换头部
options.add_argument('user-agent="Mozilla/5.0 (iPod; U; CPU iPhone OS 2_1 like Mac OS X; ja-jp) '
                     'AppleWebKit/525.18.1 (KHTML, like Gecko) Version/3.1.1 Mobile/5F137 Safari/525.20"')
# 创建浏览器对象
# driver = webdriver.PhantomJS()
driver = webdriver.Chrome(chrome_options=options)
driver.set_window_size(800, 600)

github_username = 'yelangzhiwu'
github_password = 'x421558739c'

headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
        'Connection': 'keep-alive'
    }


def check_google_key(google_key):
    """
    检查googlekey是否可用
    :return:
    """
    # google_key = 'AIzaSyAUsnERWvgUrNKQy4YvHAaeg99HdhJLpTM'
    result = requests.get(f'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
                          f'location=-33.8670522,151.1957362&radius=5000&types=food&key={google_key}', headers=headers)
    status = json.loads(result.text).get('status').strip()
    print(status)
    if status == 'OK':
        return True
    else:
        return False


def start():
    """
    入口
    :return:
    """
    driver.get('https://github.com/login')
    time.sleep(2)
    title = driver.find_element_by_class_name('bubble-title').text.strip()
    print(title)

    if title == 'Sign in to GitHub':
        print('OK')

        # 找到用户名输入框
        elem_username = driver.find_element_by_name('login')
        elem_username.send_keys(github_username)

        # 找到密码输入框
        elem_password = driver.find_element_by_name('password')
        elem_password.send_keys(github_password)

        # 找到登录按钮
        elem_commit = driver.find_element_by_class_name('btn-block')
        elem_commit.click()



    # driver.quit()

    # page_no = 1
    # url = f'https://github.com/search?p={page_no}&q=maps.googleapis.com%2Fmaps%2Fapi%2Fplace%2Fnearbysearch&type=Code'
    # result = requests.get(url)
    # print(result.text)


if __name__ == '__main__':
    start()
