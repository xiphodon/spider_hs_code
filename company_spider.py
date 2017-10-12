#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/11 16:31
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider.py
# @Software: PyCharm

# 引入WebDriver包
from selenium import webdriver
# 引入WebDriver Keys包
from selenium.webdriver.common.keys import Keys
import time
# 创建浏览器对象
driver = webdriver.Firefox()
# browser = webdriver.Chrome()
# 导航到百度主页
driver.get('http://listings.findthecompany.com/')
# driver.get('https://www.baidu.com')
time.sleep(2)
# 检查标题是否为‘百度一下，你就知道’
assert '百度一下，你就知道' in driver.title
# 找到名字为wd的元素，赋值给elem
elem = driver.find_element_by_name('wd') # 找到搜索框
elem.send_keys('selenium' + Keys.RETURN)# 搜索selenium

time.sleep(2)

# # 原因是webdriver仍默认在原页面下获取标签等信息，采用切换页面句柄的方式解决：
# #####获取当前页面句柄
# normal_window = driver.current_window_handle
# #####获取所有页面句柄
# all_Handles = driver.window_handles
# #####如果新的pay_window句柄不是当前句柄，用switch_to_window方法切换
# for pay_window in all_Handles:
#     if pay_window != normal_window:
#         driver.switch_to.window(pay_window)
# #####希望可以帮到你。



with open(r'C:\Users\topeasecpb\Desktop\test.html', 'w', encoding='utf8') as fp:
    fp.write(driver.page_source)