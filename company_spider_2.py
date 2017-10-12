#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 10:41
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_2.py
# @Software: PyCharm

import requests
import json
import os
from bs4 import BeautifulSoup
import time
from selenium import webdriver

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}

def download_all_us_company_json():
    '''
    下载所有美国公司的json数据
    :return:
    '''
    url = r'http://listings.findthecompany.com/ajax_search?_len=100&page=0&app_id=1662&_sortfld=sales_volume_us&_sortdir=DESC&_fil[0][field]=phys_country_code&_fil[0][operator]==&_fil[0][value]=805&_tpl=srp&head[]=company_name&head[]=_GC_address&head[]=total_employees&head[]=employees_here&head[]=sales_volume_us&head[]=year_started&head[]=citystate&head[]=localeze_classification&head[]=id&head[]=_encoded_title'
    driver = webdriver.Firefox()
    driver.get(url)
    time.sleep(5)

    driver.find_element_by_id('tab-1').click()
    time.sleep(1)

    # print(driver.page_source)

    with open(r'C:\Users\topeasecpb\Desktop\test.html', 'w', encoding='utf8') as fp:
        fp.write(driver.page_source)

    # r = requests.get(url, headers=headers, timeout=10)
    # print(r.text)


def parse_test_html_get_json():
    '''
    从获取的测试html中获取json字符串
    :return:
    '''
    with open(r'C:\Users\topeasecpb\Desktop\test.html', 'r', encoding='utf8') as fp:
        data_stream = fp.read()

    soup = BeautifulSoup(data_stream, 'html.parser')
    data_select = soup.select('pre.data')
    data_str = data_select[0].text.strip()
    print(json.loads(data_str))


if __name__ == '__main__':
    # download_all_us_company_json()
    parse_test_html_get_json()