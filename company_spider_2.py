#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/12 10:41
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_2.py
# @Software: PyCharm

import requests
import json
# import os
from bs4 import BeautifulSoup
import time
from selenium import webdriver

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}

headers2 = {
    'Host': 'listings.findthecompany.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate',
    'Referer': 'http://listings.findthecompany.com/ajax_search?_len=20&page=0&app_id=1662&_sortfld=sales_volume_us&'
               '_sortdir=DESC&_fil[0][field]=company_name&_fil[0][operator]=LIKE&_fil[0][value]=zhong&'
               '_fil[0][ignore]=false&_tpl=srp&head[]=company_name&head[]=_GC_address&head[]=total_employees&'
               'head[]=employees_here&head[]=sales_volume_us&head[]=year_started&head[]=citystate&'
               'head[]=localeze_classification&head[]=id&head[]=_encoded_title',
    'Cookie': '_ga=GA1.2.1403701078.1507631177; _giq_lv=1509952207342; _ftbuptc=SxAEPiymcTY9wXnohJPJeAqaLfDafy9f;'
              ' D_IID=0789754B-97E0-3D63-AEFB-AF4642DD9F80; D_UID=F5D9A8CD-0EF2-30BA-97B8-9C6968366730;'
              ' D_ZID=C2D50D29-E4E7-32F5-B7D3-0900FF08D241; D_ZUID=23CA042A-AF95-3177-9280-EB354F1E1942;'
              ' D_HID=6843501F-9D13-381D-AFDC-70061D5D1317;'
              ' D_SID=116.247.118.146:F19tAHFhBNvnXXdh3pxsq655rtNcMqK5LoakIwJepQ0; __ybotu=j8lgp40t4jbd4d1k9i;'
              ' __ybotv=1509954008570; __gads=ID=9b91696825f75dc4:T=1507631216:S=ALNI_MYKZXM-hQr9skD-wecq7VLDv6bcKg;'
              ' __ats=1509954007341; _gid=GA1.2.1109393205.1509938844; _test_cookie=1; _gqpv=1;'
              ' _ftbuptcs=IJ4lKdzViuke0Kkqz07A2nx0SpHsXDGi; OX_plg=swf|shk|pm',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'If-Modified-Since': 'Mon, 06 Nov 2017 08:17:20 +0000',
    'If-None-Match': 'W/"15608ff39108411e72343f4544b2cb0f-p33333"',
    'Cache-Control': 'max-age=0'
}


def download_all_us_company_json():
    """
    下载所有美国公司的json数据
    :return:
    """
    url = r'http://listings.findthecompany.com/ajax_search?' \
          r'_len=100&page=0&app_id=1662&_sortfld=sales_volume_us' \
          r'&_sortdir=DESC&_fil[0][field]=phys_country_code' \
          r'&_fil[0][operator]==&_fil[0][value]=805&_tpl=srp' \
          r'&head[]=company_name&head[]=_GC_address' \
          r'&head[]=total_employees&head[]=employees_here&head[]=sales_volume_us' \
          r'&head[]=year_started&head[]=citystate&head[]=localeze_classification' \
          r'&head[]=id&head[]=_encoded_title'
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
    """
    从获取的测试html中获取json字符串
    :return:
    """
    with open(r'C:\Users\topeasecpb\Desktop\test.html', 'r', encoding='utf8') as fp:
        data_stream = fp.read()

    soup = BeautifulSoup(data_stream, 'html.parser')
    data_select = soup.select('pre.data')
    data_str = data_select[0].text.strip()
    print(json.loads(data_str))


def download_html_test():
    """
    下载网页测试
    :return:
    """
    url = r'http://listings.findthecompany.com/ajax_search?_len=20&page=0&app_id=1662&_sortfld=sales_volume_us&' \
          r'_sortdir=DESC&_fil[0][field]=company_name&_fil[0][operator]=LIKE&_fil[0][value]=zhong&' \
          r'_fil[0][ignore]=false&_tpl=srp&head[]=company_name&head[]=_GC_address&head[]=total_employees&' \
          r'head[]=employees_here&head[]=sales_volume_us&head[]=year_started&head[]=citystate&' \
          r'head[]=localeze_classification&head[]=id&head[]=_encoded_title'

    r = requests.get(url, headers=headers2)
    print(r.text)


if __name__ == '__main__':
    # download_all_us_company_json()
    # parse_test_html_get_json()
    download_html_test()
