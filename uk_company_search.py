#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/4/9 14:25
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : uk_company_search.py
# @Software: PyCharm

import requests
from lxml import etree

headers = {
    'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Connection': 'keep-alive'
}

sess = requests.session()
sess.request(method='GET', url='https://find-and-update.company-information.service.gov.uk/', headers=headers)
# print(res.text)
res = sess.request(method='GET', url='https://find-and-update.company-information.service.gov.uk/search?q=ibm', headers=headers)
# print(res.text)

selector = etree.HTML(res.text)
company_li_list = selector.xpath('.//ul[@id="results"]/li[@class="type-company"]')
for li in company_li_list:
    company_name = li.xpath('.//a/text()')[0].strip()
    company_info = li.xpath('.//p[last()-1]/text()')[0].strip()
    company_address = li.xpath('.//p[last()]/text()')[0].strip()
    print(company_name, company_info, company_address)
