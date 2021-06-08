#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/5/21 2:59
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : thomasnet_spider.py
# @Software: PyCharm
import json
import os
import re

import requests
from lxml import etree

from WhileRequests import WhileRequests
from base_spider import BaseSpider


class ThomasnetSpider(BaseSpider):
    """
    北美数据采集
    """
    home_url = r'https://www.thomasnet.com'
    home_path = r'E:\thomasnet'

    home_page_path = os.path.join(home_path, 'home_page.html')
    category_json_path = os.path.join(home_path, 'category.json')
    company_list_pages_dir = os.path.join(home_path, 'company_list_pages_dir')

    company_href_json_path = os.path.join(home_path, 'company_href.json')
    unique_company_list_json_path = os.path.join(home_path, 'unique_company_list.json')

    def __init__(self, check_home_url=False):
        """
        初始化
        """
        self.requests = WhileRequests(headers={
            'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 920)',
            'Connection': 'keep-alive',
            'Accept-Language': 'en-us'
        })
        if check_home_url is True:
            self.requests.get(self.home_url)

    def download_home_page(self):
        """
        下载主页
        :return:
        """
        res = self.requests.get(self.home_url)
        self.create_file(self.home_page_path, res.text)

    def parse_home_page(self):
        """
        解析主页
        :return:
        """
        with open(self.home_page_path, 'r', encoding='utf8') as fp:
            content = fp.read()
        selector = etree.HTML(content)
        category_div_list = selector.xpath('.//li[@class="category-tab-nav"][1]//div[@class="category-lists__lists"]/div[contains(@class, "titled-list")]')
        print(category_div_list)
        print(len(category_div_list))
        category_data_list = list()
        for class_1_div in category_div_list:
            class_1_name = self.data_list_get_first(class_1_div.xpath('./h3/a/text()'))
            print(class_1_name)
            class_2_name_list = class_1_div.xpath('./ul/li/a/text()')
            class_2_href_list = class_1_div.xpath('./ul/li/a/@href')
            for class_2_name, class_2_href in zip(class_2_name_list, class_2_href_list):
                category_data_list.append({
                    'class_1': class_1_name,
                    'class_2': class_2_name,
                    'class_href': f'{self.home_url}{class_2_href}'
                })
                # print(class_1_name, class_2_name)
                # print(class_2_href)
        # print(category_data_list)
        with open(self.category_json_path, 'w', encoding='utf8') as fp:
            json.dump(category_data_list, fp)

    def download_company_list_pages(self):
        """
        下载公司列表页面
        :return:
        """
        self.mkdir(self.company_list_pages_dir)
        with open(self.category_json_path, 'r', encoding='utf8') as fp:
            data_list = json.load(fp)
        for item_class in data_list:
            class_1: str = item_class['class_1']
            class_2: str = item_class['class_2']
            class_href = item_class['class_href']

            class_1 = class_1.replace(r'/', '-').strip()
            class_2 = class_2.replace(r'/', '-').strip()
            class_1_dir_path = os.path.join(self.company_list_pages_dir, class_1)
            self.mkdir(class_1_dir_path)

            class_2_dir_path = os.path.join(class_1_dir_path, class_2)
            self.mkdir(class_2_dir_path)

            if '&cov=NA&' in class_href:
                href_head = class_href
            else:
                href_head = class_href[:class_href.rfind('-')]
            page_no = 0
            while True:
                page_no += 1
                print(f'{class_1}_{class_2}_{page_no}')

                if '&cov=NA&' in class_href:
                    href = f'{href_head}&pg={page_no}'
                else:
                    href = f'{href_head}-{page_no}.html'

                company_list_page_path = os.path.join(class_2_dir_path, f'{page_no}.html')
                if os.path.exists(company_list_page_path):
                    continue

                response = self.requests.get(href)
                selector = etree.HTML(response.text)
                pagination_ul_list = selector.xpath('.//ul[@class="pagination  pagination-sm justify-content-center text-center search-results__pagination"]')
                if len(pagination_ul_list) == 0:
                    break
                self.create_file(company_list_page_path, response.text)

            # break

    def parse_company_list_page(self):
        """
        解析公司列表页面
        :return:
        """
        all_company_href_list = list()
        for class_1_dir in os.listdir(self.company_list_pages_dir):
            # print(class_1_dir)
            class_1_path = os.path.join(self.company_list_pages_dir, class_1_dir)
            for class_2_dir in os.listdir(class_1_path):
                # print(class_2_dir)
                class_2_path = os.path.join(class_1_path, class_2_dir)
                for file_name in os.listdir(class_2_path):
                    # print(class_1_dir, class_2_dir, file_name, sep=', ')
                    file_path = os.path.join(class_2_path, file_name)
                    print(file_path)
                    with open(file_path, 'r', encoding='utf8') as fp:
                        content = fp.read()
                    selector = etree.HTML(content)
                    company_div_list = selector.xpath('.//main[@class="supplier-search-results__main"]/div[@data-supplier-tier]')
                    # print(company_div_list)
                    # print(len(company_div_list))
                    for company_div in company_div_list:
                        # print(company_div)
                        company_key_json = self.data_list_get_first(company_div.xpath('./@data-impression-tracking'))
                        company_key_dict = json.loads(company_key_json)
                        company_id = company_key_dict['company_id']

                        company_logo_a_style = self.data_list_get_first(company_div.xpath('./header/a/@style'))
                        # print(company_logo_a_style)
                        re_result = re.search(r"url\('(.*)'\)", company_logo_a_style)
                        if re_result and len(re_result.groups()) > 0:
                            logo_href = re_result.group(1)
                        else:
                            logo_href = ''

                        company_name_text = self.data_list_get_first(company_div.xpath('./header/div/h2/a/text()'))
                        company_info_href = self.data_list_get_first(company_div.xpath('./header/div/h2/a/@href'))
                        company_info_href = f'{self.home_url}{company_info_href}' if company_info_href else ''
                        # print(company_name_text)
                        # print(company_info_href)
                        company_href_dict = {
                            'id': company_id,
                            'company_name': company_name_text,
                            'company_href': company_info_href,
                            'logo_href': logo_href,
                            'class_1': class_1_dir,
                            'class_2': class_2_dir
                        }
                        all_company_href_list.append(company_href_dict)
                        print(company_href_dict)

                        # break
                    # break
                # break
            # break
        print(len(all_company_href_list))
        with open(self.company_href_json_path, 'w', encoding='utf8') as fp:
            json.dump(all_company_href_list, fp)

    def unique_all_company(self):
        """
        将公司列表相同公司合并
        :return:
        """
        with open(self.company_href_json_path, 'r', encoding='utf8') as fp:
            data_list = json.load(fp)

        unique_company_dict = dict()
        for i, data in enumerate(data_list):
            print(i)
            data: dict
            company_id = data['id']
            class_1 = data.pop('class_1')
            class_2 = data.pop('class_2')
            data['class_list'] = list()
            unique_company_dict.setdefault(company_id, data)['class_list'].append({
                'class_1': class_1,
                'class_2': class_2
            })

        print(len(unique_company_dict))
        with open(self.unique_company_list_json_path, 'w', encoding='utf8') as fp:
            json.dump(unique_company_dict, fp)

    def unique_all_company_view(self):
        """
        唯一公司数据查看
        :return:
        """
        with open(self.unique_company_list_json_path, 'r', encoding='utf8') as fp:
            data_dict = json.load(fp)
        for k, v in data_dict.items():
            print(v['class_list'])
        print(len(data_dict))


if __name__ == '__main__':
    ts = ThomasnetSpider(check_home_url=True)
    # ts.download_home_page()
    # ts.parse_home_page()
    # ts.download_company_list_pages()
    # ts.parse_company_list_page()
    # ts.unique_all_company()
    ts.unique_all_company_view()
