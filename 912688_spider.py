#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/3/19 16:40
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : 912688_spider.py
# @Software: PyCharm
import hashlib
import json
import os
import random

from lxml import etree

from WhileRequests import WhileRequests


class SouHaoHuoSpider:
    """
    搜好货爬虫
    """
    home_url = r'https://www.912688.com/'
    company_lib_url = r'https://www.912688.com/gongsi/'

    home_dir = r'E:\souhaohuo'
    home_page_path = os.path.join(home_dir, 'home.html')
    company_class_index_json_path = os.path.join(home_dir, 'company_class_index.json')

    company_list_pages_home_dir_path = os.path.join(home_dir, 'company_list_pages')

    def __init__(self, force_save=False):
        """
        初始化
        :param force_save:
        """
        self.force_save = force_save

    @staticmethod
    def mkdir(dir_path):
        """
        创建文件夹
        :param dir_path:
        :return:
        """
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

    def download_company_lib_home_page(self):
        """
        下载公司库首页
        :return:
        """
        if os.path.exists(self.home_page_path):
            print('company lib home page already exists!')
            return
        requests = WhileRequests()
        result = requests.get(self.company_lib_url, request_times=3, sleep_time=1)
        result.encoding = 'utf8'
        with open(self.home_page_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)
        print('company lib home page download OK!')

    def parse_company_lib_home_page(self):
        """
        解析公司库首页
        :return:
        """
        assert os.path.exists(self.home_page_path), 'company lib home page is not exists!'

        if os.path.exists(self.company_class_index_json_path):
            print('company class index json already exists!')
            return

        with open(self.home_page_path, 'r', encoding='utf8') as fp:
            file_content = fp.read()

        index_list = []

        selector = etree.HTML(file_content)
        tab_div_list = selector.xpath('//div[@class="base-div jc"]')
        md5 = hashlib.md5()
        for tab_div in tab_div_list:
            tab_dl_list = tab_div.xpath('./dl')
            for tab_dl in tab_dl_list:
                class_1_text = tab_dl.xpath('.//h2/text()')[0]
                a_href_list = tab_dl.xpath('.//a/@href')
                class_2_text_list = tab_dl.xpath('.//a/text()')
                # print(class_1_text)
                # print(a_href_list)
                # print(class_2_text_list)
                for class_2_text, class_2_href in zip(class_2_text_list, a_href_list):
                    md5.update(str(class_2_href).encode(encoding='utf8'))
                    href_md5 = md5.hexdigest()
                    index_list.append({
                        'href_md5': href_md5,
                        'class_1': class_1_text,
                        'class_2': class_2_text,
                        'href': class_2_href
                    })
            # break
        with open(self.company_class_index_json_path, 'w', encoding='utf8') as fp:
            fp.write(json.dumps(index_list))
        print('company class index json build OK!')

    def download_company_list_page(self):
        """
        下载公司列表页
        :return:
        """
        assert os.path.exists(self.company_class_index_json_path), 'company class index json is not exists!'

        with open(self.company_class_index_json_path, 'r', encoding='utf8') as fp:
            company_class_index_list = json.load(fp)

        if not os.path.exists(self.company_list_pages_home_dir_path):
            os.mkdir(self.company_list_pages_home_dir_path)

        requests = WhileRequests()

        for company_class_index in company_class_index_list:
            print(company_class_index)

            company_class_dir_path = os.path.join(
                self.company_list_pages_home_dir_path,
                f"{company_class_index['class_1']}_{company_class_index['class_2']}".replace('/', '、')
            )
            if not os.path.exists(company_class_dir_path):
                os.mkdir(company_class_dir_path)

            over_file_path = os.path.join(company_class_dir_path, '_over.txt')
            if not self.force_save and os.path.exists(over_file_path):
                print('-- 发现采集完毕标记文件，跳过该分类 --')
                continue

            page_no = 1
            while True:
                # 采集公司列表
                href = f"{company_class_index['href'][:-5]}-mod-page{page_no}.html"
                print(f'request {company_class_dir_path} {href}')
                sleep_time = self.random_float(0.2, 0.4) if page_no != 1 else self.random_float(1, 1.2)
                result = requests.get(href, request_times=3, sleep_time=sleep_time)
                result.encoding = 'utf8'

                selector = etree.HTML(result.text)
                if page_no == 1 and not self.force_save:
                    page_size_str_list = selector.xpath('//div[@class="s-mod-page mb30"]/span[@class="total"]/text()')
                    if len(page_size_str_list) == 0:
                        print('该分类无内容')
                        self.create_file(over_file_path, '0')
                        break
                    page_size_str = page_size_str_list[0][1:-1]
                    page_size_int = int(page_size_str)
                    print(f'该类别总页码数：{page_size_int}')
                    if len(os.listdir(company_class_dir_path)) == page_size_int:
                        print('-- 该类别下所有页面已下载过--')
                        self.create_file(over_file_path, page_size_str)
                        break

                file_path = os.path.join(company_class_dir_path, f'{page_no:03}.html')
                self.create_file(file_path, result.text)

                next_a = selector.xpath('//a[@class="page_next page-n" and @data-b="page"]')
                if len(next_a) > 0:
                    page_no += 1
                else:
                    # 页面已遍历完毕，增加标记文件
                    self.create_file(over_file_path, str(page_no))
                    break
            # break

    @staticmethod
    def create_file(file_path, content: str):
        """
        创建文件
        :param file_path: 标记文件路径
        :param content: 内容
        :return:
        """
        with open(file_path, 'w', encoding='utf8') as fp:
            fp.write(content)

    @staticmethod
    def random_float(number_1: [float, int], number_2: [float, int]) -> float:
        """
        随机一个范围内的小数
        :param number_1:
        :param number_2:
        :return:
        """
        if number_2 > number_1:
            number_1, number_2 = number_2, number_1
        delta = number_2 - number_1
        return random.random() * delta + number_1


if __name__ == '__main__':
    shh_spider = SouHaoHuoSpider()
    shh_spider.download_company_lib_home_page()
    shh_spider.parse_company_lib_home_page()
    shh_spider.download_company_list_page()






