#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/4/15 13:48
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : europages_spider.py
# @Software: PyCharm
from gevent import pool, monkey;

monkey.patch_all()

import hashlib
import json
import os
from lxml import etree

from WhileRequests import WhileRequests
from base_spider import BaseSpider


class EuroPagesSpider(BaseSpider):
    """
    europages 爬虫
    """
    home_url = r'https://www.europages.co.uk'
    business_directory_url = home_url + r'/business-directory-europe.html'

    home_dir = r'E:\euro_pages'
    business_directory_page_path = os.path.join(home_dir, 'business_directory.html')
    business_directory_json_path = os.path.join(home_dir, 'business_directory.json')

    company_item_class_page_dir = os.path.join(home_dir, 'company_item_class_dir')
    company_class_json_path = os.path.join(home_dir, 'company_class.json')
    whole_class_json_path = os.path.join(home_dir, 'whole_class.json')

    # https://www.europages.co.uk/companies/results.html?ih=01570C
    activity_company_list_url = home_url + r'/companies/results.html'
    activity_company_list_pages_dir = os.path.join(home_dir, 'activity_company_list_pages_dir')

    activity_company_list_json_dir = os.path.join(home_dir, 'activity_company_list_json_dir')
    unique_company_list_json_path = os.path.join(home_dir, 'unique_company_list.json')

    unique_company_pages_dir = os.path.join(home_dir, 'unique_company_pages_dir')

    def __init__(self):
        """
        初始化
        """
        self.requests = WhileRequests()
        self.requests.get(self.home_url)

    def down_load_business_directory_page(self):
        """
        下载企业名录页面
        :return:
        """
        if os.path.exists(self.business_directory_page_path):
            print('business_directory_page already exists!')
            return
        result = self.requests.get(self.business_directory_url, request_times=3, sleep_time=1)
        # result.encoding = 'utf8'
        with open(self.business_directory_page_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)
        print('business_directory_page_path download OK!')

    def parse_business_directory_page(self):
        """
        解析企业名录页面
        :return:
        """
        assert os.path.exists(self.business_directory_page_path), 'business_directory_page is not exists'
        with open(self.business_directory_page_path, 'r', encoding='utf8') as fp:
            content = fp.read()
        selector = etree.HTML(content)
        div_list = selector.xpath('.//article[contains(@class, "sectors-item")]')
        class_list = list()
        i = 1
        for div in div_list:
            class_1_text = self.clean_text(self.data_list_get_first(div.xpath('.//header/h2/a/span/text()')))
            class_2_a_list = div.xpath('.//ul/li/a')
            for item_a in class_2_a_list:
                href = self.clean_text(self.data_list_get_first(item_a.xpath('./@href')))
                class_2_text = self.clean_text(self.data_list_get_first(item_a.xpath('./text()')))
                class_list.append({
                    'class_id': str(i),
                    'class_1': class_1_text,
                    'class_2': class_2_text,
                    'href': href
                })
                i += 1
        with open(self.business_directory_json_path, 'w', encoding='utf8') as fp:
            json.dump(class_list, fp)

        print('企业名录 类别 json 文件保存完成')

    def download_item_class_pages(self):
        """
        下载每个分类页面
        :return:
        """
        self.mkdir(self.company_item_class_page_dir)
        with open(self.business_directory_json_path, 'r', encoding='utf8') as fp:
            class_data = json.load(fp)

        i = 0
        for item_class in class_data:
            i += 1
            print(f'\r{self.draw_data_progress(i, len(class_data))}', end='')

            class_id = item_class['class_id']
            href = item_class['href']
            file_path = os.path.join(self.company_item_class_page_dir, f'{class_id}.html')

            if os.path.exists(file_path):
                continue

            result = self.requests.get(href, request_times=3, sleep_time=1)
            with open(file_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)

        print('\n分类页面全部下载完成')

    def parse_class_pages(self):
        """
        解析分类页面
        :return:
        """
        assert os.path.exists(self.company_item_class_page_dir), 'company_item_class_page_dir is not exists'
        whole_class_dict = dict()
        i = 0
        list_dir = os.listdir(self.company_item_class_page_dir)
        for file_name in list_dir:
            i += 1
            print(f'\r{self.draw_data_progress(i, len(list_dir))}', end='')

            file_path = os.path.join(self.company_item_class_page_dir, file_name)
            class_id = file_name[:-5]
            with open(file_path, 'r', encoding='utf8') as fp:
                content = fp.read()
            selector = etree.HTML(content)
            items = selector.xpath('.//div[@class="domain-columns"]/ul/li')
            activity_list = list()
            for item in items:
                ih_id = self.data_list_get_first(item.xpath('./input/@value'))
                activity = self.data_list_get_first(item.xpath('./a/@href'))
                title = self.data_list_get_first(item.xpath('./a/@title'))
                activity_list.append({
                    'ih_id': ih_id,
                    'title': title,
                    'activity': activity
                })
            whole_class_dict[class_id] = activity_list

            # break
        with open(self.company_class_json_path, 'w', encoding='utf8') as fp:
            json.dump(whole_class_dict, fp)
        print('\n类别子选项解析完毕')

    def merge_business_directory_and_activity(self):
        """
        合并企业分类和分类内活动
        :return:
        """
        with open(self.business_directory_json_path, 'r', encoding='utf8') as fp:
            business_directory_json = json.load(fp)
        with open(self.company_class_json_path, 'r', encoding='utf8') as fp:
            activity_json = json.load(fp)

        whole_class_list = list()
        for class_item in business_directory_json:
            class_item: dict
            class_id = class_item['class_id']
            print(class_id)
            if class_id in activity_json:
                activity_list = activity_json[class_id]
                class_item['activity_list'] = activity_list
                whole_class_list.append(class_item)

        with open(self.whole_class_json_path, 'w', encoding='utf8') as fp:
            json.dump(whole_class_list, fp)
        print('\n分类数据合并完成')

    def download_activity_company_pages(self):
        """
        下载业务活动内容下公司列表
        :return:
        """
        self.mkdir(self.activity_company_list_pages_dir)
        assert os.path.exists(self.whole_class_json_path), 'whole class json is not exists'
        with open(self.whole_class_json_path, 'r', encoding='utf8') as fp:
            whole_class_list = json.load(fp)

        for item_class in whole_class_list:
            class_id = item_class['class_id']
            activity_list = item_class['activity_list']
            for activity in activity_list:
                ih_id = activity['ih_id']
                # print(ih_id)

                dir_name = f'{class_id:>04}_{ih_id}'
                dir_path = os.path.join(self.activity_company_list_pages_dir, dir_name)
                self.mkdir(dir_path)

                if os.path.exists(os.path.join(dir_path, '_over.txt')):
                    print(dir_path)
                    continue

                page_no = 0
                request_url = f'{self.activity_company_list_url}?ih={ih_id}'
                while True:
                    page_no += 1
                    file_name = f'{class_id:>04}_{ih_id}_{page_no:>04}.html'
                    file_path = os.path.join(dir_path, file_name)
                    print(file_path)

                    if os.path.exists(file_path):
                        with open(file_path, 'r', encoding='utf8') as fp:
                            content = fp.read()
                    else:
                        result = self.requests.get(request_url, sleep_time=self.random_float(0, 0.01),
                                                   request_times=200)
                        content = result.text
                        self.create_file(file_path, content)

                    selector = etree.HTML(content)
                    next_page_href_list = selector.xpath('.//a[@title="Next page"]/@href')
                    next_page_url_or_empty_str = self.data_list_get_first(next_page_href_list)
                    if len(next_page_url_or_empty_str) > 0:
                        request_url = next_page_url_or_empty_str
                    else:
                        # 没有下一页
                        file_path = os.path.join(dir_path, '_over.txt')
                        self.create_file(file_path, f'共{page_no}页')
                        break

    def parse_activity_company_list_to_json(self):
        """
        解析业务公司列表
        :return:
        """
        self.mkdir(self.activity_company_list_json_dir)

        for item_dir_name in os.listdir(self.activity_company_list_pages_dir):
            item_dir_path = os.path.join(self.activity_company_list_pages_dir, item_dir_name)
            if not os.path.exists(item_dir_path):
                continue
            item_json_path = os.path.join(self.activity_company_list_json_dir, f'{item_dir_name}.json')
            if os.path.exists(item_json_path):
                continue
            item_activity_company_list = list()
            for item_file_name in os.listdir(item_dir_path):
                if item_file_name.startswith("_"):
                    continue
                item_file_path = os.path.join(item_dir_path, item_file_name)
                if not os.path.exists(item_file_path):
                    continue

                print(item_file_path)
                # file_name = item_file_name[:-5]
                # class_id, ih_id, page_no = file_name.split('_')

                with open(item_file_path, 'r', encoding='utf8') as fp:
                    content = fp.read()

                selector = etree.HTML(content)
                company_li_list = selector.xpath('//li[@class="article-company card card--1dp vcard"]')
                for company_li in company_li_list:
                    company_name = self.data_list_get_first(company_li.xpath('.//div[@class="company-info"]/a/@title'))
                    company_href = self.data_list_get_first(company_li.xpath('.//div[@class="company-info"]/a/@href'))
                    company_country = self.data_list_get_first(
                        company_li.xpath('.//span[@class="company-country"]/text()'))
                    company_city = self.data_list_get_first(
                        company_li.xpath('.//span[@class="company-city"]/text()')).strip('-').strip()

                    company_verified = 1 if len(company_li.xpath('.//div/img[@class="company-verified"]')) > 0 else 0
                    # print(company_name, company_href, company_country, company_city, company_verified)
                    item_activity_company_list.append({
                        'c_name': company_name,
                        'c_href': company_href,
                        'c_country': company_country,
                        'c_city': company_city,
                        'c_verified': company_verified
                    })
                # break

            with open(item_json_path, 'w', encoding='utf8') as fp:
                json.dump(item_activity_company_list, fp)

            # break

    def merge_activity_company_to_unique_company_json(self):
        """
        合并按业务划分的公司数据，为唯一公司json数据
        :return:
        """
        assert os.path.exists(self.activity_company_list_json_dir), 'activity_company_list_json_dir is not exists'

        unique_company_dict = dict()
        count = 0
        for file_name in os.listdir(self.activity_company_list_json_dir):
            file_name_prefix = file_name[:-5]
            file_path = os.path.join(self.activity_company_list_json_dir, file_name)
            print(file_path)
            # class_id, ih_id = file_name_prefix.split('_')
            if not os.path.exists(file_path):
                continue

            with open(file_path, 'r', encoding='utf8') as fp:
                data_list = json.load(fp)

            for data in data_list:
                data: dict
                c_key = hashlib.md5(data['c_href'].encode('utf8')).hexdigest()
                unique_company_dict.setdefault(c_key, {
                    'info': data,
                    'activity': dict()
                })
                unique_company_dict[c_key]['activity'][file_name_prefix] = ''
                count += 1
            # break
        print(f'共计{count}条，去重后共计{len(unique_company_dict)}条')
        print('数据保存中...')
        with open(self.unique_company_list_json_path, 'w', encoding='utf8') as fp:
            json.dump(unique_company_dict, fp)
        print('数据保存成功')

    def gevent_pool_download_company_info_page(self, gevent_pool_size=10):
        """
        多协程下载公司详情页
        :return:
        """
        self.mkdir(self.unique_company_pages_dir)
        with open(self.unique_company_list_json_path, 'r', encoding='utf8') as fp:
            data_dict = json.load(fp)
        data_dict_len = len(data_dict)
        gevent_pool = pool.Pool(gevent_pool_size)
        result_list = gevent_pool.map(self.download_company_info_page,
                                      [(i, data_dict_len, k, v) for i, (k, v) in enumerate(data_dict.items(), start=1)])
        return result_list

    def download_company_info_page(self, data):
        """
        下载公司详情页
        :param data: 相关数据
        :return:
        """
        i, size, c_key, content = data
        print(f'\r{self.draw_data_progress(i, size)}', end='')
        c_href = content['info']['c_href']
        page_file_name = f'{c_key}.html'
        page_file_path = os.path.join(self.unique_company_pages_dir, page_file_name)
        if os.path.exists(page_file_path):
            return
        result = self.requests.get(c_href, sleep_time=self.random_float(0, 0.01), request_times=100)
        self.create_file(page_file_path, result.text)


if __name__ == '__main__':
    eps = EuroPagesSpider()
    # eps.down_load_business_directory_page()
    # eps.parse_business_directory_page()
    # eps.download_item_class_pages()
    # eps.parse_class_pages()
    # eps.merge_business_directory_and_activity()
    # eps.download_activity_company_pages()
    # eps.parse_activity_company_list_to_json()
    # eps.merge_activity_company_to_unique_company_json()
    # 26,133,3321,6204755,1650470
    eps.gevent_pool_download_company_info_page(gevent_pool_size=8)
