#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/4/15 13:48
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : europages_spider.py
# @Software: PyCharm
import re
import time
from pprint import pprint
from typing import Union, List, Dict

from gevent import pool, monkey;
import mysql.connector

import settings

monkey.patch_all()

import hashlib
import json
import os
from lxml import etree

from WhileRequests import WhileRequests
from base_spider import BaseSpider, DataProgress


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
    unique_company_phone_number_json_dir = os.path.join(home_dir, 'unique_company_phone_number_json_dir')
    unique_company_phone_args_json_dir = os.path.join(home_dir, 'unique_company_phone_args_json_dir')
    unique_company_phone_args_json_path = os.path.join(home_dir, 'unique_company_phone_args.json')
    unique_company_json_dir = os.path.join(home_dir, 'unique_company_json_dir')
    image_dir = os.path.join(home_dir, 'image_dir')
    image_json_dir = os.path.join(image_dir, 'image_json_dir')
    image_path_json_dir = os.path.join(image_dir, 'image_path_json_dir')
    logo_img_dir = os.path.join(image_dir, 'logo_img_dir')
    activity_img_dir = os.path.join(image_dir, 'activity_img_dir')
    product_img_dir = os.path.join(image_dir, 'product_img_dir')
    unique_company_intact_json_dir = os.path.join(home_dir, 'unique_company_intact_json_dir')
    unique_company_intact_json_part_dir = os.path.join(home_dir, 'unique_company_intact_json_part_dir')

    def __init__(self, check_home_url=False):
        """
        初始化
        """
        super(self.__class__, self).__init__()
        self.requests = WhileRequests(headers={
            'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 920)',
            'Connection': 'keep-alive',
            'Accept-Language': 'en-us'
        })
        if check_home_url is True:
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
            # print(f'\r{self.draw_data_progress(i, len(class_data))}', end='')
            self.data_progress.print_data_progress(i, len(class_data))

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
            self.data_progress.print_data_progress(i, len(list_dir))
            # print(f'\r{self.draw_data_progress(i, len(list_dir))}', end='')

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
        # print(f'\r{self.draw_data_progress(i, size)}', end='')
        self.data_progress.print_data_progress(i, size)
        c_href = content['info']['c_href']
        page_file_name = f'{c_key}.html'
        page_file_path = os.path.join(self.unique_company_pages_dir, page_file_name)
        if os.path.exists(page_file_path):
            return
        result = self.requests.get(c_href, sleep_time=self.random_float(0, 0.01), request_times=100)
        self.create_file(page_file_path, result.text)

    def extract_company_phone_request_args(self):
        """
        抽取公司电话号码请求参数
        :return:
        """
        assert os.path.exists(self.unique_company_pages_dir), 'unique_company_pages_dir is not exists'
        self.mkdir(self.unique_company_phone_args_json_dir)
        phone_request_args_list = list()
        part_size = 10_000
        print('start...')
        for i, file_name in enumerate(os.listdir(self.unique_company_pages_dir), start=1):
            file_path = os.path.join(self.unique_company_pages_dir, file_name)
            print(i, file_path)

            with open(file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)
            phone_onclick = self.data_list_get_first(selector.xpath('.//div[@itemprop="telephone"]/a/@onclick'))
            # print(phone_onclick)

            match_obj = re.match(r"EpGetInfoTel\(event,'(.*)','(.*)','(.*)'\);", phone_onclick, re.M | re.I)

            if match_obj:
                no = match_obj.group(1)
                company_id = match_obj.group(2)
                customer_id = match_obj.group(3)
                # https://www.europages.co.uk/InfosTelecomJson.json?uidsid=00000005393168-714803001&id=1444
                url = f'https://www.europages.co.uk/InfosTelecomJson.json?uidsid={company_id}-{customer_id}&id={no}'
                print(i, url)
                phone_request_args_list.append({
                    # 'no': no,
                    # 'company_id': company_id,
                    # 'customer_id': customer_id,
                    'url': url,
                    'page_name': file_name
                })

                if i % part_size == 0:
                    part_json_file_name = f'{i - part_size + 1}_{i}.json'
                    part_json_file_path = os.path.join(self.unique_company_phone_args_json_dir, part_json_file_name)
                    with open(part_json_file_path, 'w', encoding='utf8') as fp:
                        json.dump(phone_request_args_list, fp)
                    phone_request_args_list.clear()
            else:
                print(i, 'no match')

            # break
        with open(os.path.join(self.unique_company_phone_args_json_dir, 'end.json'), 'w', encoding='utf8') as fp:
            json.dump(phone_request_args_list, fp)

    def merge_extract_company_phone_request_args(self):
        """
        合并抽取的公司联系方式参数
        :return:
        """
        all_company_phone_list = list()
        for json_name in os.listdir(self.unique_company_phone_args_json_dir):
            print(json_name)
            json_path = os.path.join(self.unique_company_phone_args_json_dir, json_name)
            if not os.path.exists(json_path):
                continue
            with open(json_path, 'r', encoding='utf8') as fp:
                data = json.load(fp)
            all_company_phone_list.extend(data)
        with open(self.unique_company_phone_args_json_path, 'w', encoding='utf8') as fp:
            json.dump(all_company_phone_list, fp)

    def download_all_company_phone_number(self, gevent_pool_size=50):
        """
        下载所有公司手机号码
        :return:
        """
        if not os.path.exists(self.unique_company_phone_args_json_path):
            return
        with open(self.unique_company_phone_args_json_path, 'r', encoding='utf8') as fp:
            data = json.load(fp)

        data_len = len(data)
        gevent_pool = pool.Pool(gevent_pool_size)
        result_list = gevent_pool.map(self.download_company_phone_number,
                                      [(i, data_len, item) for i, item in enumerate(data, start=1)])
        return result_list

    def download_company_phone_number(self, data):
        """
        下载公司电话号码
        :param data: i, data_len, item
        :return:
        """
        i, size, item = data
        # print(f'\r{self.draw_data_progress(i, size)}', end='')
        self.data_progress.print_data_progress(i, size)
        url = item['url']
        page_name = item['page_name']
        phone_file_name = f'{page_name[:-5]}.json'
        phone_file_path = os.path.join(self.unique_company_phone_number_json_dir, phone_file_name)
        if os.path.exists(phone_file_path):
            return

        result = self.requests.get(url, timeout=30)
        try:
            phone_json = result.json()
            phone_number = phone_json.get('digits', '').replace(' ', '')
            with open(phone_file_path, 'w', encoding='utf8') as fp:
                json.dump({'phone': phone_number}, fp)
        except:
            pass

    def parse_company_info_pages(self):
        """
        解析公司详情文件
        :return:
        """
        assert os.path.exists(self.unique_company_pages_dir), 'unique_company_pages_dir is not exists'
        self.mkdir(self.unique_company_json_dir)
        file_list = os.listdir(self.unique_company_pages_dir)
        for i, file_name in enumerate(file_list, start=1):
            self.data_progress.print_data_progress(i, len(file_list))
            # if i > 200:
            #     break
            file_md5 = file_name[:-5]

            file_path = os.path.join(self.unique_company_pages_dir, file_name)
            # print(i, file_path)
            json_path = os.path.join(self.unique_company_json_dir, f'{file_md5}.json')
            if os.path.exists(json_path):
                continue

            with open(file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            company_dict = dict()
            selector = etree.HTML(content)

            web_href = self.data_list_get_first(selector.xpath('//li[@class="drop-down__menu-item"]/a[@label="English" and @title="English"]/@href'))
            if web_href == '':
                continue

            company_ids = web_href.rsplit(sep='/', maxsplit=1)[-1].replace('.html', '').split(sep='-', maxsplit=1)
            if len(company_ids) != 2:
                continue

            company_id, user_id = company_ids
            company_dict['company_id'] = company_id
            company_dict['user_id'] = user_id
            company_dict['md5'] = file_md5
            # print(web_href)
            # print(company_ids)

            container_div = self.data_list_get_first(selector.xpath('//div[@class="container"]'), default=None)
            if container_div is None:
                continue

            company_logo_src = self.data_list_get_first(
                container_div.xpath('.//div[@class="company-logo--container"]/div[@class="company-logo"]/img/@src'))
            company_dict['company_logo_src'] = company_logo_src

            company_name = self.data_list_get_first(
                container_div.xpath('.//div[@class="company-baseline"]/h1[@itemprop="name"]/span/text()')
            )
            company_dict['company_name'] = self.clean_text(company_name)

            company_verified = 1 if self.data_list_get_first(
                container_div.xpath(
                    './/div[@class="company-baseline"]/h1[@itemprop="name"]/img[@class="badge-verified"]'),
                default=None
            ) is not None else 0
            company_dict['company_verified'] = company_verified

            company_country = self.data_list_get_first(
                container_div.xpath('.//div[@class="company-baseline"]/h2/span[@class="u-normal"]/text()')
            )
            company_dict['company_country'] = self.clean_text(company_country)

            company_description = self.data_list_get_first(
                container_div.xpath('.//p[@class="company-description"]/text()')
            )
            company_dict['company_description'] = self.clean_text(company_description)

            # company_web = self.data_list_get_first(container_div.xpath(
            #     './/ul[@class="epage-info-image__list"]/li/span[@class="epage-info-image--website"]/a/@href')
            # )
            # company_dict['company_web'] = company_web
            # print(company_web)

            company_social_list = container_div.xpath('.//ul[@class="company-social"]/li/a/@href')
            company_dict['company_social'] = company_social_list

            product_view_list = container_div.xpath(
                './/ul[@class="epage-info-image__list epage-info-image__list--in-lang"]/li[@class="js-clickable-in"]/a/span[@class="epage-info-image"]/img')
            product_catalog_list = list()
            for product_view in product_view_list:
                product_catalogue_name = self.data_list_get_first(product_view.xpath('./@alt'))
                product_catalogue_img_src = self.data_list_get_first(product_view.xpath('./@src'))
                product_catalog_list.append({
                    'name': self.clean_text(product_catalogue_name),
                    'img_src': product_catalogue_img_src
                })
            company_dict['product_catalog_list'] = product_catalog_list
            # print(product_catalog_list)

            key_figure_list = list()
            key_figures = container_div.xpath(
                './/div[@class="page__layout-content--container"]//ul[@class="data-list"]/li')
            for key_figure in key_figures:
                value = self.data_list_get_first(key_figure.xpath('.//div[contains(@class, "data")]/text()'))
                label = self.data_list_get_first(key_figure.xpath('.//div[contains(@class, "label")]/text()'))
                key_figure_list.append({
                    'label': self.clean_text(label),
                    'value': self.clean_text(value)
                })
            company_dict['key_figure_list'] = key_figure_list
            # print(key_figure_list)

            organisation_list = list()
            organisation_li_list = container_div.xpath('.//ul[@class="organisation-list"]/li')
            for organisation_li in organisation_li_list:
                value = self.data_list_get_first(organisation_li.xpath('./span[contains(@class, "data")]/text()'))
                label = self.data_list_get_first(organisation_li.xpath('./span[contains(@class, "label")]/text()'))
                organisation_list.append({
                    'label': self.clean_text(label),
                    'value': self.clean_text(value)
                })
            company_dict['organisation_list'] = organisation_list
            # print(organisation_list)

            incoterms_list = list()
            incoterms_li_list = container_div.xpath('.//ul[@class="incoterms-list"]/li[@class="incoterms-list--item"]')
            for incoterms_li in incoterms_li_list:
                key = self.data_list_get_first(incoterms_li.xpath('./div/span[@class="u-biggest u-bold"]/text()'))
                # key_desc = self.data_list_get_first(incoterms_li.xpath('./div/span[@class="u-small"]/text()'))
                # incoterms_list.append({
                #     'key': key,
                #     'key_desc': key_desc
                # })
                incoterms_list.append(key)
            incoterms = ','.join(incoterms_list)
            company_dict['incoterms'] = incoterms
            # print(incoterms)

            h4_list = container_div.xpath('.//h4[@class="mtl h5-like"]')
            company_dict['payment_methods'] = []
            company_dict['banks'] = []
            for h4 in h4_list:
                h4_text = self.data_list_get_first(h4.xpath('./text()'))
                if h4_text.lower() == 'payment methods':
                    payment_methods = h4.xpath('./following-sibling::ul[1]/li/span/text()')
                    company_dict['payment_methods'] = payment_methods
                if h4_text.lower() == 'banks':
                    banks = h4.xpath('./following-sibling::ul[1]/li/span/text()')
                    company_dict['banks'] = banks

            activity_list = list()
            activity_li_list = container_div.xpath(
                './/div[@class="page__layout-content--container"]//ul[@class="epage-info-image__list" and @itemtype="https://schema.org/ImageObject"]/li[@class="js-clickable-in"]')
            for activity_li in activity_li_list:
                # print(etree.tostring(activity_li))
                img_src = self.data_list_get_first(activity_li.xpath('./span[@class="epage-info-image"]/a/img/@src'))
                name = self.data_list_get_first(activity_li.xpath('./span[@itemprop="name"]/text()'))
                activity_list.append({
                    'img_src': img_src,
                    'name': name
                })
            company_dict['activity_list'] = activity_list
            # print(activity_list)

            keyword_list = container_div.xpath('.//ul[@class="keyword-tag"]/li[@itemprop="itemListElement"]/text()')
            company_dict['keywords'] = ','.join(keyword_list)

            company_address = self.clean_text(self.data_list_get_first(selector.xpath('//dd[@itemprop="addressLocality"]/pre/text()')))
            company_dict['company_address'] = company_address
            # print(company_address)

            company_vat_id = self.clean_text(self.data_list_get_first(selector.xpath('//span[@itemprop="vatID"]/text()')))
            company_dict['vat_id'] = company_vat_id
            # print(company_vat_id)

            company_web = self.clean_text(self.data_list_get_first(selector.xpath('//div[@class="page__layout-sidebar--container-desktop"]//a[@itemprop="url"]/@href')))
            company_dict['company_web'] = company_web
            # print(company_web)

            company_tel = self.clean_text(self.data_list_get_first(selector.xpath('//span[@class="js-num-tel js-hidden"]/text()')))
            company_dict['company_tel'] = company_tel
            # print(company_tel)

            # pprint(company_dict)
            # pprint(company_dict['activity_list'])
            # pprint(company_dict['company_address'])
            # pprint(company_dict['company_country'])
            # pprint(company_dict['company_description'])
            # pprint(company_dict['company_id'])
            # pprint(company_dict['company_logo_src'])
            # pprint(company_dict['company_name'])
            # pprint(company_dict['company_social'])
            # pprint(company_dict['company_tel'])
            # pprint(company_dict['company_verified'])
            # pprint(company_dict['company_web'])
            # pprint(company_dict['incoterms'])
            # pprint(company_dict['key_figure_list'])
            # pprint(company_dict['keywords'])
            # pprint(company_dict['md5'])
            # pprint(company_dict['organisation_list'])
            # pprint(company_dict['product_catalog_list'])
            # pprint(company_dict['user_id'])
            # pprint(company_dict['vat_id'])
            # pprint(company_dict['payment_methods'])
            # pprint(company_dict['banks'])

            with open(json_path, 'w', encoding='utf8') as fp:
                json.dump(company_dict, fp)

    def view_company_info_json(self):
        """
        查看公司详情json
        :return:
        """
        json_list = os.listdir(self.unique_company_json_dir)
        for i, file_name in enumerate(json_list[:1000], start=1):
            file_path = os.path.join(self.unique_company_json_dir, file_name)
            with open(file_path, 'r', encoding='utf8') as fp:
                data = json.load(fp)
            data: dict
            # 'company_logo_src': 'https://www.europages.com/filestore/opt/logo/e3/c8/16273134_21586427.png'
            company_logo_src = data['company_logo_src']
            # 'activity_list': [{'img_src': 'https://www.europages.com/filestore/opt/gallery/7e/a3/18831144_b7d8cd3e.jpg',
            #                     'name': 'bac de rétention'},
            #                    {'img_src': 'https://www.europages.com/filestore/opt/gallery/b0/f9/19180942_da747d2c.jpg',
            #                     'name': 'chariot de rangement'},
            #                    {'img_src': 'https://www.europages.com/filestore/opt/gallery/9e/8a/19180946_205a5076.jpg',
            #                     'name': 'chariot spécifique'}]
            activity_list = data['activity_list']
            # 'product_catalog_list': [{'img_src': 'https://www.europages.com/filestore/vig500/opt/product/a2/77/product_f1b0a1d0.jpg',
            #                            'name': 'Steel Enclosures & Panels & Cabins'},
            #                           {'img_src': 'https://www.europages.com/filestore/vig500/opt/product/cf/9/product_1bc6a5a9.jpg',
            #                            'name': 'HIGH SECURITY STEEL SLIDING GATES'}]
            product_catalog_list = data['product_catalog_list']
            if activity_list or product_catalog_list:
                pprint(data)

    def extract_include_image_data(self):
        """
        抽取包含图片的数据
        :return:
        """
        self.mkdir(self.image_dir)
        self.mkdir(self.image_json_dir)
        json_list = os.listdir(self.unique_company_json_dir)
        for i, file_name in enumerate(json_list, start=1):
            self.data_progress.print_data_progress(i, len(json_list))
            md5 = file_name[:-5]
            image_json_path = os.path.join(self.image_json_dir, f'{md5}.json')
            if os.path.exists(image_json_path):
                continue

            file_path = os.path.join(self.unique_company_json_dir, file_name)
            with open(file_path, 'r', encoding='utf8') as fp:
                data = json.load(fp)
            data: dict
            company_logo_src = data['company_logo_src']
            activity_list = data['activity_list']
            product_catalog_list = data['product_catalog_list']
            if company_logo_src or activity_list or product_catalog_list:
                image_dict = {
                    'md5': md5,
                    'company_logo_src': company_logo_src,
                    'activity_list': activity_list,
                    'product_catalog_list': product_catalog_list
                }
                with open(image_json_path, 'w', encoding='utf8') as fp:
                    json.dump(image_dict, fp)
                # break

    def gevent_pool_download_company_image(self, gevent_pool_size=20):
        """
        多协程下载公司图片
        :param gevent_pool_size:
        :return:
        """
        self.mkdir(self.image_path_json_dir)
        self.mkdir(self.logo_img_dir)
        self.mkdir(self.activity_img_dir)
        self.mkdir(self.product_img_dir)
        file_list = os.listdir(self.image_json_dir)

        gevent_pool = pool.Pool(gevent_pool_size)
        result_list = gevent_pool.map(self.download_company_image,
                                      [(i, file_name) for i, file_name in enumerate(file_list, start=1)])
        return result_list

    def download_company_image(self, args):
        """
        下载公司图片
        :param args
        :return:
        """
        i, file_name = args
        print('---------------------------------------------------------')
        print(i, file_name)
        md5 = file_name[:-5]

        image_path_json_path = os.path.join(self.image_path_json_dir, file_name)
        if os.path.exists(image_path_json_path):
            return
        file_path = os.path.join(self.image_json_dir, file_name)
        with open(file_path, 'r', encoding='utf8') as fp:
            data = json.load(fp)
        # pprint(data)
        # {'activity_list': [
        #     {'img_src': 'https://www.europages.com/filestore/opt/gallery/b2/d1/20964432_21810f7e.jpg',
        #      'name': 'Hochleistungskeramik von Haldenwanger!'},
        #     {'img_src': 'https://www.europages.com/filestore/opt/gallery/4d/6d/20964438_5d2b5fbc.jpg',
        #      'name': 'Keramikrollen - Bei uns eine Vielfalt'},
        #     {'img_src': 'https://www.europages.com/filestore/opt/gallery/1f/6/20964439_32724e0b.jpg',
        #      'name': 'Unsere Vielfalt an Rohren'}],
        #  'company_logo_src': 'https://www.europages.com/filestore/opt/logo/94/93/abc675098c4cfcb9d8925aa038c9495a4dbb6ef8.jpg',
        #  'md5': '0084f9702f97364ff6fa28e9addda802',
        #  'product_catalog_list': [
        #      {'img_src': 'https://www.europages.com/filestore/vig500/opt/product/c1/9c/coatings_27555a7e.jpg',
        #       'name': 'Diamond Like Carbon Coatings'},
        #      {
        #          'img_src': 'https://www.europages.com/filestore/vig500/opt/product/fc/3/diamondshield-coating_e3e0c64e.jpg',
        #          'name': 'DiamondShield Coatings'},
        #      {'img_src': 'https://www.europages.com/filestore/vig500/opt/product/f1/aa/cvd-sic_a5ef5fc6.jpg',
        #       'name': 'CVD Diamond'},
        #      {
        #          'img_src': 'https://www.europages.com/filestore/vig500/opt/product/3/e9/scratch-resistant-glass_a087b0a2.jpg',
        #          'name': 'Scratch Resistant Glass Coatings'}]}

        company_logo_src = data['company_logo_src']
        activity_list = data['activity_list']
        product_catalog_list = data['product_catalog_list']

        company_logo_path = ''
        if company_logo_src:
            try:
                file_suf = self.get_url_suffix(company_logo_src)
                company_logo_name = f'{md5}{file_suf}'
                abs_company_logo_path = os.path.join(self.logo_img_dir, company_logo_name)
                if not os.path.exists(abs_company_logo_path):
                    logo_res = self.requests.get(company_logo_src)
                    with open(abs_company_logo_path, 'wb') as fp:
                        fp.write(logo_res.content)
                company_logo_path = abs_company_logo_path.lstrip(self.home_dir)
            except:
                company_logo_path = ''
        data['company_logo_path'] = company_logo_path

        new_activity_list = list()
        if len(activity_list) > 0:
            for a_i, activity in enumerate(activity_list):
                try:
                    img_src = activity['img_src']
                    file_suf = self.get_url_suffix(img_src)
                    activity_name = f'{md5}_{a_i}{file_suf}'
                    abs_activity_path = os.path.join(self.activity_img_dir, activity_name)
                    if not os.path.exists(abs_activity_path):
                        activity_res = self.requests.get(img_src)
                        with open(abs_activity_path, 'wb') as fp:
                            fp.write(activity_res.content)
                    activity_path = abs_activity_path.lstrip(self.home_dir)
                except:
                    activity_path = ''
                activity['activity_path'] = activity_path
                new_activity_list.append(activity)
        data['activity_list'] = activity_list

        new_product_catalog_list = list()
        if len(product_catalog_list) > 0:
            for p_i, product in enumerate(product_catalog_list):
                try:
                    img_src = product['img_src']
                    file_suf = self.get_url_suffix(img_src)
                    product_name = f'{md5}_{p_i}{file_suf}'
                    abs_product_path = os.path.join(self.product_img_dir, product_name)
                    if not os.path.exists(abs_product_path):
                        product_res = self.requests.get(img_src)
                        with open(abs_product_path, 'wb') as fp:
                            fp.write(product_res.content)
                    product_path = abs_product_path.lstrip(self.home_dir)
                except:
                    product_path = ''
                product['product_path'] = product_path
                new_product_catalog_list.append(product)
        data['product_catalog_list'] = product_catalog_list

        pprint(data)
        with open(image_path_json_path, 'w', encoding='utf8') as fp:
            json.dump(data, fp)

    def merge_company_info_and_img_path_info(self):
        """
        合并公司数据和对应的图片数据
        :return:
        """
        self.mkdir(self.unique_company_intact_json_dir)
        main_json_list = os.listdir(self.unique_company_json_dir)
        # img_json_list = os.listdir(self.image_path_json_dir)
        dp = DataProgress()

        for i, main_json_name in enumerate(main_json_list, start=1):
            # dp.print_data_progress(i, len(main_json_list))
            print(f'{i}/{len(main_json_list)} {main_json_name}')
            intact_json_path = os.path.join(self.unique_company_intact_json_dir, main_json_name)
            if os.path.exists(intact_json_path):
                continue

            main_json_path = os.path.join(self.unique_company_json_dir, main_json_name)
            with open(main_json_path, 'r', encoding='utf8') as fp:
                main_json_data = json.load(fp)
            # print(main_json_data)

            img_json_path = os.path.join(self.image_path_json_dir, main_json_name)
            if os.path.exists(img_json_path):
                with open(img_json_path, 'r', encoding='utf8') as fp:
                    img_json_data = json.load(fp)
                main_json_data['activity_list'] = img_json_data['activity_list']
                main_json_data['product_catalog_list'] = img_json_data['product_catalog_list']
                main_json_data['company_logo_path'] = img_json_data['company_logo_path']
            else:
                main_json_data['company_logo_path'] = ''

            with open(intact_json_path, 'w', encoding='utf8') as fp:
                json.dump(main_json_data, fp)

            # if i > 200:
            #     break

    def merge_all_intact_json_to_one(self):
        """
        合并完整json
        :return:
        """
        self.mkdir(self.unique_company_intact_json_part_dir)

        data_list = list()
        max_size = 10000
        file_list = os.listdir(self.unique_company_json_dir)
        file_list_size = len(file_list)
        dp = DataProgress()
        for i, file_name in enumerate(file_list, start=1):
            dp.print_data_progress(i, file_list_size)
            # if i <= 1630000:
            #     continue

            file_path = os.path.join(self.unique_company_intact_json_dir, file_name)
            with open(file_path, 'r', encoding='utf8') as fp:
                data = json.load(fp)
            data_list.append(data)

            if i % max_size == 0:
                part_file_name = f'{i - max_size + 1}_{i}.json'
                part_file_path = os.path.join(self.unique_company_intact_json_part_dir, part_file_name)
                with open(part_file_path, 'w', encoding='utf8') as fp:
                    json.dump(data_list, fp)
                data_list.clear()
        if len(data_list) != 0:
            part_file_name = f'{file_list_size - file_list_size % max_size + 1}_{file_list_size}.json'
            part_file_path = os.path.join(self.unique_company_intact_json_part_dir, part_file_name)
            with open(part_file_path, 'w', encoding='utf8') as fp:
                json.dump(data_list, fp)

    def insert_data_to_local_db(self):
        """
        插入数据到本地数据库
        :return:
        """
        # conn = mysql.connector.connect(
        #     host='127.0.0.1',
        #     user='root',
        #     passwd='123456',
        #     database='company_db',
        #     auth_plugin='mysql_native_password'
        # )

        conn = mysql.connector.connect(
            host=settings.sp_host,
            user=settings.sp_user,
            passwd=settings.sp_password,
            database=settings.sp_database,
            auth_plugin='mysql_native_password'
        )

        cur = conn.cursor()

        if not cur:
            raise (NameError, "数据库连接失败")
        else:
            print('数据库连接成功')

        # dp = DataProgress()
        file_list = os.listdir(self.unique_company_intact_json_part_dir)
        for i, file_name in enumerate(file_list, start=1):
            # print(f'{i}/{len(file_list)} {file_name}')
            # dp.print_data_progress(i, len(file_list))
            file_path = os.path.join(self.unique_company_intact_json_part_dir, file_name)
            with open(file_path, 'r', encoding='utf8') as fp:
                data_list = json.load(fp)

            for j, data in enumerate(data_list, start=1):
                print(f'{i}/{len(file_list)} {j}')
                company_id = self.db_str_replace_strip(data['company_id'])
                user_id = self.db_str_replace_strip(data['user_id'])
                md5 = self.db_str_replace_strip(data['md5'])
                company_logo_src = self.db_str_replace_strip(data['company_logo_src'])
                company_name = self.db_str_replace_strip(data['company_name'])
                company_verified = self.db_str_replace_strip(data['company_verified'])
                company_country = self.db_str_replace_strip(data['company_country'])
                company_description = self.db_str_replace_strip(data['company_description'])
                company_social = self.db_str_replace_strip(json.dumps(data['company_social']))
                product_catalog_list = self.db_str_replace_strip(json.dumps(self.replace_list_k_v(data['product_catalog_list'], 'product_path', '\\', '/')))
                key_figure_list = self.db_str_replace_strip(json.dumps(data['key_figure_list']))
                organisation_list = self.db_str_replace_strip(json.dumps(data['organisation_list']))
                incoterms = self.db_str_replace_strip(data['incoterms'])
                payment_methods = self.db_str_replace_strip(json.dumps(data['payment_methods']))
                banks = self.db_str_replace_strip(json.dumps(data['banks']))
                activity_list = self.db_str_replace_strip(json.dumps(self.replace_list_k_v(data['activity_list'], 'activity_path', '\\', '/')))
                keywords = self.db_str_replace_strip(data['keywords'])
                company_address = self.db_str_replace_strip(data['company_address'])
                vat_id = self.db_str_replace_strip(data['vat_id'])
                company_web = self.db_str_replace_strip(data['company_web'])
                company_tel = self.db_str_replace_strip(data['company_tel'])
                company_logo_path = self.db_str_replace_strip(data['company_logo_path']).replace('\\', '/')

                query_sql = f"select * from euro_page where md5='{md5}'"
                cur.execute(query_sql.encode('utf8'))
                one = cur.fetchone()
                conn.commit()

                if one:
                    continue

                sql_str = ''
                try:
                    sql_str = (
                        "insert into euro_page("
                        "company_id, user_id, md5, company_logo_src, company_name, company_verified,"
                        "company_country, company_description, company_social, product_catalog_list,"
                        "key_figure_list, organisation_list, incoterms, payment_methods, banks, activity_list,"
                        "keywords, company_address, vat_id, company_web, company_tel, company_logo_path) values("
                        "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',"
                        " '{}', '{}', '{}', '{}', '{}', '{}', '{}')"
                    ).format(
                        company_id, user_id, md5, company_logo_src, company_name, company_verified,
                        company_country, company_description, company_social, product_catalog_list,
                        key_figure_list, organisation_list, incoterms, payment_methods, banks, activity_list,
                        keywords, company_address, vat_id, company_web, company_tel, company_logo_path
                    )
                    # print(sql_str.encode('utf8'))
                    cur.execute(sql_str.encode('utf8'))
                    conn.commit()
                except Exception as e:
                    print(sql_str)
                    print(e)


            # break
        conn.close()

    def replace_list_k_v(self, lst: List[Dict[str, str]], k, old_str, new_str):
        """
        替换列表中字典对应key的value
        :param lst: 列表
        :param k: 键
        :param old_str: 原字符串
        :param new_str: 需替换字符串
        :return:
        """
        new_list = list()
        for i in lst:
            temp_dict = dict()
            for _k, _v in i.items():
                if _k == k:
                    _v = _v.replace(old_str, new_str)
                temp_dict[_k] = _v
            new_list.append(temp_dict)
        return new_list





if __name__ == '__main__':
    eps = EuroPagesSpider(check_home_url=False)
    # eps.down_load_business_directory_page()
    # eps.parse_business_directory_page()
    # eps.download_item_class_pages()
    # eps.parse_class_pages()
    # eps.merge_business_directory_and_activity()
    # eps.download_activity_company_pages()
    # eps.parse_activity_company_list_to_json()
    # eps.merge_activity_company_to_unique_company_json()
    # 26,133,3321,6204755,1650470
    # eps.gevent_pool_download_company_info_page(gevent_pool_size=8)
    # eps.extract_company_phone_request_args()
    # eps.merge_extract_company_phone_request_args()
    # eps.download_all_company_phone_number()
    # eps.parse_company_info_pages()
    # eps.view_company_info_json()
    # eps.extract_include_image_data()
    # eps.gevent_pool_download_company_image(gevent_pool_size=20)
    # eps.merge_company_info_and_img_path_info()
    # eps.merge_all_intact_json_to_one()
    eps.insert_data_to_local_db()
