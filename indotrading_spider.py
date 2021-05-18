#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/5/12 19:11
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : indotrading_spider.py
# @Software: PyCharm
import json
import os
import re
import mysql.connector
import requests
from lxml import etree
from lxml.etree import _Element
from lxml.html.clean import Cleaner

import settings
from WhileRequests import WhileRequests
from base_spider import BaseSpider


class IndotradingSpider(BaseSpider):
    """
    indotrading 爬虫
    """
    city_list = ['Jakarta',
                 'Surabaya',
                 'Tangerang',
                 'Bandung',
                 'Bekasi',
                 'Medan',
                 'Semarang',
                 'Bogor',
                 'Yogyakarta',
                 'Depok',
                 'Samarinda',
                 'Bali',
                 'Tanjung Pinang',
                 'Makassar',
                 'Pekan Baru',
                 'Bandar Lampung',
                 'Palembang',
                 'Banda Aceh',
                 'Jambi',
                 'Banjarmasin',
                 'Padang',
                 'Pontianak',
                 'Manado']

    home_url = r'https://en.indotrading.com'

    home_path = r'E:\indotrading'
    city_dir = os.path.join(home_path, 'city_dir')
    company_url_list_json = os.path.join(home_path, 'company_url_list.json')
    company_info_pages_dir = os.path.join(home_path, 'company_info_pages_dir')
    company_info_json_dir = os.path.join(home_path, 'company_info_json_dir')
    all_company_info_json_path = os.path.join(home_path, 'all_company_info.json')

    def __init__(self, check_home_url=False):
        """
        初始化
        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 920)',
            'Connection': 'keep-alive',
            'Accept-Language': 'en-us'
        }
        # self.requests = WhileRequests(headers=headers)
        self.requests = WhileRequests()
        if check_home_url is True:
            self.requests.get(self.home_url)

    def download_company_list_page(self):
        """
        下载公司列表页
        :return:
        """
        self.mkdir(self.city_dir)
        city_url_list = [f'{self.home_url}/{city.lower().replace(" ", "")}/company/' for city in self.city_list]
        for city, city_url in zip(self.city_list, city_url_list):
            city_dir_path = os.path.join(self.city_dir, city)
            self.mkdir(city_dir_path)
            print(city, city_url)
            page_no = 0
            while True:
                page_no += 1
                file_path = os.path.join(city_dir_path, f'{page_no}.html')
                if os.path.exists(file_path):
                    continue
                city_page_url = f'{city_url}{page_no}'
                print(city_page_url)
                result = self.requests.get(city_page_url)
                content = result.text

                selector = etree.HTML(content)
                page_ul_list = selector.xpath('.//ul[@class="d-flex justify-content-center page-num-list w-50"]')
                if len(page_ul_list) == 0:
                    break
                self.create_file(file_path, content)
            # break

    def parse_company_list_page(self):
        """
        解析公司列表页面
        :return:
        """
        all_company_url_list = list()
        for city_name in os.listdir(self.city_dir):
            print(city_name)
            city_path = os.path.join(self.city_dir, city_name)
            for file_name in os.listdir(city_path):
                file_path = os.path.join(city_path, file_name)
                with open(file_path, 'r', encoding='utf8') as fp:
                    content = fp.read()
                selector = etree.HTML(content)
                company_url_list = selector.xpath('.//a[@class="span-bold fs-18 product_title pr-10"]/@href')
                all_company_url_list.extend(company_url_list)
        with open(self.company_url_list_json, 'w', encoding='utf8') as fp:
            json.dump(all_company_url_list, fp)

    def download_all_company_info_page(self):
        """
        下载所有公司详情页
        :return:
        """
        with open(self.company_url_list_json, 'r', encoding='utf8') as fp:
            data = json.load(fp)
        self.mkdir(self.company_info_pages_dir)
        for url in data:
            print(url)
            url: str
            url_company_name = url[url.rfind('/') + 1:]
            page_name = f'{url_company_name}.html'
            page_path = os.path.join(self.company_info_pages_dir, page_name)
            if os.path.exists(page_path):
                continue
            result = self.requests.get(url)
            self.create_file(page_path, result.text)
            print(url, 'OK')

    def parse_all_company_info_page(self):
        """
        解析公司详情页面
        :return:
        """
        self.mkdir(self.company_info_json_dir)
        all_company_list = list()
        for i, file_name in enumerate(os.listdir(self.company_info_pages_dir), start=1):
            current_file_json_name = f'{file_name[:-5]}.json'
            current_file_json_path = os.path.join(self.company_info_json_dir, current_file_json_name)
            if os.path.exists(current_file_json_path):
                with open(current_file_json_path, 'r', encoding='utf8') as json_fp:
                    json_data = json.load(json_fp)
                all_company_list.append(json_data)
                continue

            file_path = os.path.join(self.company_info_pages_dir, file_name)
            if os.path.exists(file_path):
                temp_dict = dict()
                print(i, file_path)
                with open(file_path, 'r', encoding='utf8') as fp:
                    content = fp.read()
                # selector = etree.HTML(content)
                # cleaner = Cleaner(style=True, scripts=True, page_structure=False, safe_attrs_only=False)
                # content = cleaner.clean_html(content)
                selector = etree.HTML(content, parser=etree.HTMLParser(encoding='utf8'))
                company_name = self.data_list_get_first(selector.xpath('.//a[@class="mb-5 text--black text--uppercase"]/h1/text()'))
                # print(company_name)
                temp_dict['company_name'] = company_name

                comapny_tag_div_list = selector.xpath('.//div[@class="ratedkk mrt-5"]/div[@class="d-flex a-center"]/div/div[1]')
                company_tag_list = list()
                for company_tag_div in comapny_tag_div_list:
                    company_tag = company_tag_div.xpath('string()').strip()
                    company_tag: str
                    if company_tag.startswith('Verified Supplier'):
                        company_tag = 'Verified Supplier'
                    company_tag_list.append(company_tag)
                # print(company_tag_list)
                temp_dict['company_tag_list'] = company_tag_list

                contact_div_list = selector.xpath('.//div[@class="col-sm-8 compro-sosmed"]')
                if len(contact_div_list) > 0:
                    contact_div = contact_div_list[0]

                    phone_compid = self.data_list_get_first(contact_div.xpath('./a[@data-eventaction="Phone Click"]/@compid'))
                    # print(phone_compid)
                    phone_result = self.requests.post('https://webapi.indotrading.com/api/WhoViewMe/UpdateCompanyPhoneLeads', data={
                        'CompanyId': phone_compid,
                        'ProductID': ''
                    })
                    phone_dict = phone_result.json()
                    # print(phone_dict)
                    temp_dict['phone_info'] = phone_dict

                    web_a_text = self.data_list_get_first(contact_div.xpath('./a[not(contains(@class, "hidden")) and contains(@href, "http")]/@href'))
                    # print(web_a_text)
                    temp_dict['web'] = web_a_text

                address_text_list = selector.xpath('.//div[@class="d-flex j-sp-between"]/div/div[@class="mt-10"]/text()')
                address_text = address_text_list[0].strip()
                address_city = address_text_list[1].strip()
                # print(address_text)
                # print(address_city)
                temp_dict['address_text'] = address_text
                temp_dict['address_city'] = address_city

                company_info_div_list = selector.xpath('.//div[@class="container company-info"]/div/div[contains(@class, "w-25")]')
                # print(company_info_div_list)

                company_info_list = list()
                for company_info_div in company_info_div_list:
                    company_info_text = company_info_div.xpath('string()')
                    company_info_text: str
                    company_info_text = company_info_text.strip()
                    company_info_k_v = company_info_text.split('\n\n')
                    # print(company_info_k_v)
                    if len(company_info_k_v) == 2:
                        company_info_list.append({
                            'company_info_key': company_info_k_v[0].strip(),
                            'company_info_value': company_info_k_v[1].strip()
                        })
                # print(company_info_list)
                temp_dict['company_info_list'] = company_info_list

                category_div_list = selector.xpath('.//div[@class="pr-10 position-sticky"]/div')
                if len(category_div_list) > 0:
                    error_category_div = category_div_list[0]
                    error_category_div_str = etree.tostring(error_category_div, encoding='utf8').decode('utf8')
                    error_category_div_str: str
                    category_text_list = re.findall(r'href="https://en.indotrading.com/21harapan/.+">(.+)\s<span class="text--red">', error_category_div_str)
                    # print(category_text_list)
                    temp_dict['category_list'] = category_text_list

                product_list = list()
                product_div_list = selector.xpath('.//div[@id="divProduct"]//div[@class="newProd p-relative"]')
                # print(product_div_list)
                for product_div in product_div_list:
                    img_src = self.data_list_get_first(product_div.xpath('.//img[@class="img-fluid fw"]/@data-src'))
                    # print(img_src)
                    title = self.data_list_get_first(product_div.xpath('.//div[@class="fw title"]/a/text()'))
                    # print(title)
                    price = self.data_list_get_first(product_div.xpath('.//div[@class="fw price"]/text()')).strip()
                    # print(price)
                    category = self.data_list_get_first(product_div.xpath('.//div[@class="fw category"]/div[2]/text()')).strip()
                    # print(category)
                    date = self.data_list_get_first(product_div.xpath('.//div[@class="fw date"]/div[2]/text()')).strip()
                    # print(date)
                    min_order = self.data_list_get_first(product_div.xpath('.//div[@class="fw min-order"]/div[2]/text()')).strip()
                    # print(min_order)
                    product_list.append({
                        'img_src': img_src,
                        'title': title,
                        'price': price,
                        'category': category,
                        'date': date,
                        'min_order': min_order
                    })
                temp_dict['product_list'] = product_list

                company_profile_text = self.data_list_get_first(selector.xpath('.//div[@class="mb-20 profile-info"]/div/div/div/text()'))
                # print(company_profile_text)
                temp_dict['company_profile'] = company_profile_text

                all_company_list.append(temp_dict)
                print(temp_dict)

                with open(current_file_json_path, 'w', encoding='utf8') as json_wp:
                    json.dump(temp_dict, json_wp)
            # break

        with open(self.all_company_info_json_path, 'w', encoding='utf8') as fp:
            json.dump(all_company_list, fp)

    def insert_data_db(self):
        """
        数据插入数据库
        :return:
        """
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

        with open(self.all_company_info_json_path, 'r', encoding='utf8') as fp:
            data_list = json.load(fp)

        count = 0
        for item in data_list:
            company_name = item['company_name'].replace("'", "''").strip('\\')
            if company_name == '':
                continue

            company_tag_list = json.dumps(item['company_tag_list']).replace("'", "''").strip('\\')
            phone_info = json.dumps(item['phone_info']).replace("'", "''").strip('\\')
            web = item['web'].replace("'", "''").strip('\\')
            address_text = item['address_text'].replace("'", "''").strip('\\')
            address_city = item['address_city'].replace("'", "''").strip('\\')
            company_info_list = json.dumps(item['company_info_list']).replace("'", "''").strip('\\')
            category_list = json.dumps(item['category_list']).replace("'", "''").strip('\\')
            product_list = json.dumps(item['product_list']).replace("'", "''").strip('\\')
            company_profile = item['company_profile'].replace("'", "''").strip('\\')

            count += 1
            sql_str = ''
            try:
                sql_str = (
                    "insert into indotrading("
                    "company_name, company_tag_list, phone_info, web, address_text, address_city,"
                    "company_info_list, category_list, product_list, company_profile) values("
                    "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')"
                ).format(
                    company_name, company_tag_list, phone_info, web, address_text, address_city,
                    company_info_list, category_list, product_list, company_profile
                )

                cur.execute(sql_str.encode('utf8'))
                conn.commit()
                print(count)
            except Exception as e:
                print(sql_str)
                print(e)

            # break
        conn.close()


if __name__ == '__main__':
    its = IndotradingSpider(check_home_url=False)
    # its.download_company_list_page()
    # its.parse_company_list_page()
    # its.download_all_company_info_page()
    # its.parse_all_company_info_page()
    its.insert_data_db()
