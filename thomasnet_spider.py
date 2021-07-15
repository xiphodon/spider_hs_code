#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/5/21 2:59
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : thomasnet_spider.py
# @Software: PyCharm
import json
import os
import random
import re
import time

import requests
from gevent import pool, monkey;
from lxml import etree

import settings
from WhileRequests import WhileRequests
from base_spider import BaseSpider, DataProgress
import mysql.connector

monkey.patch_all()


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

    company_info_page_dir = os.path.join(home_path, 'company_info_page_dir')
    company_info_page_json_dir = os.path.join(home_path, 'company_info_page_json_dir')

    image_dir_path = os.path.join(home_path, 'image_dir')
    logo_dir_path = os.path.join(image_dir_path, 'logo_dir')
    product_dir_path = os.path.join(image_dir_path, 'product_dir')
    services_dir_path = os.path.join(image_dir_path, 'services_dir')

    company_info_offered_dir_path = os.path.join(home_path, 'offered_dir')
    img_href_path_json_path = os.path.join(home_path, 'img_href_path.json')

    merge_company_json_dir = os.path.join(home_path, 'merge_company_json_dir')

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
            print(v)
        print(len(data_dict))

    def download_all_company_info_page(self):
        """
        下载所有公司详情页面
        :return:
        """
        self.mkdir(self.company_info_page_dir)
        with open(self.unique_company_list_json_path, 'r', encoding='utf8') as fp:
            data_dict = json.load(fp)

        dp = DataProgress()
        for i, (k, v) in enumerate(data_dict.items(), start=1):
            # print(i, k, v)
            dp.print_data_progress(i, len(data_dict))

            company_id = k
            file_name = f'{company_id}.html'
            file_path = os.path.join(self.company_info_page_dir, file_name)
            if os.path.exists(file_path):
                continue

            url = v['company_href']
            response = self.requests.get(url)
            self.create_file(file_path, response.text)

    def parse_company_info_pages(self):
        """
        解析公司详情页
        :return:
        """
        self.mkdir(self.company_info_page_json_dir)
        dp = DataProgress()
        page_file_list = os.listdir(self.company_info_page_dir)
        for i, file_name in enumerate(page_file_list, start=1):
            # file_name = '10025252.html'
            dp.print_data_progress(i, len(page_file_list))
            # print(i, file_name, '-' * 20)
            company_id = file_name[:-5]

            json_path = os.path.join(self.company_info_page_json_dir, f'{company_id}.json')
            if os.path.exists(json_path):
                continue

            file_path = os.path.join(self.company_info_page_dir, file_name)
            with open(file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)

            nav_div = self.data_list_get_first(selector.xpath('//div[@id="copro_naft" and @class="copro_naft"]'), default=None)
            if nav_div is None:
                continue

            company_name_view = self.data_list_get_first(nav_div.xpath('./div[@class="codetail"]/h1'))
            company_name = self.clean_text(company_name_view.xpath('string(.)')) if company_name_view is not None else ''
            # print(company_name)

            company_web = self.data_list_get_first(nav_div.xpath('.//a[@data-click_origin="Visit Website"]/@href'))
            # print(company_web)

            company_verified = '1' if self.data_list_get_first(nav_div.xpath('.//span[@class="supplier-badge__label"]/text()')) == 'Thomas Verified' else '0'
            # print(company_verified)

            # company_type_view = self.data_list_get_first(nav_div.xpath('.//svg[@xmlns="http://www.w3.org/2000/svg" and @class="icon"]/parent::*'))
            # company_type = self.clean_text(company_type_view.xpath('string(.)')) if company_type_view else ''
            # print(company_type)

            phone = self.data_list_get_first(nav_div.xpath('.//p[@class="phoneline"]/span/text()'))
            # print(phone)

            company_pdm = self.clean_text('\n'.join(selector.xpath('.//div[@id="copro_pdm"]/p/text()')))
            # print(company_pdm)

            company_about = self.clean_text('\n'.join(selector.xpath('.//div[@id="copro_about"]/p/text()')))
            # print(company_about)

            company_detail_views = selector.xpath('.//div[@id="copro_bizdetails"]//div[@class="bizdetail"]')

            company_type = ''
            additional_activities = ''
            key_personnel = ''
            social = ''
            annual_sales = ''
            employees = ''
            year_founded = ''
            for item_view in company_detail_views:
                label = self.data_list_get_first(item_view.xpath('./div[@class="label"]/text()'))
                value = item_view.xpath('./ul/li/text()')

                if label:
                    # print(label, value)

                    if label.startswith('Primary Company Type'):
                        company_type = ';'.join(value)
                    elif label.startswith('Additional Activities'):
                        additional_activities = ';'.join(value)
                    elif label.startswith('Key Personnel'):
                        key_personnel = ';'.join(value)
                    elif label.startswith('Social'):
                        social = ';'.join(item_view.xpath('./ul/li/a/@href'))
                    elif label.startswith('Annual Sales'):
                        annual_sales = ';'.join(value)
                    elif label.startswith('No of Employees'):
                        employees = ';'.join(value)
                    elif label.startswith('Year Founded'):
                        year_founded = ';'.join(value)

            # print(company_type)
            # print(additional_activities)
            # print(key_personnel)
            # print(social)
            # print(annual_sales)
            # print(employees)
            # print(year_founded)

            product_views = selector.xpath('.//div[@class="copro_addl ccp clear prod"]/div[@class="tile match-height2"]')
            product_list = list()
            for product_i, product_view in enumerate(product_views, start=1):
                product_name = self.data_list_get_first(product_view.xpath('./div[@class="headline ccpheadline"]/a/text()'))
                product_img_href = 'https:' + self.data_list_get_first(product_view.xpath('./div[@class="thumb"]//img/@src'))
                product_desc = self.data_list_get_first(product_view.xpath('./div[@class="summary"]/text()'))
                product_img_path = os.path.join(self.product_dir_path, f'{company_id}_{product_i}{self.get_url_suffix(product_img_href)}')
                product_list.append({
                    'product_name': product_name,
                    'product_img_href': product_img_href,
                    'product_desc': product_desc,
                    'product_img_path': product_img_path.replace('\\', '/').lstrip('E:/thomasnet')
                })
            # print(product_list)

            services_views = selector.xpath(
                './/div[@class="copro_addl ccp clear cap"]/div[@class="tile match-height2"]')
            services_list = list()
            for services_i, services_view in enumerate(services_views, start=1):
                services_name = self.data_list_get_first(
                    services_view.xpath('./div[@class="headline ccpheadline"]/a/text()'))
                services_img_href = 'https:' + self.data_list_get_first(
                    services_view.xpath('./div[@class="thumb"]//img/@src'))
                services_desc = self.data_list_get_first(services_view.xpath('./div[@class="summary"]/text()'))
                services_img_path = os.path.join(self.services_dir_path,
                                                f'{company_id}_{services_i}{self.get_url_suffix(services_img_href)}')
                services_list.append({
                    'services_name': services_name,
                    'services_img_href': services_img_href,
                    'services_desc': services_desc,
                    'services_img_path': services_img_path.replace('\\', '/').lstrip('E:/thomasnet')
                })
            # print(services_list)

            certifications_page_href = ''
            menu_views = selector.xpath('.//ul[@id="copro_menu"]/li/a')
            for menu_view in menu_views:
                menu_title = self.data_list_get_first(menu_view.xpath('./text()'))
                menu_href = self.data_list_get_first(menu_view.xpath('./@href'))
                if menu_title.startswith('Certifications'):
                    certifications_page_href = self.home_url + menu_href
            # print(certifications_page_href)

            temp_dict = {
                'company_id': company_id,
                'company_name': company_name,
                'company_web': company_web,
                'company_verified': company_verified,
                'phone': phone,
                'company_pdm': company_pdm,
                'company_about': company_about,
                'company_type': company_type,
                'additional_activities': additional_activities,
                'key_personnel': key_personnel,
                'social': social,
                'annual_sales': annual_sales,
                'employees': employees,
                'year_founded': year_founded,
                'product_list': product_list,
                'services_list': services_list,
                'certifications_page_href': certifications_page_href
            }

            with open(json_path, 'w', encoding='utf8') as fp:
                json.dump(temp_dict, fp)

            # if i > 300:
            #     break

    def download_logo(self):
        """
        下载logo
        :return:
        """
        self.mkdir(self.logo_dir_path)
        with open(self.unique_company_list_json_path, 'r', encoding='utf8') as fp:
            data_dict = json.load(fp)

        count = 0
        for i, (company_id, item) in enumerate(data_dict.items(), start=1):
            # print(i, item)
            logo_href = item['logo_href']
            # print(logo_href)

            if not logo_href:
                continue

            count += 1

            img_suffix = self.get_url_suffix(logo_href)
            logo_name = f'{company_id}_logo{img_suffix}'
            logo_path = os.path.join(self.logo_dir_path, logo_name)

            if os.path.exists(logo_path):
                continue

            logo_res = self.requests.get(logo_href)
            with open(logo_path, 'wb') as fp:
                fp.write(logo_res.content)

            print(logo_path)
            # break

        print(count)

    def parse_product_img(self):
        """
        解析产品图片
        :return:
        """
        self.mkdir(self.product_dir_path)
        self.mkdir(self.services_dir_path)

        json_file_list = os.listdir(self.company_info_page_json_dir)
        img_list = list()
        for i, file_name in enumerate(json_file_list, start=1):
            print(i, file_name)
            file_path = os.path.join(self.company_info_page_json_dir, file_name)
            with open(file_path, 'r', encoding='utf8') as fp:
                company_dict = json.load(fp)
            # print(json.dumps(company_dict))
            product_list = company_dict['product_list']
            services_list = company_dict['services_list']

            for product_item in product_list:
                product_href = product_item['product_img_href']
                product_path = product_item['product_img_path']
                # print(product_href)
                # print(product_path)
                img_list.append({
                    'href': product_href,
                    'path': product_path
                })

            for services_item in services_list:
                services_href = services_item['services_img_href']
                services_path = services_item['services_img_path']
                # print(services_href)
                # print(services_path)
                img_list.append({
                    'href': services_href,
                    'path': services_path
                })

            # print(img_list)

        with open(self.img_href_path_json_path, 'w', encoding='utf8') as fp:
            json.dump(img_list, fp)

    def gevent_pool_download_img(self, gevent_pool_size=20):
        """
        多协程下载商品、服务相关图片
        :return:
        """
        dp = DataProgress()
        with open(self.img_href_path_json_path, 'r', encoding='utf8') as fp:
            data_list = json.load(fp)

        gevent_pool = pool.Pool(gevent_pool_size)
        result_list = gevent_pool.map(self.download_img,
                                      [(i, item, len(data_list), dp) for i, item in enumerate(data_list, start=1)])
        return result_list

    def download_img(self, data):
        """
        下载商品、服务相关图片
        :param data:
        :return:
        """
        i, item, total_len, dp = data

        dp.print_data_progress(i, total_len)
        # print(i, item, total_len)
        href = item['href']
        file_path = item['path']

        img_path = self.home_path + '\\' + file_path.replace('/', '\\')
        if os.path.exists(img_path):
            return

        img_res = self.requests.get(href)
        with open(img_path, 'wb') as fp:
            fp.write(img_res.content)

    def parse_company_info_offered(self):
        """
        解析公司详情中提供的产品和服务
        (废弃)
        :return:
        """
        dp = DataProgress()
        self.mkdir(self.company_info_offered_dir_path)
        page_file_list = os.listdir(self.company_info_page_dir)
        for i, file_name in enumerate(page_file_list, start=1):
            file_name = '10025252.html'
            company_id = file_name[:-5]
            print(i, company_id, file_name)
            offered_json_path = os.path.join(self.company_info_offered_dir_path, f'{company_id}.json')

            if os.path.exists(offered_json_path):
                continue

            company_offered_dict = dict()
            company_offered_dict['company_id'] = company_id
            file_path = os.path.join(self.company_info_page_dir, file_name)
            with open(file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)
            # print(selector)
            products_services_div_list = selector.xpath('.//div[@id="copro_prodserv_cats"]/div[@class="prodserv_group"]')
            print(products_services_div_list)

            all_product_href_list = selector.xpath('.//div[@id="copro_prodserv_cats"]/div[@class="prodserv_group"]/div[@class="all"]/a/@href')
            print(all_product_href_list)
            all_product_href = ''
            for _href in all_product_href_list:
                _href: str
                if 'product_family' in _href:
                    all_product_href = self.home_url + _href
                    break
            print(all_product_href)

            if all_product_href == '':
                # 读取当前产品服务内容
                for products_services_div in products_services_div_list:
                    title = self.clean_text(self.data_list_get_first(products_services_div.xpath('./h3/text()')))
                    product_service_list = products_services_div.xpath('./ul/li/a/text()')
                    print(title, product_service_list)
                    if title != 'All Products / Services':
                        # 当前offered
                        company_offered_dict['company_offered'] = {
                            'title': title,
                            'product_service_list': product_service_list
                        }
                    else:
                        # 所有offered
                        _product_service_list = list()
                        for item in product_service_list:
                            _product_service_list.append({
                                'name': item,
                                'sub_list': []
                            })
                        company_offered_dict['all_products_services'] = _product_service_list
            else:
                # 下载所有产品页
                all_product_page = self.requests.get(all_product_href)
                all_product_page_text = all_product_page.text
                print(all_product_page_text)

                selector_2 = etree.HTML(all_product_page_text)
                company_offered_div_list = selector_2.xpath('.//div[@id="famprodserv"]')
                all_products_services_div_list = selector_2.xpath('.//div[@id="allprodserv"]')

                print(company_offered_div_list)
                print(all_products_services_div_list)

                if len(company_offered_div_list) > 0:
                    company_offered_div = company_offered_div_list[0]
                    title = self.data_list_get_first(company_offered_div.xpath('./h3/text()'))
                    product_service_list = company_offered_div.xpath('.//li/a/text()')
                    # 当前offered
                    company_offered_dict['company_offered'] = {
                        'title': title,
                        'product_service_list': product_service_list
                    }

                if len(all_products_services_div_list) > 0:
                    all_products_services_div = all_products_services_div_list[0]

            print(company_offered_dict)

            break

    def merge_unique_company_info(self):
        """
        合并公司信息
        :return:
        """
        self.mkdir(self.merge_company_json_dir)
        with open(self.unique_company_list_json_path, 'r', encoding='utf8') as fp:
            company_list_dict = json.load(fp)

        json_file_list = os.listdir(self.company_info_page_json_dir)
        dp = DataProgress()
        for i, file_name in enumerate(json_file_list, start=1):
            # print(i, file_name)
            dp.print_data_progress(i, len(json_file_list))
            merge_company_json_path = os.path.join(self.merge_company_json_dir, file_name)
            if os.path.exists(merge_company_json_path):
                continue

            file_path = os.path.join(self.company_info_page_json_dir, file_name)
            with open(file_path, 'r', encoding='utf8') as fp:
                company_info = json.load(fp)

            company_info: dict
            del company_info['company_pdm']
            del company_info['certifications_page_href']

            # print(company_info)
            company_id = company_info['company_id']

            item_company = company_list_dict.get(company_id, None)
            if item_company is None:
                continue

            del item_company['id']

            if item_company['logo_href']:
                logo_name = f"{company_id}_logo{self.get_url_suffix(item_company['logo_href'])}"
                item_company['logo_path'] = os.path.join(self.logo_dir_path, logo_name).lstrip(self.home_path).replace('\\', '/')
            else:
                item_company['logo_path'] = ''

            # print(item_company)

            company_info.update(item_company)

            # print(company_info)

            with open(merge_company_json_path, 'w', encoding='utf8') as fp:
                json.dump(company_info, fp)

            # if i > 10:
            #     break

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

        error_count = 0
        error_list = list()

        dp = DataProgress()
        file_list = os.listdir(self.merge_company_json_dir)
        for i, file_name in enumerate(file_list, start=1):
            # print(f'{i}/{len(file_list)} {file_name}')
            dp.print_data_progress(i, len(file_list))
            file_path = os.path.join(self.merge_company_json_dir, file_name)
            with open(file_path, 'r', encoding='utf8') as fp:
                data = json.load(fp)

            # print(f'{i}/{len(file_list)}')
            company_id = self.db_str_replace_strip(data['company_id'])
            company_name = self.db_str_replace_strip(data['company_name'])
            company_web = self.db_str_replace_strip(data['company_web'])
            company_verified = self.db_str_replace_strip(data['company_verified'])
            phone = self.db_str_replace_strip(data['phone'])
            company_about = self.db_str_replace_strip(data['company_about'])
            company_type = self.db_str_replace_strip(data['company_type'])
            additional_activities = self.db_str_replace_strip(data['additional_activities'])
            key_personnel = self.db_str_replace_strip(data['key_personnel'])
            social = self.db_str_replace_strip(data['social'])
            annual_sales = self.db_str_replace_strip(data['annual_sales'])
            employees = self.db_str_replace_strip(data['employees'])
            year_founded = self.db_str_replace_strip(data['year_founded'])
            product_list = self.db_str_replace_strip(json.dumps(data['product_list']))
            services_list = self.db_str_replace_strip(json.dumps(data['services_list']))
            company_href = self.db_str_replace_strip(data['company_href'])
            logo_href = self.db_str_replace_strip(data['logo_href'])
            class_list = self.db_str_replace_strip(json.dumps(data['class_list']))
            logo_path = self.db_str_replace_strip(data['logo_path'])

            query_sql = f"select * from thomasnet where company_id='{company_id}'"
            cur.execute(query_sql.encode('utf8'))
            one = cur.fetchone()
            conn.commit()

            if one:
                continue

            sql_str = ''
            try:
                sql_str = (
                    "insert into thomasnet("
                    "company_id, company_name, company_web, company_verified, phone, company_about,"
                    "company_type, additional_activities, key_personnel, social, annual_sales,"
                    "employees, year_founded, product_list, services_list, company_href, logo_href,"
                    "class_list, logo_path) values("
                    "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',"
                    " '{}', '{}', '{}', '{}')"
                ).format(
                    company_id, company_name, company_web, company_verified, phone, company_about,
                    company_type, additional_activities, key_personnel, social, annual_sales,
                    employees, year_founded, product_list, services_list, company_href, logo_href,
                    class_list, logo_path
                )
                # print(sql_str.encode('utf8'))
                cur.execute(sql_str.encode('utf8'))
                conn.commit()
            except Exception as e:
                print(sql_str)
                print(e)
                error_count += 1
                error_list.append(e)

            # break
        conn.close()
        print(error_count)
        print(error_list)


if __name__ == '__main__':
    ts = ThomasnetSpider(check_home_url=False)
    # ts.download_home_page()
    # ts.parse_home_page()
    # ts.download_company_list_pages()
    # ts.parse_company_list_page()
    # ts.unique_all_company()
    # ts.unique_all_company_view()
    # ts.download_all_company_info_page()
    # ts.parse_company_info_pages()
    # ts.download_logo()
    # ts.parse_product_img()
    # ts.gevent_pool_download_img(gevent_pool_size=20)
    # ts.parse_company_info_offered()
    # ts.merge_unique_company_info()
    ts.insert_data_to_local_db()
