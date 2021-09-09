#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/8/13 12:24
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : fis_ru.py
# @Software: PyCharm
import json
import random
from pathlib import Path
from pprint import pprint

import requests
from lxml import etree

from WhileRequests import WhileRequests
from base_spider import BaseSpider


class FisRuSpider(BaseSpider):
    """
    fis.ru 爬虫
    """
    home_url = r'https://fis.ru/'
    home_dir = Path(r'E:\fis_ru')

    home_page_path = home_dir / 'home_page.html'
    class_1_json = home_dir / 'class_1.json'
    class_2_pages_dir = home_dir / 'class_2_pages_dir'
    class_2_json = home_dir / 'class_2.json'
    class_3_pages_dir = home_dir / 'class_3_pages_dir'
    class_3_json = home_dir / 'class_3.json'
    class_3_simple_json = home_dir / 'class_3_simple.json'

    company_list_pages_dir = home_dir / 'company_list_pages_dir'
    company_info_pages_dir = home_dir / 'company_info_pages_dir'
    company_info_json_dir = home_dir / 'company_info_json_dir'

    company_index_json = home_dir / 'company_index.json'
    company_index_id_json = home_dir / 'company_index_id.json'
    company_index_contacts_json = home_dir / 'company_index_contacts.json'

    company_image_dir = home_dir / 'image_dir'
    company_logo_dir = company_image_dir / 'logo_dir'
    company_product_img_dir = company_image_dir / 'product_img_dir'

    company_file_dir = home_dir / 'file_dir'
    company_price_file_dir = company_file_dir / 'price_file_dir'

    company_contacts_pages_dir = home_dir / 'contacts_pages_dir'

    proxies = {
        'http': 'http://61.161.27.29:9999',
        'https': 'https://49.86.9.56:9999'
    }

    def __init__(self, check_home_url=False):
        super(FisRuSpider, self).__init__()
        self.home_dir.mkdir(exist_ok=True)

        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 920)',
            'Connection': 'keep-alive',
            'Accept-Language': 'en-us'
        }

        headers = {
            'Referer': 'https://fis.ru/Avtoservis-avtozapchasti-i-avtotovary',
            'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
            'sec-ch-ua-mobile': '?0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36'
        }

        self.requests = WhileRequests(headers=headers)
        self.session = requests.sessions.Session()
        if check_home_url is True:
            self.requests.get(self.home_url)

    def download_home_page(self):
        """
        下载主页
        :return:
        """
        res = self.requests.get(self.home_url)
        with open(self.home_page_path.as_posix(), 'w', encoding='utf8') as fp:
            fp.write(res.text)

    def parse_home_page(self):
        """
        解析主页
        :return:
        """
        with open(self.home_page_path.as_posix(), 'r', encoding='utf8') as fp:
            context = fp.read()

        selector = etree.HTML(context)
        class_1_text_list = selector.xpath('.//div[@class="item"]/a[@class="menu"]/text()')
        class_1_href_list = selector.xpath('.//div[@class="item"]/a[@class="menu"]/@href')
        # print(class_1_text_list)
        # print(class_1_href_list)
        class_1_list = list()
        for i, (t, u) in enumerate(zip(class_1_text_list, class_1_href_list), start=1):
            temp_dict = {
                'class_1_id': i,
                'class_1_title': t,
                'class_1_url': self.home_url.rstrip('/') + u
            }
            class_1_list.append(temp_dict)
        print(class_1_list)
        with open(self.class_1_json.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(class_1_list, fp)

    def download_class_2_pages(self):
        """
        下载二级类别页面
        :return:
        """
        self.class_2_pages_dir.mkdir(exist_ok=True)
        with open(self.class_1_json.as_posix(), 'r', encoding='utf8') as fp:
            class_1_list = json.load(fp)

        for class_1 in class_1_list:
            class_1_id = class_1['class_1_id']
            class_1_url = class_1['class_1_url']
            class_2_file_path = self.class_2_pages_dir / f'{class_1_id}.html'
            res = self.requests.get(class_1_url)
            with open(class_2_file_path.as_posix(), 'w', encoding='utf8') as fp:
                fp.write(res.text)

    def parse_class_2_pages(self):
        """
        解析类别二页面
        :return:
        """
        with open(self.class_1_json.as_posix(), 'r', encoding='utf8') as fp:
            class_1_list = json.load(fp)
        class_1_dict = dict()
        for i in class_1_list:
            class_1_dict[str(i['class_1_id'])] = {
                'class_1_title': i['class_1_title'],
                'class_1_url': i['class_1_url']
            }

        class_2_list = list()
        for file_path in self.class_2_pages_dir.iterdir():
            print(file_path)
            # file_name = file_path.name
            class_1_id = file_path.stem
            with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)
            class_2_href_list = selector.xpath('.//div[@class="catalog"]/div[@class="list"]/div[@class="item"]/a/@href')
            class_2_text_list = selector.xpath('.//div[@class="catalog"]/div[@class="list"]/div[@class="item"]/a/text()')
            # print(class_2_href_list)
            # print(class_2_text_list)

            for href, title in zip(class_2_href_list, class_2_text_list):
                href = self.home_url.rstrip('/') + href
                print(href, title)
                class_2_list.append({
                    'class_1_id': class_1_id,
                    'class_1_title': class_1_dict[class_1_id]['class_1_title'],
                    'class_1_url': class_1_dict[class_1_id]['class_1_url'],
                    'class_2_id': str(len(class_2_list) + 1),
                    'class_2_title': title,
                    'class_2_url': href
                })

            # break
        with open(self.class_2_json.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(class_2_list, fp)

    def download_class_3_pages(self):
        """
        下载三级类别页面
        :return:
        """
        self.class_3_pages_dir.mkdir(exist_ok=True)
        with open(self.class_2_json.as_posix(), 'r', encoding='utf8') as fp:
            class_2_list = json.load(fp)

        for class_2 in class_2_list:
            class_1_id = class_2['class_1_id']
            class_2_id = class_2['class_2_id']
            class_2_url = class_2['class_2_url']
            class_3_file_path = self.class_3_pages_dir / f'{class_1_id}_{class_2_id}.html'
            if class_3_file_path.exists():
                continue
            print(class_2_url)
            res = self.requests.get(class_2_url, sleep_time=random.random() * 4 + 0.5, timeout=5, proxies=None)
            with open(class_3_file_path.as_posix(), 'w', encoding='utf8') as fp:
                fp.write(res.text)

    def parse_class_3_pages(self):
        """
        解析类别三页面
        :return:
        """
        with open(self.class_2_json.as_posix(), 'r', encoding='utf8') as fp:
            class_2_list = json.load(fp)
        class_2_dict = dict()
        for i in class_2_list:
            class_2_dict[i['class_2_id']] = i

        class_3_list = list()
        for file_path in self.class_3_pages_dir.iterdir():
            # print(file_path)
            stem = file_path.stem
            class_1_id, class_2_id = stem.split('_', 1)
            print(class_1_id, class_2_id)

            with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)
            class_3_href_list = selector.xpath('.//div[@class="list"]//a/@href')
            class_3_text_list = selector.xpath('.//div[@class="list"]//a/text()')
            print(class_3_href_list)
            print(class_3_text_list)

            for href, title in zip(class_3_href_list, class_3_text_list):
                href = self.home_url.rstrip('/') + href
                print(href, title)
                class_3_list.append(dict(**(class_2_dict[class_2_id]), **{
                    'class_3_id': str(len(class_3_list) + 1),
                    'class_3_title': title,
                    'class_3_href': href
                }))

            # break

        with open(self.class_3_json.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(class_3_list, fp)

        print(len(class_3_list))
        class_3_simple_list = list()
        for i in class_3_list:
            print(i)
            class_1_id = i['class_1_id']
            class_2_id = i['class_2_id']
            class_3_id = i['class_3_id']
            class_3_href = i['class_3_href']
            class_1_2_3_id = f'{class_1_id}_{class_2_id}_{class_3_id}'
            class_3_simple_list.append({
                'class_1_2_3_id': class_1_2_3_id,
                'class_3_href': class_3_href
            })
        with open(self.class_3_simple_json.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(class_3_simple_list, fp)

    def download_company_pages_list(self):
        """
        下载公司列表
        :return:
        """
        self.company_list_pages_dir.mkdir(exist_ok=True)
        with open(self.class_3_simple_json.as_posix(), 'r', encoding='utf8') as fp:
            class_3_simple_list = json.load(fp)

        for class_3_simple in class_3_simple_list:
            # print(class_3_simple)
            page_no = 1
            class_1_2_3_id = class_3_simple['class_1_2_3_id']

            while True:
                class_3_href = class_3_simple['class_3_href'] + f'?providers=list&Page={page_no}'
                file_path = self.company_list_pages_dir / f'{class_1_2_3_id}-{page_no}.html'
                if file_path.exists():
                    print(file_path)
                    with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                        file_content = fp.read()
                else:
                    print(class_3_href)
                    res = self.requests.get(class_3_href, sleep_time=random.random() * 2 + 0.5, timeout=5)
                    file_content = res.text

                    with open(file_path.as_posix(), 'w', encoding='utf8') as fp:
                        fp.write(file_content)

                selector = etree.HTML(file_content)
                page_i_list = selector.xpath('.//div[@class="pagination"]/p/a/i')
                if page_no == 1:
                    if len(page_i_list) == 0:
                        # 第一页无上下页
                        break
                elif page_no > 1:
                    if len(page_i_list) < 2:
                        # 不是第一页，且没有同时有上下页，即最后一页
                        break
                page_no += 1

    def parse_company_pages_list(self):
        """
        解析公司列表页
        :return:
        """
        company_index_dict = dict()
        dp = self.DataProgress()
        file_list = list(self.company_list_pages_dir.iterdir())
        for i, file_path in enumerate(file_list, start=1):
            # print(i, file_path)
            dp.print_data_progress(i, len(file_list))
            class_mark = file_path.stem.split('-', 1)[0]
            with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)
            company_href_list = selector.xpath('.//div[@class="companyName"]/h3/a/@href')
            company_name_list = selector.xpath('.//div[@class="companyName"]/h3/a/text()')

            for name, href in zip(company_name_list, company_href_list):
                company_index_dict.setdefault(href, {
                    'company_name': name,
                    'class_mark': list()
                })['class_mark'].append(class_mark)

            # break
        print(len(company_index_dict))

        with open(self.company_index_json.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(company_index_dict, fp)

    def build_company_index_id_json(self):
        """
        构建公司索引id的json文件
        :return:
        """
        with open(self.company_index_json.as_posix(), 'r', encoding='utf8') as fp:
            company_index_dict = json.load(fp)

        company_index_list = list()
        company_index_dict: dict
        no_auto = 1
        for href, v in company_index_dict.items():
            v['href'] = href
            v['no'] = no_auto
            company_index_list.append(v)
            no_auto += 1

        with open(self.company_index_id_json.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(company_index_list, fp)

    def download_company_info_pages(self):
        """
        下载公司详情页
        :return:
        """
        self.company_info_pages_dir.mkdir(exist_ok=True)
        with open(self.company_index_id_json.as_posix(), 'r', encoding='utf8') as fp:
            company_index_list = json.load(fp)

        for i, item in enumerate(company_index_list, start=1):
            # print(i, item)
            href = item['href']
            no = item['no']
            file_name = f'{no}.html'
            file_path = self.company_info_pages_dir / file_name
            if file_path.exists():
                print(file_path)
                continue

            print(href)
            res = self.requests.get(href, sleep_time=random.random() * 1.8 + 0.6, timeout=8, request_times=5)
            if res is not None:
                with open(file_path.as_posix(), 'w', encoding='utf8') as fp:
                    fp.write(res.text)

            # break

    def parse_company_info_pages(self):
        """
        解析公司详情页
        :return:
        """
        self.company_info_json_dir.mkdir(exist_ok=True)
        dp = self.DataProgress()
        file_list = list(self.company_info_pages_dir.iterdir())
        for i, file_path in enumerate(file_list, start=1):
            dp.print_data_progress(i, len(file_list))
            # file_path = Path(r'./temp.html')

            company_info_dict = dict()
            no = file_path.stem
            # print(i, file_path, no)

            json_path = self.company_info_json_dir / f'{no}.json'
            if json_path.exists():
                continue

            company_info_dict['no'] = no

            with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                content = fp.read()
            # content = self.requests.get('https://soyuzdetaley.ru/').text

            selector = etree.HTML(content)
            url_1 = self.data_list_get_first(selector.xpath('.//form[@id="search-form"]/@action'))
            url_2 = self.data_list_get_first(selector.xpath('.//meta[@property="og:url"]/@content'))

            home_page_type = 'normal'

            if url_2:
                # 自主网址
                home_page_type = 'self'

            company_info_dict['home_page_type'] = home_page_type
            # print(home_page_type)
            if home_page_type == 'normal':
                url = url_1
                # print(url)
                company_info_dict['url'] = url
                company_desc_div = self.data_list_get_first(selector.xpath('.//div[@id="center"]/div[@class="right"]/div[@class="block"]/div[@class="content"]'), default=None)
                if company_desc_div is not None:
                    company_desc = company_desc_div.xpath('string(.)')
                    company_desc = self.clean_text(company_desc).rstrip('Подробно о компании →')
                else:
                    company_desc = ''
                company_info_dict['company_desc'] = company_desc
                # print(company_desc)

                address = ''
                web = ''
                email = ''
                tel = ''
                contacts_div_list = selector.xpath('.//div[@id="contacts"]/div[@class="field"]')
                for contacts_div in contacts_div_list:
                    contacts_field = self.data_list_get_first(contacts_div.xpath('./b/text()'))
                    if contacts_field == 'Адрес:':
                        # 地址
                        address = self.data_list_get_first(contacts_div.xpath('./text()'))
                    elif contacts_field == 'Сайт:':
                        # 网址
                        web = self.data_list_get_first(contacts_div.xpath('./text()'))
                    elif contacts_field == 'Эл. почта:':
                        # 邮箱
                        email = self.data_list_get_first(contacts_div.xpath('./text()'))
                    else:
                        phone_list = contacts_div.xpath('.//span[@class="companyPhones"]/text()')
                        tel = ','.join(phone_list)
                company_info_dict['address'] = self.clean_text(address)
                company_info_dict['web'] = self.clean_text(web)
                company_info_dict['email'] = self.clean_text(email)
                company_info_dict['tel'] = self.clean_text(tel)
                # print(address)
                # print(web)
                # print(email)
                # print(phone_list)

                contacts_info_list = list()
                contacts_one_div_list = selector.xpath('.//div[@id="contacts"]/div[@class="contacts-one"]')
                for contacts_one_div in contacts_one_div_list:
                    # print(contacts_one_div)
                    contact_headimg_url = self.data_list_get_first(contacts_one_div.xpath('.//div[@class="image"]/div/a/@href'))
                    if contact_headimg_url:
                        contact_headimg_url = url + contact_headimg_url
                    # print(contact_headimg_url)
                    contact_name = self.data_list_get_first(contacts_one_div.xpath('.//p[@class="name"]/text()'))
                    # print(contact_name)
                    contact_status = self.data_list_get_first(contacts_one_div.xpath('.//p[@class="status"]/text()'))
                    # print(contact_status)

                    contact_phone_list = contacts_one_div.xpath('.//span[@class="companyPhones"]/text()')
                    # print(contact_phone_list)
                    contacts_info_list.append({
                        'headimg_url': contact_headimg_url,
                        'name': contact_name,
                        'status': contact_status,
                        'tel': ','.join(contact_phone_list)
                    })
                company_info_dict['contacts_info_list'] = contacts_info_list

                price_file_date = ''
                price_file_name = ''
                price_file_url = ''
                price_div = self.data_list_get_first(selector.xpath('.//div[@class="price"]'), default=None)
                if price_div is not None:
                    price_file_date = self.data_list_get_first(price_div.xpath('.//p[@class="date"]/text()'))
                    price_file_name = self.data_list_get_first(price_div.xpath('.//img/@alt'))
                    price_file_url = url + self.data_list_get_first(price_div.xpath('.//a/@href'))

                price_file_suffix = self.get_url_suffix(price_file_url, default='.xls')
                company_info_dict['price_file_date'] = price_file_date
                company_info_dict['price_file_name'] = price_file_name
                company_info_dict['price_file_url'] = price_file_url
                price_file_path = self.company_price_file_dir / f'{no}{price_file_suffix}' if price_file_url else ''
                company_info_dict['price_file_path'] = price_file_path.as_posix() if price_file_path else ''
                # print(price_file_date)
                # print(price_file_name)
                # print(price_file_url)

                logo_url = self.data_list_get_first(selector.xpath('.//div[@class="logo"]/a/img/@src'))
                if logo_url:
                    logo_url = url + logo_url
                logo_suffix = self.get_url_suffix(logo_url)
                company_info_dict['logo_url'] = logo_url
                logo_path = self.company_logo_dir / f'{no}{logo_suffix}' if logo_url else ''
                company_info_dict['logo_path'] = logo_path.as_posix() if logo_path else ''

                product_img_url_list = selector.xpath('.//li[@class="b-goods__item"]//img[@class="b-goods__img"]/@src')
                product_title_list = selector.xpath('.//li[@class="b-goods__item"]/p[@class="b-goods__title"]/@title')
                product_price_list = selector.xpath('.//li[@class="b-goods__item"]/p[@class="b-goods__price"]/nobr/text()')
                # print(product_img_url_list)
                # print(product_title_list)
                # print(product_price_list)

                product_list = list()
                for _i, (product_img_url, product_title, product_price) in enumerate(zip(product_img_url_list, product_title_list, product_price_list), start=1):
                    product_img_suffix = self.get_url_suffix(product_img_url)
                    product_img_name = f'{no}_{_i}{product_img_suffix}'
                    product_img_path = self.company_product_img_dir / product_img_name
                    product_list.append({
                        'product_img_url': product_img_url,
                        'product_name': product_title,
                        'product_img_path': product_img_path.as_posix(),
                        'product_price': product_price
                    })
                company_info_dict['product_list'] = product_list
            else:
                # 自主首页
                url = url_2
                company_info_dict['url'] = url

                company_desc_div = self.data_list_get_first(selector.xpath(
                    './/div[@class="about__txt"]'), default=None)
                if company_desc_div is not None:
                    company_desc = company_desc_div.xpath('string(.)')
                    company_desc = self.clean_text(company_desc).rstrip('Узнайте о нас больше')
                else:
                    company_desc = ''
                company_info_dict['company_desc'] = company_desc

                tel = ''

                address = self.data_list_get_first(selector.xpath('.//p[@class="company-text contact-info__address"]/text()'))
                web = self.data_list_get_first(selector.xpath('.//div[@class="contact-info"]/a[@class="company-text blue-link"]/@href'))
                if web:
                    if web.startswith('//'):
                        web = 'http:' + web
                else:
                    web = ''
                email = ''
                tel = ','.join(selector.xpath('.//div[@class="contact-info"]/p[@class="company-text"]/a[@class="show-phone-number"]/span[@class="phone-number"]/text()'))

                company_info_dict['address'] = self.clean_text(address)
                company_info_dict['web'] = self.clean_text(web)
                company_info_dict['email'] = self.clean_text(email)
                company_info_dict['tel'] = self.clean_text(tel)

                contacts_info_list = list()
                contacts_one_div_list = selector.xpath('.//div[@class="contact-person clearfix"]')
                for contacts_one_div in contacts_one_div_list:
                    # print(contacts_one_div)
                    contact_headimg_url = self.data_list_get_first(
                        contacts_one_div.xpath('.//div[@class="contact-person__photo"]//img[@class="hoverable"]/@src'))
                    if contact_headimg_url:
                        contact_headimg_url = url + contact_headimg_url
                    # print(contact_headimg_url)
                    contact_name = self.data_list_get_first(contacts_one_div.xpath('.//div[@class="contact-person__text"]/p[@class="company-text"]/text()'))
                    # print(contact_name)
                    contact_status = self.data_list_get_first(contacts_one_div.xpath('.//div[@class="contact-person__text"]/p[@class="company-text smoky-text"]/text()'))
                    # print(contact_status)

                    contact_phone_list = contacts_one_div.xpath('.//a[@class="show-phone-number"]/@data-tel')
                    # print(contact_phone_list)
                    contacts_info_list.append({
                        'headimg_url': contact_headimg_url,
                        'name': contact_name,
                        'status': contact_status,
                        'tel': ','.join(contact_phone_list)
                    })
                company_info_dict['contacts_info_list'] = contacts_info_list

                price_file_date = ''
                price_file_name = ''
                price_file_url = ''
                price_div = self.data_list_get_first(selector.xpath('.//div[@class="price"]'), default=None)
                if price_div is not None:
                    price_file_date = self.data_list_get_first(price_div.xpath('.//p[@class="date"]/text()'))
                    price_file_name = self.data_list_get_first(price_div.xpath('.//img/@alt'))
                    price_file_url = url + self.data_list_get_first(price_div.xpath('.//a/@href'))

                price_file_suffix = self.get_url_suffix(price_file_url, default='.xls')
                company_info_dict['price_file_date'] = price_file_date
                company_info_dict['price_file_name'] = price_file_name
                company_info_dict['price_file_url'] = price_file_url
                price_file_path = self.company_price_file_dir / f'{no}{price_file_suffix}' if price_file_url else ''
                company_info_dict['price_file_path'] = price_file_path.as_posix() if price_file_path else ''
                # print(price_file_date)
                # print(price_file_name)
                # print(price_file_url)

                logo_url = self.data_list_get_first(selector.xpath('.//div[@class="logo"]/a/img/@src'))
                if logo_url:
                    logo_url = url + logo_url
                logo_suffix = self.get_url_suffix(logo_url)
                company_info_dict['logo_url'] = logo_url
                logo_path = self.company_logo_dir / f'{no}{logo_suffix}' if logo_url else ''
                company_info_dict['logo_path'] = logo_path.as_posix() if logo_path else ''

                # print(selector.xpath('.//div[contains(@class, "b-stuff__item")]'))

                product_img_url_list = selector.xpath('.//div[@class="r-span3 b-stuff__item"]//img[@itemprop="image"]/@src')
                product_title_list = selector.xpath('.//div[@class="r-span3 b-stuff__item"]//img[@itemprop="image"]/@alt')
                product_price_list = selector.xpath(
                    './/div[@class="r-span3 b-stuff__item"]//p[@class="company-text b-stuff__price"]')
                # print(product_img_url_list)
                # print(product_title_list)
                # print(product_price_list)

                product_list = list()
                for _i, (product_img_url, product_title, product_price) in enumerate(
                        zip(product_img_url_list, product_title_list, product_price_list), start=1):
                    product_img_suffix = self.get_url_suffix(product_img_url)
                    product_img_name = f'{no}_{_i}{product_img_suffix}'
                    product_img_path = self.company_product_img_dir / product_img_name
                    product_list.append({
                        'product_img_url': product_img_url,
                        'product_name': product_title,
                        'product_img_path': product_img_path.as_posix(),
                        'product_price': product_price.xpath('string(.)')
                    })
                company_info_dict['product_list'] = product_list

            # pprint(company_info_dict)
            with open(json_path.as_posix(), 'w', encoding='utf8') as fp:
                json.dump(company_info_dict, fp)

            # break

    def build_company_info_download_files_json(self):
        """
        构建公司详情下载文件json
        :return:
        """
        file_list = list(self.company_info_json_dir.iterdir())
        for i, file_path in enumerate(file_list, start=1):
            print(i, file_path)
            no = file_path.stem


    def create_company_index_contacts_json(self):
        """
        创建公司索引联系方式json
        :return:
        """
        new_data_list = list()
        with open(self.company_index_id_json.as_posix(), 'r', encoding='utf8') as fp:
            data_list = json.load(fp)

        # print(data_list)
        for item in data_list:
            # print(item)
            company_contacts_url = item['href'] + '/contacts'
            item['contacts_url'] = company_contacts_url
            new_data_list.append(item)

        with open(self.company_index_contacts_json.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(new_data_list, fp)

    def download_company_contacts_pages(self):
        """
        下载公司联系方式页面
        :return:
        """
        self.company_contacts_pages_dir.mkdir(exist_ok=True)
        with open(self.company_index_contacts_json.as_posix(), 'r', encoding='utf8') as fp:
            data_list = json.load(fp)

        for item in data_list:
            file_name = f"{item['no']}.html"
            file_path = self.company_contacts_pages_dir / file_name
            contacts_url = item['contacts_url']
            print(contacts_url)
            res = self.requests.get(contacts_url, sleep_time=random.random() * 1.8 + 0.6, timeout=8, request_times=5)
            if res is not None:
                with open(file_path.as_posix(), 'w', encoding='utf8') as fp:
                    fp.write(res.text)


if __name__ == '__main__':
    frs = FisRuSpider(check_home_url=False)
    # frs.download_home_page()
    # frs.parse_home_page()
    # frs.download_class_2_pages()
    # frs.parse_class_2_pages()
    # frs.download_class_3_pages()
    # frs.parse_class_3_pages()
    # frs.download_company_pages_list()
    # frs.parse_company_pages_list()
    # frs.build_company_index_id_json()
    # frs.download_company_info_pages()
    # frs.parse_company_info_pages()
    # res = frs.requests.get('https://zipparts.fis.ru/')
    # with open('./temp.html', 'w', encoding='utf8') as fp:
    #     fp.write(res.text)
    # frs.build_company_info_download_files_json()
    # frs.create_company_index_contacts_json()
    frs.download_company_contacts_pages()
