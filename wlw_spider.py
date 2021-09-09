#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/7/23 21:37
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : wlw_spider.py
# @Software: PyCharm
import asyncio
import concurrent
import hashlib
import json
import time
from pathlib import Path
from typing import Optional

import aiohttp
import requests
from lxml import etree

import settings
from WhileRequests import WhileRequests
from base_spider import BaseSpider


class WlwSpider(BaseSpider):
    """
    wlw 爬虫
    """
    home_url = r'https://www.wlw.de'
    home_dir = Path(r'E:\wlw')

    company_list_pages_dir = home_dir / 'company_list_pages_dir'
    all_company_url_list_json_path = home_dir / 'all_company_url_list.json'
    all_company_pages_dir = home_dir / 'company_pages_dir'
    all_company_json_dir = home_dir / 'company_json_dir'

    image_dir = home_dir / 'image_dir'
    company_product_img_dir = image_dir / 'product_img_dir'
    company_logo_dir = image_dir / 'logo_dir'

    img_url_and_path_json_path = home_dir / 'img_url_and_path.json'

    def __init__(self, check_home_url):
        super(WlwSpider, self).__init__()
        self.requests = WhileRequests(headers={
            'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 920)',
            'Connection': 'keep-alive',
            'Accept-Language': 'en-us'
        })
        self.session = requests.sessions.Session()

        if check_home_url is True:
            self.requests.get(self.home_url)
        self.mkdir(self.home_dir)

    def download_company_list_pages(self):
        """
        下载公司列表页面
        :return:
        """
        self.company_list_pages_dir.mkdir(exist_ok=True)
        # 680
        max_page = 680
        dp = self.DataProgress()
        for p in range(1, max_page + 1):
            dp.print_data_progress(p, max_page)
            file_name = f'{p}.html'
            file_path = self.company_list_pages_dir / file_name
            if file_path.exists():
                continue
            company_list_url = f'{self.home_url}/en/search/page/{p}?audit=Checked&q=c'
            result = self.requests.get(company_list_url)
            with open(file_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)

    def parse_company_list_pages(self):
        """
        解析公司列表页面数据
        :return:
        """
        all_company_url_list = list()
        for i, file_path in enumerate(self.company_list_pages_dir.iterdir(), start=1):
            print(i, file_path.as_posix())

            with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)
            company_url_list = selector.xpath('.//a[@class="company-title-link"]/@href')
            company_url_list = [f'{self.home_url}{i}' for i in company_url_list]
            all_company_url_list.extend(company_url_list)

        with open(self.all_company_url_list_json_path, 'w', encoding='utf8') as fp:
            json.dump(all_company_url_list, fp)

    def download_all_company_pages(self):
        """
        下载所有公司详情页面
        :return:
        """
        self.session.get(self.home_url)
        self.all_company_pages_dir.mkdir(exist_ok=True)
        with open(self.all_company_url_list_json_path, 'r', encoding='utf8') as fp:
            data = json.load(fp)

        dp = self.DataProgress()
        for i, url in enumerate(data, start=1):
            # print(i, url)
            dp.print_data_progress(i, len(data))
            url: str
            file_name = url.split('/')[-1] + '.html'
            file_path = self.all_company_pages_dir / file_name
            if file_path.exists():
                continue
            # response = self.requests.get(url)
            response = self.session.get(url)
            with open(file_path, 'w', encoding='utf8') as fp:
                fp.write(response.text)

    def parse_company_pages(self):
        """
        解析公司页面
        :return:
        """
        self.company_logo_dir.mkdir(parents=True, exist_ok=True)
        self.company_product_img_dir.mkdir(parents=True, exist_ok=True)
        self.all_company_json_dir.mkdir(exist_ok=True)
        dp = self.DataProgress()
        file_list = list(self.all_company_pages_dir.iterdir())
        for i, file_path in enumerate(file_list, start=1):
            dp.print_data_progress(i, len(file_list))
            file_name = file_path.name
            # print(file_name)
            md5 = hashlib.md5(str(file_name).encode('utf8')).hexdigest()

            json_path = self.all_company_json_dir / f'{md5}.json'
            if json_path.exists():
                continue
            # print(md5)
            # print(i, file_path.as_posix())
            with open(file_path.resolve().as_posix(), 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)
            company_json_str = self.data_list_get_first(
                selector.xpath('.//script[@data-n-head="ssr" and @type="application/ld+json"]/text()'))
            try:
                company_info = json.loads(company_json_str)
            except:
                continue

            categories = selector.xpath('.//div[@class="categories__container"]//div[@class="chip__title"]/text()')
            company_info['categories'] = categories

            product_url_list = selector.xpath('.//div[@class="vis-media__picture"]/img/@ix-src')
            product_img_list = [{
                'url': url,
                'path': (self.company_product_img_dir / f'{md5}_{j}{self.get_url_suffix(url)}').as_posix().replace(
                    self.home_dir.as_posix(), '', 1)
            } for j, url in enumerate(product_url_list)]
            company_info['product_img_list'] = product_img_list

            logo_url = company_info.get('logo')
            if logo_url:
                company_info['logo_path'] = (
                            self.company_logo_dir / f'{md5}{self.get_url_suffix(logo_url)}').as_posix().replace(
                    self.home_dir.as_posix(), '', 1)
            else:
                company_info['logo_path'] = ''

            company_info['md5'] = md5
            company_info['description'] = self.clean_text(company_info.get('description', ''))
            # print(company_info)

            with open(json_path.as_posix(), 'w', encoding='utf8') as fp:
                json.dump(company_info, fp)

            # if i > 10:
            #     break

    def extract_all_img_url_and_path(self):
        """
        抽取所有的图片img的url和path
        :return:
        """
        all_img_url_and_path_list = list()
        file_list = list(self.all_company_json_dir.iterdir())
        dp = self.DataProgress()
        for i, file_path in enumerate(file_list, start=1):
            dp.print_data_progress(i, len(file_list))
            with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                data = json.load(fp)
            logo_url = data.get('logo', '')
            logo_path = data.get('logo_path', '')
            product_img_list = data.get('product_img_list', list())
            all_img_url_and_path_list.append({
                'type': 'logo',
                'url': logo_url,
                'path': logo_path
            })
            for item in product_img_list:
                item['type'] = 'product'
                all_img_url_and_path_list.append(item)

        with open(self.img_url_and_path_json_path.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(all_img_url_and_path_list, fp)

    def gevent_pool_download_img(self, pool_size: Optional[int] = 20):
        """
        多协程下载图片
        :return:
        """
        with open(self.img_url_and_path_json_path.as_posix(), 'r', encoding='utf8') as fp:
            data_list = json.load(fp)

        loop = asyncio.get_event_loop()

        start_p = 0
        if pool_size is None:
            pool_size = len(data_list)
        dp = self.DataProgress()
        while start_p < len(data_list):
            _data_list = data_list[start_p: start_p + pool_size]

            tasks = [loop.create_task(self.download_img(url_path_dict, i + start_p, len(data_list), dp)) for
                     i, url_path_dict in enumerate(_data_list, start=1)]
            wait_coro = asyncio.wait(tasks)
            loop.run_until_complete(wait_coro)
            # for task in tasks:
            #     print(task.result())
            start_p += pool_size
            # time.sleep(0.5)
        loop.close()

    def gevent_pool_download_img_pro(self, pool_size: Optional[int] = 20):
        """
        多协程下载图片
        :return:
        """
        with open(self.img_url_and_path_json_path.as_posix(), 'r', encoding='utf8') as fp:
            data_list = json.load(fp)

        if pool_size is None:
            pool_size = len(data_list)

        loop = asyncio.get_event_loop()

        tasks = [loop.create_task(self.download_img_pro(data_list)) for _ in range(pool_size)]
        wait_coro = asyncio.wait(tasks)
        loop.run_until_complete(wait_coro)
        # for task in tasks:
        #     print(task.result())
        loop.close()

    async def download_img(self, data, i, total, dp):
        """
        异步下载图片
        :param data:
        :param i:
        :param total:
        :param dp:
        :return:
        """
        dp.print_data_progress(i, total)

        img_type = data['type']
        img_url = data['url']
        img_path = Path(self.home_dir.as_posix() + data['path'])
        # print(data['path'])
        if img_path.exists():
            return

        async with aiohttp.ClientSession() as session:
            max_retry = 5
            retry_count = 0
            while retry_count < max_retry:
                try:
                    async with session.get(img_url, timeout=6) as resp:
                        r = await resp.read()
                        with open(img_path.as_posix(), 'wb') as fp:
                            fp.write(r)
                        return
                except:
                    retry_count += 1

        # reponse = self.requests.get(img_url)
        # with open(img_path.as_posix(), 'wb') as fp:
        #     fp.write(reponse.content)

    async def download_img_pro(self, data_list: list):
        """
        异步下载图片
        :param data_list:
        :return:
        """
        while len(data_list) > 0:
            data = data_list.pop()

            img_type = data['type']
            img_url = data['url']
            img_path = Path(self.home_dir.as_posix() + data['path'])
            # print(data['path'])
            if img_path.exists():
                return

            async with aiohttp.ClientSession() as session:
                max_retry = 5
                retry_count = 0
                while retry_count < max_retry:
                    try:
                        async with session.get(img_url, timeout=6) as resp:
                            r = await resp.read()
                            with open(img_path.as_posix(), 'wb') as fp:
                                fp.write(r)
                            return
                    except:
                        retry_count += 1

        # reponse = self.requests.get(img_url)
        # with open(img_path.as_posix(), 'wb') as fp:
        #     fp.write(reponse.content)

    def upload_company_info_to_db(self):
        """
        上传公司信息到数据库
        :return:
        """
        import mysql.connector

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

        error_list = list()
        for i, file_path in enumerate(self.all_company_json_dir.iterdir()):
            print(i, file_path.as_posix())
            with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                data = json.load(fp)
            data: dict
            # print(data)

            logo_href = self.db_str_replace_strip(data.get('logo', ''))
            company_name = self.db_str_replace_strip(data.get('legalName', ''))
            web = self.db_str_replace_strip(data.get('url', ''))
            telephone = self.db_str_replace_strip(data.get('telephone', ''))
            email = self.db_str_replace_strip(data.get('email', '').replace('mailto:', ''))
            description = self.db_str_replace_strip(data.get('description', ''))
            fax = self.db_str_replace_strip(data.get('faxNumber', ''))
            area_served = self.db_str_replace_strip(data.get('areaServed', ''))
            founding_date = self.db_str_replace_strip(data.get('foundingDate', ''))
            address_locality = self.db_str_replace_strip(data.get('address', {}).get('addressLocality', ''))
            address_country = self.db_str_replace_strip(data.get('address', {}).get('addressCountry', ''))
            postal_code = self.db_str_replace_strip(data.get('address', {}).get('postalCode', ''))
            street_address = self.db_str_replace_strip(data.get('address', {}).get('streetAddress', ''))
            employees_min = self.db_str_replace_strip(data.get('numberOfEmployees', {}).get('minValue', ''))
            employees_max = self.db_str_replace_strip(data.get('numberOfEmployees', {}).get('maxValue', ''))
            location_map = self.db_str_replace_strip(data.get('location', {}).get('hasMap', ''))
            employee = self.db_str_replace_strip(json.dumps([{
                'given_name': item.get('givenName', ''),
                'family_name': item.get('familyName', '')
            } for item in data.get('employee', [])]))
            categories = self.db_str_replace_strip(json.dumps(data.get('categories', [])))
            product_img_list = self.db_str_replace_strip(json.dumps(data.get('product_img_list', [])))
            logo_path = self.db_str_replace_strip(data.get('logo_path', ''))
            md5 = self.db_str_replace_strip(data.get('md5', ''))

            # print(logo_href)
            # print(company_name)
            # print(web)
            # print(telephone)
            # print(email)
            # print(description)
            # print(fax)
            # print(area_served)
            # print(founding_date)
            # print(address_locality)
            # print(address_country)
            # print(postal_code)
            # print(street_address)
            # print(employees_min)
            # print(employees_max)
            # print(location_map)
            # print(employee)
            # print(categories)
            # print(product_img_list)
            # print(logo_path)
            # print(md5)

            query_sql = f"select * from wlw where md5='{md5}'"
            cur.execute(query_sql.encode('utf8'))
            one = cur.fetchone()
            conn.commit()

            if one:
                continue

            sql_str = ''
            try:
                sql_str = (
                    "insert into wlw("
                    "logo_href, company_name, web, telephone, email, description, fax, area_served,"
                    "founding_date, address_locality, address_country, postal_code, street_address, employees_min,"
                    "employees_max, location_map, employee, categories, product_img_list, logo_path, md5) values("
                    "'{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}',"
                    " '{}', '{}', '{}', '{}', '{}', '{}')"
                ).format(
                    logo_href, company_name, web, telephone, email, description, fax, area_served,
                    founding_date, address_locality, address_country, postal_code, street_address, employees_min,
                    employees_max, location_map, employee, categories, product_img_list, logo_path, md5
                )
                # print(sql_str.encode('utf8'))
                cur.execute(sql_str.encode('utf8'))
                conn.commit()
            except Exception as e:
                print(sql_str)
                print(e)
                error_list.append(e)

        print(error_list)


if __name__ == '__main__':
    ws = WlwSpider(check_home_url=False)
    # ws.download_company_list_pages()
    # ws.parse_company_list_pages()
    # ws.download_all_company_pages()
    # ws.parse_company_pages()
    # ws.extract_all_img_url_and_path()
    # ws.gevent_pool_download_img(pool_size=100)
    # ws.gevent_pool_download_img_pro(pool_size=20)
    ws.upload_company_info_to_db()
