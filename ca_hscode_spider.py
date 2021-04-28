#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/4/26 17:57
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : ca_hscode_spider.py
# @Software: PyCharm
import json
import os

import mysql.connector
import requests
import xlrd
import xlwt
from lxml import etree

import settings
from WhileRequests import WhileRequests
from base_spider import BaseSpider


class CaHsCodeSpider(BaseSpider):
    """
    加拿大hscode爬虫
    """
    home_url = r'https://www.ic.gc.ca'
    url = 'https://www.ic.gc.ca/app/scr/ic/sbms/cid/searchProductResults.html?dy=2019&keyword=950691&searchedKeyword=950691'
    url_hscode = 'https://www.ic.gc.ca/app/scr/ic/sbms/cid/productReport.html?hsCode='

    hscode_excel_file_path = r'C:\Users\topeasecpb\Desktop\new_ca_hscode.xlsx'
    # request = WhileRequests()
    session = requests.sessions.session()

    home_path = r'E:\canada_hscode_data'
    all_data_json_path = os.path.join(home_path, 'all_data.json')

    def __init__(self):
        """
        初始化
        """
        self.hs_code_list = list()
        self.session.get(self.home_url)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
            'Connection': 'keep-alive'
        }

    def load_hscode_data(self):
        """
        加载hscode数据
        :return:
        """
        book = xlrd.open_workbook(self.hscode_excel_file_path)
        # names = book.sheet_names()
        # sheet = book.sheet_by_index(0)
        sheet = book.sheet_by_name('Sheet1')
        # rows = sheet.nrows
        # cols = sheet.ncols
        hs_code_list = sheet.col_values(0)[1:]
        self.hs_code_list = list(set(hs_code_list))

    def request_hs_code_html(self, hs_code):
        """
        请求hs code页面
        :param hs_code:
        :return:
        """
        file_path = os.path.join(self.home_path, f'{hs_code}.xls')

        if os.path.exists(file_path):
            # print(f'{hs_code} file is exist')
            return

        index_url = f'{self.url_hscode}{hs_code}'
        result = self.session.get(index_url, headers=self.headers)

        selector = etree.HTML(result.text)
        a_href_list = selector.xpath('.//div[@class="tabpanels"]/div[@class="ic-panel"]/div/a/@href')
        if len(a_href_list) == 0:
            # print(f'{hs_code} no file')
            return
        a_href = a_href_list[0]

        file_url = f'{self.home_url}{a_href}'
        result = self.session.get(file_url, headers=self.headers)
        # print(file_path)
        with open(file_path, 'wb') as fp:
            fp.write(result.content)

    def download_all_hs_code_file(self):
        """
        下载所有的hscode文件
        :return:
        """
        for i, hs_code in enumerate(self.hs_code_list, start=1):
            print(f'\r{self.draw_data_progress(i, len(self.hs_code_list))}', end='')
            self.request_hs_code_html(hs_code)

    def parse_hs_code_data(self):
        """
        解析hscode相关excel文件数据
        :return:
        """

        data_list = list()
        for file_name in os.listdir(self.home_path):
            hscode = file_name[:-4]
            file_path = os.path.join(self.home_path, file_name)
            if not os.path.exists(file_path):
                continue
            print(file_path)

            try:
                book = xlrd.open_workbook(file_path)
            except xlrd.biffh.XLRDError:
                os.remove(file_path)
                continue
            # names = book.sheet_names()
            # sheet = book.sheet_by_index(0)
            sheet = book.sheet_by_name('Report')
            # rows = sheet.nrows
            # cols = sheet.ncols
            company_name_list = sheet.col_values(0)
            company_name_start_index = -1
            company_name_end_index = -1
            for i, v in enumerate(company_name_list):
                v: str
                if v.strip() == 'Company name':
                    # 包含
                    company_name_start_index = i + 1
                if company_name_start_index != -1 and v.strip() == '':
                    # 包含
                    company_name_end_index = i
                    break
            if company_name_start_index == -1 or company_name_start_index == company_name_end_index > 0:
                continue
            company_name_list = company_name_list[company_name_start_index: company_name_end_index + 1]
            city_list = sheet.col_values(1)[company_name_start_index: company_name_end_index + 1]
            province_list = sheet.col_values(2)[company_name_start_index: company_name_end_index + 1]
            postal_code_list = sheet.col_values(3)[company_name_start_index: company_name_end_index + 1]

            for company_name, city, province, postal in zip(company_name_list, city_list, province_list, postal_code_list):
                data_list.append({
                    'hscode': hscode,
                    'company_name': company_name,
                    'city': city,
                    'province': province,
                    'postal': postal
                })

        with open(self.all_data_json_path, 'w', encoding='utf8') as fp:
            json.dump(data_list, fp)

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

        with open(self.all_data_json_path, 'r', encoding='utf8') as fp:
            data_list = json.load(fp)

        count = 0
        for item in data_list:
            company_name = item['company_name'].replace("'", "''").strip('\\')
            if company_name == '':
                continue

            hscode = item['hscode'].replace("'", "''").strip('\\')
            city = item['city'].replace("'", "''").strip('\\')
            province = item['province'].replace("'", "''").strip('\\')
            postal = item['postal'].replace("'", "''").strip('\\')

            count += 1
            sql_str = ''
            try:
                sql_str = (
                    "insert into canada_hscode("
                    "hscode, company_name, city, province, postal) values("
                    "'{}', '{}', '{}', '{}', '{}')"
                ).format(
                    hscode, company_name, city, province, postal
                )

                cur.execute(sql_str.encode('utf8'))
                conn.commit()
                print(count)
            except Exception as e:
                print(sql_str)
                print(e)

        conn.close()


if __name__ == '__main__':
    chcs = CaHsCodeSpider()
    # chcs.load_hscode_data()
    # chcs.request_hs_code_html('950691')
    # chcs.download_all_hs_code_file()
    # chcs.parse_hs_code_data()
    chcs.insert_data_db()
