#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/20 18:42
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_5.py
# @Software: PyCharm

import json
import os
import requests
from bs4 import BeautifulSoup
# import time
# import random
from multiprocessing import Pool

home_url = r'https://foreign.mingluji.com'
countries_url = home_url + r'/Country_Index'

home_data = r'E:\work_all\topease\company_spider_3'
countries_list_dir_path = os.path.join(home_data, r'country_list')
company_desc_list_dir_path = os.path.join(home_data, r'company_desc_list')

company_countries_list_html = os.path.join(home_data, r'company_countries_list.html')
countries_url_list_json_path = os.path.join(home_data, r'countries_url_list.json')
company_url_list_json_path = os.path.join(home_data, r'company_url_list.json')
company_desc_list_json_path = os.path.join(home_data, r'company_desc_list.json')
all_keys_json_path = os.path.join(home_data, r'all_keys_list.json')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}


def download_countries_list():
    """
    下载国家列表页
    :return:
    """
    result = requests.get(countries_url, headers=headers, timeout=5)
    # print(result.text)
    with open(company_countries_list_html, 'w', encoding='utf8') as fp:
        fp.write(result.text)


def int_text_del_douhao_to_int(int_text):
    """
    数字字符串，去中间的逗号，转为纯数字字符串
    :param int_text:
    :return:
    """
    int_text_split_list = str(int_text).split(',')
    return ''.join(int_text_split_list)


def while_requests_get(page_url):
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            result = requests.get(page_url, headers=headers, timeout=5)
            # result = requests.get(page_url, headers=headers, proxies=proxies, timeout=5)
        except Exception as e:
            if while_times < 100:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url)
                continue
            else:
                raise e
        else:
            return result


def parse_countries_list_to_json():
    """
    解析国家列表界面，并持久化为json文件
    :return:
    """
    countries_url_list_json = []

    with open(company_countries_list_html, 'r', encoding='utf8') as fp:
        data_stream = fp.read()

    soup = BeautifulSoup(data_stream, 'html.parser')

    countries_ol_select = soup.select('ol')[0]
    countries_select = countries_ol_select.select('li')

    for item_select in countries_select:
        country_and_buyers_and_pages = item_select.text
        country_and_buyers_and_pages_split_list = country_and_buyers_and_pages.split('-')
        buyers_sum_text = country_and_buyers_and_pages_split_list[1].strip()
        pages_size_text = country_and_buyers_and_pages_split_list[-1].strip()

        buyers_sum_int_text = buyers_sum_text.split(' ')[0]
        pages_size_int_text = pages_size_text.split(' ')[0]

        buyers_sum = int_text_del_douhao_to_int(buyers_sum_int_text)
        pages_size = int_text_del_douhao_to_int(pages_size_int_text)

        # print(buyers_sum, pages_size)

        item_a_select = item_select.select('a')[0]
        country_name = item_a_select.get('title')
        country_url = home_url + item_a_select.get('href')

        temp_dict = {
            'country_name': country_name,
            'country_url': country_url,
            'pages_size': pages_size,
            'buyers_sum': buyers_sum
        }

        countries_url_list_json.append(temp_dict)

    with open(countries_url_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(countries_url_list_json))


def download_country_company_list():
    """
    下载各个国家的公司列表页面
    :return:
    """
    with open(countries_url_list_json_path, 'r', encoding='utf8') as fp:
        countries_url_list_json = json.loads(fp.read())

    for item_dict in countries_url_list_json:
        country_name = item_dict['country_name']
        country_url = item_dict['country_url']
        pages_size = item_dict['pages_size']
        # buyers_sum = item_dict['buyers_sum']

        country_name_path = os.path.join(countries_list_dir_path, country_name)
        if not os.path.exists(country_name_path):
            os.mkdir(country_name_path)

        for page_index in range(int(pages_size) * 2):
            company_list_url = country_url + r'/' + str(page_index)

            company_list_file_name = company_list_url.replace('https://', '').replace('/', '_') + '.html'
            company_list_path = os.path.join(country_name_path, company_list_file_name)

            if os.path.exists(company_list_path):
                continue

            result = while_requests_get(company_list_url)

            with open(company_list_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)

            print(country_name, company_list_path)

            # break


def parse_country_company_list_to_json():
    """
    解析国家目录下的公司列表页
    :return:
    """

    company_url_list_json = []
    company_id = 0
    for item_country_dir_name in os.listdir(countries_list_dir_path):
        item_country_dir_path = os.path.join(countries_list_dir_path, item_country_dir_name)
        for item_file_name in os.listdir(item_country_dir_path):
            item_file_path = os.path.join(item_country_dir_path, item_file_name)

            print(item_country_dir_name, item_file_name)

            if os.path.getsize(item_file_path) < 22 << 10:
                continue

            with open(item_file_path, 'r', encoding='utf8') as fp:
                context = fp.read()

            soup = BeautifulSoup(context, 'html.parser')
            company_list_select = soup.select('table > tr')[1].select('td')[0].select('li')

            for item_company_select in company_list_select:
                try:
                    company_href = home_url + item_company_select.select('a')[0].get('href').strip()
                    company_name = item_company_select.text.strip()

                    company_id += 1

                    temp_dict = {
                        'company_id': str(company_id),
                        'company_name': company_name,
                        'company_country': item_country_dir_name,
                        'company_href': company_href
                    }

                    company_url_list_json.append(temp_dict)
                except Exception as e:
                    print('===============================================')
                    print(e)
                    print('===============================================')
                    continue

    with open(company_url_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_url_list_json))


def read_company_url_list_json():
    """
    读取公司url列表
    :return:
    """
    with open(company_url_list_json_path, 'r', encoding='utf8') as fp:
        context = fp.read()
    return json.loads(context)


def download_company_desc_files(company_desc_dict):
    """
    下载所有公司详情页
    :return:
    """
    company_id = company_desc_dict['company_id']
    # company_name = company_desc_dict['company_name']
    company_country = company_desc_dict['company_country']
    company_href = company_desc_dict['company_href']

    company_desc_file_name = company_country + '^' + company_id + '.html'

    company_country_dir_path = os.path.join(company_desc_list_dir_path, company_country)

    if not os.path.exists(company_country_dir_path):
        os.mkdir(company_country_dir_path)

    company_desc_file_path = os.path.join(company_country_dir_path, company_desc_file_name)

    if not os.path.exists(company_desc_file_path):
        result = while_requests_get(company_href)
        with open(company_desc_file_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)
        print(company_country, company_href, 'save OK')
    else:
        print(company_country, company_id, 'saved')


def multiprocessing_download_company_desc_files(pool_num=50):
    """
    多进程下载所有公司详情页
    :return:
    """
    company_url_list_json = read_company_url_list_json()
    print(len(company_url_list_json))

    pool = Pool(pool_num)
    pool.map(download_company_desc_files, company_url_list_json)
    pool.close()
    pool.join()


def parse_company_desc_files_to_json():
    """
    解析所有的公司详情页并存为json文件
    :return:
    """
    company_desc_list_json = []
    for country_dir_name in os.listdir(company_desc_list_dir_path):
        country_dir_path = os.path.join(company_desc_list_dir_path, country_dir_name)
        for file_name in os.listdir(country_dir_path):
            file_path = os.path.join(country_dir_path, file_name)

            print(country_dir_name, '_____', file_name)

            company_id = str(file_name.split('^')[-1]).split('.')[0]

            with open(file_path, 'r', encoding='utf8') as fp:
                context = fp.read()

            soup = BeautifulSoup(context, 'html.parser')

            company_all_info_select = soup.select('div[itemtype^="http"]')

            if len(company_all_info_select) > 0:
                company_desc_en = ''
                company_desc_cn = ''
                company_desc_select = company_all_info_select[0].select('dl')
                if len(company_desc_select) > 0:
                    company_desc_select_dd = company_desc_select[0].select('dd')
                    if len(company_desc_select_dd) >= 3:
                        company_desc_en = company_desc_select_dd[1].text.strip()
                        company_desc_cn = company_desc_select_dd[2].text.strip()
                        if company_desc_cn.startswith('('):
                            company_desc_cn = company_desc_cn.replace('(', '', 1).strip()
                        if company_desc_cn.endswith(')'):
                            company_desc_cn = company_desc_cn[:-1].strip()
                # print(company_desc_en, company_desc_cn)
                # print()

                company_name = ''
                company_name_select = company_all_info_select[0].select('span[itemprop="name"]')
                if len(company_name_select) > 0:
                    company_name = company_name_select[0].text.strip()
                # print(company_name)
                # print()

                country_or_area_en = ''
                country_or_area_cn = ''
                country_or_area_select = company_all_info_select[0].select('span[itemprop="location"]')
                if len(country_or_area_select) > 0:
                    country_or_area = country_or_area_select[0].text.strip()
                    country_or_area_list = country_or_area.split('(')
                    country_or_area_en = country_or_area_list[0].strip()
                    country_or_area_cn = country_or_area_list[1].replace(')', '').strip()
                # print(country_or_area)
                # print()

                company_introduction = ''
                company_introduction_select = company_all_info_select[0].select('span[itemprop="description"]')
                if len(company_introduction_select) > 0:
                    company_introduction = company_introduction_select[0].text.strip()
                # print(company_introduction)
                # print()

                all_dl_dd_dl_dd_select = company_all_info_select[0].select('dl > dd > dl > dd')
                # for i in all_dl_dd_dl_dd_select:
                #     print(i)

                employee_count = ''
                if len(all_dl_dd_dl_dd_select) > 5:
                    employee_count = all_dl_dd_dl_dd_select[5].text.strip()
                # print(employee_count)
                # print()

                contact_person = ''
                contact_person_select = company_all_info_select[0].select('span[itemprop="employees"]')
                if len(contact_person_select) > 0:
                    contact_person = contact_person_select[0].text.strip()
                # print(contact_person)
                # print()

                company_address = ''
                company_address_select = company_all_info_select[0].select('span[itemprop="address"]')
                if len(company_address_select) > 0:
                    company_address = company_address_select[0].text.strip()
                # print(company_address)
                # print()

                post_code = ''
                if len(all_dl_dd_dl_dd_select) > 8:
                    post_code = all_dl_dd_dl_dd_select[8].text.strip()
                # print(post_code)
                # print()

                company_telephone = ''
                company_telephone_select = company_all_info_select[0].select('span[itemprop="telephone"]')
                if len(company_telephone_select) > 0:
                    company_telephone = company_telephone_select[0].text.strip()
                # print(company_telephone)
                # print()

                company_fax = ''
                company_fax_select = company_all_info_select[0].select('span[itemprop="faxNumber"]')
                if len(company_fax_select) > 0:
                    company_fax = company_fax_select[0].text.strip()
                # print(company_fax)
                # print()

                company_email = ''
                company_email_select = company_all_info_select[0].select('span[itemprop="email"]')
                if len(company_email_select) > 0:
                    company_email = company_email_select[0].text.strip()
                # print(company_email)
                # print()

                company_web = ''
                if len(all_dl_dd_dl_dd_select) > 12:
                    company_web = all_dl_dd_dl_dd_select[12].text.strip()
                # print(company_web)
                # print()

                category = ''
                category_select = company_all_info_select[0].select('dl > dd > dl > dd > a.extiw')
                if len(category_select) > 0:
                    category = category_select[-1].text.strip()
                # print(category)
                # print()

                temp_dict = {
                    'company_id': company_id,
                    'company_desc_en': company_desc_en,
                    'company_desc_cn': company_desc_cn,
                    'company_name': company_name,
                    'country_or_area_en': country_or_area_en,
                    'country_or_area_cn': country_or_area_cn,
                    'company_introduction': company_introduction,
                    'employee_count': employee_count,
                    'contact_person': contact_person,
                    'company_address': company_address,
                    'post_code': post_code,
                    'company_telephone': company_telephone,
                    'company_fax': company_fax,
                    'company_email': company_email,
                    'company_web': company_web,
                    'category': category
                }

                for key, value in temp_dict.items():
                    if value == 'N.A.':
                        temp_dict[key] = ''

                company_desc_list_json.append(temp_dict)

                # print(temp_dict)
            # break
        # break
    with open(company_desc_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_desc_list_json))


def read_company_desc_list_json():
    """
    读取公司详情列表json文件
    :return:
    """
    with open(company_desc_list_json_path, 'r', encoding='utf8') as fp:
        company_desc_list_json = json.loads(fp.read())
    return company_desc_list_json


if __name__ == '__main__':
    # download_countries_list()
    # parse_countries_list_to_json()
    # download_country_company_list()
    # parse_country_company_list_to_json()
    # download_company_desc_files(read_company_url_list_json()[0])
    # multiprocessing_download_company_desc_files()
    parse_company_desc_files_to_json()
