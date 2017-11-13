#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/10/20 14:27
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_4.py
# @Software: PyCharm

import json
import os
import requests
from bs4 import BeautifulSoup
import time
# import random
from multiprocessing import Pool
import ocr_img_to_str
import re

home_url = r'http://www.listcompany.org'

home_data = r'E:\work_all\topease\company_spider_2'
countries_list_dir_path = os.path.join(home_data, r'country_list')
company_desc_list_dir_path = os.path.join(home_data, r'company_desc_list')
company_desc_phone_list_dir_path = os.path.join(home_data, r'company_desc_phone_list')
phone_img_list_dir_path = os.path.join(home_data, r'phone_img_list')
phone_str_json_list_dir_path = os.path.join(home_data, r'phone_str_json_list')

company_countries_list_html = os.path.join(home_data, r'company_countries_list.html')
countries_url_list_json_path = os.path.join(home_data, r'countries_url_list.json')
company_desc_url_list_json_path = os.path.join(home_data, r'company_desc_url_list.json')
company_desc_list_json_1_path = os.path.join(home_data, r'company_desc_list_1.json')
company_desc_all_keys_json_path = os.path.join(home_data, r'company_desc_all_keys.json')
company_desc_has_phone_url_json_path = os.path.join(home_data, r'company_desc_has_phone_url.json')
company_desc_has_phone_str_json_path = os.path.join(home_data, r'company_desc_has_phone_str.json')
company_id_and_phone_url_json_path = os.path.join(home_data, r'company_id_and_phone_url.json')
company_id_and_phone_str_json_path = os.path.join(home_data, r'company_id_and_phone_str.json')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}

proxies = {"http": "http://61.135.217.7:80", "https": "http://182.99.195.131:8118", }


def download_countries_list():
    """
    下载国家列表页
    :return:
    """
    result = requests.get(home_url + '/', headers=headers, timeout=5)
    # print(result.text)
    with open(company_countries_list_html, 'w', encoding='utf8') as fp:
        fp.write(result.text)


def parse_countries_list_to_json():
    """
    解析国家列表界面，并持久化为json文件
    :return:
    """
    countries_url_list_json = []

    with open(company_countries_list_html, 'r', encoding='utf8') as fp:
        data_stream = fp.read()

    soup = BeautifulSoup(data_stream, 'html.parser')

    countries_select = soup.select('ul.clearfix > li > a')

    for item_select in countries_select:
        country_url_end = item_select.get('href')
        if country_url_end == '#':
            continue

        country_name = item_select.text
        country_url = home_url + country_url_end

        countries_url_list_json.append({
            'country_name': country_name,
            'country_url': country_url
        })

    with open(countries_url_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(countries_url_list_json))


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
            if while_times < 1000:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url)
                time.sleep(0.2)
                continue
            else:
                raise e
        else:
            return result


def download_countries_company_list_files():
    """
    下载各个国家的公司列表页面
    :return:
    """
    with open(countries_url_list_json_path, 'r', encoding='utf8') as fp:
        countries_url_list_json = json.loads(fp.read())

    # print(countries_url_list_json)
    for item_country_url_dict in countries_url_list_json:
        country_name = item_country_url_dict['country_name']
        country_url = item_country_url_dict['country_url']
        country_url_home = str(country_url).replace(r'.html', '')

        print(country_name)

        country_dir_path = os.path.join(countries_list_dir_path, country_name)
        if not os.path.exists(country_dir_path):
            os.mkdir(country_dir_path)

        first_page_url = country_url_home + r'/p1.html'
        first_page_url_file_name = first_page_url.replace('http://', '').replace('/', '_')
        first_page_url_path = os.path.join(country_dir_path, first_page_url_file_name)

        if os.path.exists(first_page_url_path):
            with open(first_page_url_path, 'r', encoding='utf8') as fp:
                context = fp.read()
        else:
            result = while_requests_get(first_page_url)
            context = result.text
            with open(first_page_url_path, 'w', encoding='utf8') as fp:
                fp.write(context)

        soup = BeautifulSoup(context, 'html.parser')

        last_page_select = soup.select('div.pagea > ul > a')

        if len(last_page_select) > 0:
            if last_page_select[-1].text == 'Last':
                page_size_href = last_page_select[-1].get('href').replace('.html', '')
                page_size_index_start = page_size_href.rfind('p') + 1
                page_size = int(page_size_href[page_size_index_start:])
            else:
                continue
        else:
            continue

        for page_index in range(2, page_size + 1):
            page_url = country_url_home + r'/p' + str(page_index) + r'.html'
            page_url_file_name = page_url.replace('http://', '').replace('/', '_')
            page_url_path = os.path.join(country_dir_path, page_url_file_name)

            if os.path.exists(page_url_path):
                continue
            else:
                result = while_requests_get(page_url)
                inner_context = result.text

                if len(inner_context) < 2 << 10:
                    print(1 / 0)
                else:
                    with open(page_url_path, 'w', encoding='utf8') as fp:
                        fp.write(inner_context)

                    print(country_name, page_size, page_url_path)
                    # time.sleep(random.randint(10, 50)/10)

                    # break

                    # print(country_name, country_url)


def parse_countries_company_list_to_json():
    """
    解析各个国家的公司列表到json文件
    :return:
    """
    company_id = 0
    company_desc_url_list = []
    for country_dir in os.listdir(countries_list_dir_path):
        country_dir_path = os.path.join(countries_list_dir_path, country_dir)
        for company_list_file in os.listdir(country_dir_path):
            company_list_file_path = os.path.join(country_dir_path, company_list_file)
            # print(country_dir, company_list_file_path)

            with open(company_list_file_path, 'r', encoding='utf8') as fp:
                context = fp.read()

            soup = BeautifulSoup(context, 'html.parser')

            company_list_select_ul = soup.select('div.body > ul')

            if len(company_list_select_ul) > 0:
                company_list_select = company_list_select_ul[0].select('li')

                for item_company in company_list_select:
                    item_company_name_and_href = item_company.select('h4 > a')[0]
                    company_name = item_company_name_and_href.get('title').strip()
                    company_href = item_company_name_and_href.get('href').strip()

                    company_id += 1

                    temp_dict = {
                        'company_id': company_id,
                        'company_name': company_name,
                        'company_href': company_href,
                    }

                    company_desc_url_list.append(temp_dict)
                    print(country_dir, company_id, company_name, company_href)

    with open(company_desc_url_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_desc_url_list))


def read_company_desc_url_list():
    """
    读取公司列表json
    :return:
    """
    with open(company_desc_url_list_json_path, 'r', encoding='utf8') as fp:
        data = fp.read()
    return json.loads(data)


def download_company_desc_html(item_company_info_dict):
    """
    下载公司详情页
    :param item_company_info_dict:
    :return:
    """
    company_id = item_company_info_dict['company_id']
    # company_name = item_company_info_dict['company_name']
    company_href = item_company_info_dict['company_href']

    file_name = str(company_href).replace('http://', '').replace('/', '_').replace('-', '_')\
        .replace('.html', '') + '^' + str(company_id) + '.html'
    file_path = os.path.join(company_desc_list_dir_path, file_name)

    if not os.path.exists(file_path):
        result = while_requests_get(company_href)

        if len(result.text) < 2 << 10:
            while True:
                print('##################################################')
                print('####-------------- change IP -----------------####')
                print('##################################################')
                # _r = requests.get(home_url)
                _r = requests.get('http://www.baidu.com')
                if _r.status_code == 200:
                    break
                time.sleep(3)
        else:
            with open(file_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)

            print(file_path)


def multiprocessing_download_files(download_file_func, read_json_list, pool_num=30):
    """
    多进程下载页面
    :return:
    """
    print(len(read_json_list))

    pool = Pool(pool_num)
    pool.map(download_file_func, read_json_list)
    pool.close()
    pool.join()


def while_multiprocessing_download_files():
    """
    循环执行多进程爬取
    防止异常终止
    :return:
    """
    while True:
        try:
            multiprocessing_download_files(download_company_desc_html, read_company_desc_url_list(), pool_num=70)
        except Exception as e:
            print(e)


def parse_company_desc_files_to_json():
    """
    解析公司详情页，存为json文件
    :return:
    """
    count = 0
    company_desc_all_keys_set = set()
    company_desc_list_json_1 = []
    for file_name in os.listdir(company_desc_list_dir_path):
        count += 1
        print('\r' + str(count), end='')
        # if count == 1:
        #     continue
        file_path = os.path.join(company_desc_list_dir_path, file_name)
        with open(file_path, 'r', encoding='utf8') as fp:
            data_stream = fp.read()
        soup = BeautifulSoup(data_stream, 'html.parser')
        # print(soup)

        temp_dict = {}

        company_id = str(file_name.split('^')[-1]).split('.')[0]
        company_id_key = 'company_id'
        temp_dict[company_id_key] = company_id
        company_desc_all_keys_set.add(company_id_key)
        # print(company_id)

        company_name_select = soup.select('div.the06 > h1')
        if len(company_name_select) > 0:
            company_name = company_name_select[0].text.strip()
            company_name_key = 'company_name'
            temp_dict[company_name_key] = company_name
            company_desc_all_keys_set.add(company_name_key)
            # print(company_name)

        company_desc_select = soup.select('div.the08')
        if len(company_desc_select) > 0:
            company_desc = company_desc_select[0].text.replace('Company Description', '', 1).strip()
            company_desc_key = 'company_desc'
            temp_dict[company_desc_key] = company_desc
            company_desc_all_keys_set.add(company_desc_key)
            # print(company_desc)

        company_info_and_contact_info_select = soup.select('div.the09 > ul')

        if len(company_info_and_contact_info_select) > 0:
            company_info_select = company_info_and_contact_info_select[0].select('li')
            for item_li in company_info_select:
                company_info_key = item_li.select('strong')[0].text.strip().replace(':', '')
                company_info_value = item_li.select('span')[0].text.strip()
                if company_info_value == '- -':
                    company_info_value = ''
                temp_dict[company_info_key] = company_info_value
                company_desc_all_keys_set.add(company_info_key)
                # print(company_info_key, ':', company_info_value)

        # print()
        has_url_end_keys_list = []
        if len(company_info_and_contact_info_select) > 1:
            company_contact_info_select = company_info_and_contact_info_select[1].select('li')
            for item_li in company_contact_info_select:
                company_info_key = item_li.select('strong')[0].text.strip().replace(':', '')
                company_info_value = item_li.select('span')[0].text.strip()
                if company_info_value == '':
                    company_info_img_select = item_li.select('span')[0].select('img')
                    if len(company_info_img_select) > 0:
                        has_url_end_keys_list.append(company_info_key)
                        company_info_value = company_info_img_select[0].get('src').strip()
                temp_dict[company_info_key] = company_info_value
                temp_dict['has_url_end_keys_list_key'] = has_url_end_keys_list
                company_desc_all_keys_set.add(company_info_key)
                # print(company_info_key, ':', company_info_value)

        # print(temp_dict)
        # if count == 5:
        #     break
        # break
        company_desc_list_json_1.append(temp_dict)
    # print(company_desc_list_json_1)
    # print(company_desc_all_keys_set)

    with open(company_desc_list_json_1_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_desc_list_json_1))

    with open(company_desc_all_keys_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(list(company_desc_all_keys_set)))


def merge_phone_url_to_company_desc_json():
    """
    合并手机号码图片url地址到公司详情json
    :return:
    """
    company_desc_has_phone_url = []
    with open(company_desc_url_list_json_path, 'r', encoding='utf8') as fp:
        company_desc_url_list_json = json.loads(fp.read())

    with open(company_desc_list_json_1_path, 'r', encoding='utf8') as fp:
        company_desc_list_json_1 = json.loads(fp.read())

    # print(len(company_desc_url_list_json))
    # print(len(company_desc_list_json_1))
    # print()
    # print(json.dumps(company_desc_url_list_json[0]))
    # print(json.dumps(company_desc_list_json_1[0]))

    count = 0

    for desc_json_item in company_desc_list_json_1:
        desc_json_item_id_str = desc_json_item['company_id']

        for url_json_item in company_desc_url_list_json:
            url_json_item_id_int = url_json_item['company_id']

            if desc_json_item_id_str == str(url_json_item_id_int):
                company_href = url_json_item['company_href']
                has_url_end_keys_list_key_list = desc_json_item.get('has_url_end_keys_list_key', [])
                for item_has_url_key in has_url_end_keys_list_key_list:
                    desc_json_item[item_has_url_key] = company_href + desc_json_item[item_has_url_key]
                count += 1
                print(url_json_item_id_int, count)
                company_desc_has_phone_url.append(desc_json_item)
                break

        # if count > 10:
        #     break

    with open(company_desc_has_phone_url_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_desc_has_phone_url))


def parse_company_id_and_phone_url_to_json():
    """
    解析出公司id和对应手机号码url
    :return:
    """
    company_id_and_phone_url_json = []
    with open(company_desc_has_phone_url_json_path, 'r', encoding='utf8') as fp:
        company_desc_has_phone_url_json = json.loads(fp.read())

    for item_dict in company_desc_has_phone_url_json:
        company_id = item_dict['company_id']
        has_url_end_keys_list_key = item_dict.get('has_url_end_keys_list_key', [])
        for item_key in has_url_end_keys_list_key:
            item_value = item_dict[item_key]
            temp_list = [company_id, item_value]
            company_id_and_phone_url_json.append(temp_list)
            print(temp_list)

    with open(company_id_and_phone_url_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_id_and_phone_url_json))


def download_all_company_phone_img(item):
    """
    下载所有的公司手机号图片
    :param item: 单条信息，形如[company_id, phone_url]
    :return:
    """
    company_id, phone_url = item
    img_dir_path = os.path.join(phone_img_list_dir_path, company_id)

    if not os.path.exists(img_dir_path):
        os.mkdir(img_dir_path)

    img_name = str(phone_url).split('/')[-1]
    img_path = os.path.join(img_dir_path, img_name)

    if not os.path.exists(img_path):
        result = while_requests_get(phone_url)
        if result.status_code == 200:
            with open(img_path, 'wb') as fp:
                for chunk in result.iter_content(1024):
                    fp.write(chunk)
            print(img_path, 'OK')
    else:
        # print(img_path, 'saved')
        pass


def parse_test_demo():
    """
    解析测试
    :return:
    """
    test_file_path = r'E:\work_all\topease\company_spider_2\company_desc_list\www.listcompany' \
                     r'.org_Yi_Wu_Zhan_Xin_Wig_Factory_Info^407223.html'
    with open(test_file_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    # soup = BeautifulSoup(bytes(data_stream, 'utf8').decode('iso-8859-1'), 'html.parser')
    soup = BeautifulSoup(data_stream, 'html.parser')
    print(soup)


def read_company_id_and_phone_url_list():
    """
    读取公司id和公司手机号码图片url的json
    :return:
    """
    with open(company_id_and_phone_url_json_path, 'r', encoding='utf8') as fp:
        data_stream = fp.read()
    return json.loads(data_stream)


def read_company_desc_has_phone_url_json_list():
    """
    读取公司详情（含有手机号码图片链接的json）
    :return:
    """
    with open(company_desc_has_phone_url_json_path, 'r', encoding='utf8') as fp:
        company_desc_has_phone_url_json = json.loads(fp.read())
    return company_desc_has_phone_url_json


def ocr_phone_img_to_str(item_dict):
    """
    ocr
    手机号码图像识别
    :param item_dict:单条信息，形如[company_id, phone_url]
    :return:
    """
    company_id, phone_url = item_dict
    img_dir_path = os.path.join(phone_img_list_dir_path, company_id)

    img_name = str(phone_url).split('/')[-1]
    img_path = os.path.join(img_dir_path, img_name)
    img_str_json_path = os.path.join(phone_str_json_list_dir_path, company_id + '^' + img_name + '.txt')

    if not os.path.exists(img_str_json_path):
        if os.path.exists(img_path):
            img_str = ocr_img_to_str.img_to_str(img_path)

            if len(img_str) <= 3:
                img_str = ''

            if re.search(r'[a-zA-Z]+', img_str):
                print(img_str_json_path, ' --- error --- ', img_str)
                img_str = ''

            temp_dict = {
                'company_id': company_id,
                'phone_str': img_str
            }

            with open(img_str_json_path, 'w', encoding='utf8') as fp:
                fp.write(json.dumps(temp_dict))


def marge_all_phone_str_to_json():
    """
    合并所有的图像识别后的手机号码
    :return:
    """
    count = 0
    all_count = len(os.listdir(phone_str_json_list_dir_path))
    all_phone_str_json = []
    for file_name in os.listdir(phone_str_json_list_dir_path):
        file_path = os.path.join(phone_str_json_list_dir_path, file_name)
        file_name_split_list = file_name.split('^')
        company_id = file_name_split_list[0]
        phone_img_name = file_name_split_list[1].replace('.txt', '')
        # print(company_id, phone_img_name)

        with open(file_path, 'r', encoding='utf8') as fp:
            item_dict = json.loads(fp.read())

        item_dict['phone_img_name'] = phone_img_name

        all_phone_str_json.append(item_dict)

        count += 1
        print('\r%.4f%%' % (count / all_count * 100), end='')

    with open(company_id_and_phone_str_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(all_phone_str_json))


def read_company_id_and_phone_str_json():
    """
    读取公司id&手机号码的json文件
    :return:
    """
    with open(company_id_and_phone_str_json_path, 'r', encoding='utf8') as fp:
        company_id_and_phone_str_json = json.loads(fp.read())
    return company_id_and_phone_str_json


def marge_company_desc_and_phone_str_to_new_json():
    """
    合并公司详情与手机号码到新的json文件
    :return:
    """
    company_desc_has_phone_str_json = []
    company_desc_has_phone_url_json = read_company_desc_has_phone_url_json_list()
    company_id_and_phone_str_json = read_company_id_and_phone_str_json()

    count = 0
    all_count = len(company_id_and_phone_str_json)

    for item_desc in company_desc_has_phone_url_json:
        company_id_1 = item_desc['company_id']
        has_url_end_keys_list_key_list = item_desc.get('has_url_end_keys_list_key', [])
        for key in has_url_end_keys_list_key_list:
            url_value = str(item_desc[key])
            phone_img_name_1 = url_value.split('/')[-1]

            for item_phone in company_id_and_phone_str_json:
                company_id_2 = item_phone['company_id']
                phone_img_name_2 = item_phone['phone_img_name']
                if company_id_1 == company_id_2 and phone_img_name_1 == phone_img_name_2:
                    item_desc[key] = item_phone['phone_str']
                    count += 1
                    print('\r%.4f%%, %s' % ((count / all_count * 100), company_id_1), end='')
                else:
                    continue
        company_desc_has_phone_str_json.append(item_desc)
        # if len(company_desc_has_phone_str_json) == 10:
        #     break

    with open(company_desc_has_phone_str_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_desc_has_phone_str_json))


if __name__ == '__main__':
    # download_countries_list()
    # parse_countries_list_to_json()
    # download_countries_company_list_files()
    # parse_countries_company_list_to_json()
    # multiprocessing_download_files(download_company_desc_html, read_company_desc_url_list(), pool_num=50)
    # while_multiprocessing_download_files()
    # parse_company_desc_files_to_json()
    # merge_phone_url_to_company_desc_json()
    # parse_company_id_and_phone_url_to_json()
    # multiprocessing_download_files(download_all_company_phone_img, read_company_id_and_phone_url_list(), pool_num=80)
    # multiprocessing_download_files(ocr_phone_img_to_str, read_company_id_and_phone_url_list(), pool_num=7)
    # marge_all_phone_str_to_json()
    marge_company_desc_and_phone_str_to_new_json()
    # parse_test_demo()
