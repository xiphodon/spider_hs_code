#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/5 16:24
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : madeinchina_spider.py
# @Software: PyCharm


from gevent import pool, monkey; monkey.patch_all()
import WhileRequests
from lxml import etree
import json
import os
import time
from decimal import Decimal
import re


home_url = r'https://www.made-in-china.com/'
suppliers_discovery_url = home_url + r'suppliers-discovery/'

home_path_dir = r'E:\mic'
suppliers_discovery_path = os.path.join(home_path_dir, 'suppliers_discovery.html')
suppliers_discovery_json_path = os.path.join(home_path_dir, 'suppliers_discovery.json')

suppliers_discovery_all_group_list_dir = os.path.join(home_path_dir, 'all_group_list_dir')
suppliers_category_company_list_dir = os.path.join(home_path_dir, 'com_list')

suppliers_class_list_json_path = os.path.join(home_path_dir, 'suppliers_class_list.json')

company_list_json_dir = os.path.join(home_path_dir, 'company_list_json_dir')
company_list_json_path = os.path.join(company_list_json_dir, 'company_list.json')

company_page_list_dir = os.path.join(home_path_dir, 'company_page_list')

request = WhileRequests.WhileRequests()

id_interval = 100000


def mkdir(dir_path):
    """
    创建文件夹
    :param dir_path:
    :return:
    """
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)


def download_suppliers_discovery():
    """
    下载供应商频道列表
    :return:
    """
    file_content = None

    if os.path.exists(suppliers_discovery_path):

        with open(suppliers_discovery_path, 'r', encoding='utf8') as fp:
            file_content = fp.read()

    else:
        print('get suppliers discovery from web')
        result = request.get(suppliers_discovery_url, request_times=0.5)

        with open(suppliers_discovery_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)

        file_content = result.text

    if file_content is not None:
        # print(file_content)
        selector = etree.HTML(file_content)

        suppliers_discovery_div = selector.xpath('//div[@id="J-sd"]')[0]
        # print(suppliers_discovery_div)

        suppliers_category_name_list = suppliers_discovery_div.xpath('./ul/li/a/text()')
        # print(suppliers_category_name_list)

        suppliers_category_div_list = suppliers_discovery_div.xpath('./div[@class="tab-con"]')
        # print(suppliers_category_div_list)

        suppliers_all_category_url_list = list()

        for category_name, category_div in zip(suppliers_category_name_list, suppliers_category_div_list):
            suppliers_category_url_list = category_div.xpath('.//a[@class="cat-icon"]/@href')

            # print(category_name)
            # print(suppliers_category_url_list)

            for item_url in suppliers_category_url_list:
                temp_dict = {
                    'category_name': category_name,
                    'suppliers_category_url': 'http:' + item_url
                }
                suppliers_all_category_url_list.append(temp_dict)

        # print(suppliers_all_category_url_list)

        with open(suppliers_discovery_json_path, 'w', encoding='utf8') as fp:
            fp.write(json.dumps(suppliers_all_category_url_list))


def get_suppliers_discovery_json():
    """
    获取供应商频道json数据
    :return:
    """
    if not os.path.exists(suppliers_discovery_json_path):
        download_suppliers_discovery()

    with open(suppliers_discovery_json_path, 'r', encoding='utf8') as fp:
        suppliers_all_category_url_list = json.load(fp)

    return suppliers_all_category_url_list


def download_suppliers_category_group_list_html():
    """
    下载供应各商频道的组列表页面
    :return:
    """
    suppliers_all_category_url_list = get_suppliers_discovery_json()

    for item_category in suppliers_all_category_url_list:
        print(item_category)
        suppliers_category_name = item_category['category_name']
        suppliers_category_url = item_category['suppliers_category_url']

        suppliers_category_name_dir = os.path.join(suppliers_discovery_all_group_list_dir, suppliers_category_name)
        if not os.path.exists(suppliers_category_name_dir):
            os.mkdir(suppliers_category_name_dir)

        suppliers_category_file_name = str(suppliers_category_url).replace('http://', '').replace('/', '_')
        suppliers_category_name_url_file_path = os.path.join(suppliers_category_name_dir, suppliers_category_file_name)

        if not os.path.exists(suppliers_category_name_url_file_path):
            print(f'download OK : {suppliers_category_url}')
            result = request.get(suppliers_category_url, request_times=1)
            content = result.text

            with open(suppliers_category_name_url_file_path, 'w', encoding='utf8') as fp:
                fp.write(content)


def download_suppliers_category_url_html():
    """
    下载供应商品品类页面
    :return:
    """
    suppliers_class_list = list()

    # i = 0

    for item_dir_name in os.listdir(suppliers_discovery_all_group_list_dir):
        item_dir_path = os.path.join(suppliers_discovery_all_group_list_dir, item_dir_name)
        for item_file_name in os.listdir(item_dir_path):

            # i += 1
            # if i != 2:
            #     continue

            item_file_path = os.path.join(item_dir_path, item_file_name)
            print(item_file_path)

            with open(item_file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)

            suppliers_class_1_name = selector.xpath(r'//div[@id="tag-tit"]/div[@id="tag"]/h1/text()')[0]
            print(f'- {suppliers_class_1_name}')

            suppliers_group_list_div = selector.xpath(r'//div[@class="list-group"]')[0]
            # print(suppliers_group_list_div)

            suppliers_class_2_div_list = suppliers_group_list_div.xpath(r'.//div[@class="section"]')
            # print(suppliers_class_2_div_list)

            for item_section in suppliers_class_2_div_list:
                suppliers_class_2_name = item_section.xpath(r'.//div[@class="heading"]/h2/text()')[0]
                print(f'-- {suppliers_class_2_name}')

                suppliers_class_2_category_div = item_section.xpath(r'.//div[@class="list-cat"]')[0]
                # print(suppliers_class_2_category_div)

                # if suppliers_class_2_name == 'Special Apparel':
                #     print(etree.tostring(suppliers_class_2_category_div).decode())
                # print(etree.tostring(suppliers_class_2_category_div).decode())

                suppliers_class_3_li_list = suppliers_class_2_category_div.xpath(r'./ul/li')
                suppliers_class_3_li_list_2 = suppliers_class_2_category_div.xpath(r'./li')
                suppliers_class_3_li_list.extend(suppliers_class_3_li_list_2)
                # print(suppliers_class_3_li_list)

                suppliers_class_3_name_last = None
                suppliers_class_3_url_last = None
                for item_class_3_view in suppliers_class_3_li_list:
                    # print(item_class_3_view)

                    suppliers_class_3_name_list = item_class_3_view.xpath(r'./h3/a/text()')
                    if len(suppliers_class_3_name_list) == 0:
                        suppliers_class_3_name = suppliers_class_3_name_last
                        suppliers_class_3_url = suppliers_class_3_url_last
                    else:
                        suppliers_class_3_name = suppliers_class_3_name_list[0]
                        suppliers_class_3_name_last = suppliers_class_3_name
                        suppliers_class_3_url = item_class_3_view.xpath(r'./h3/a/@href')[0]
                        suppliers_class_3_url_last = suppliers_class_3_url

                    print(f'--- {suppliers_class_3_name}')
                    print(suppliers_class_3_url)

                    suppliers_class_4_li_list = item_class_3_view.xpath(r'./ul/li')

                    if len(suppliers_class_4_li_list) > 0:
                        # 有第四级别
                        for item_class_4_view in suppliers_class_4_li_list:
                            suppliers_class_4_name = item_class_4_view.xpath(r'./a/text()')[0]
                            suppliers_class_4_url = item_class_4_view.xpath(r'./a/@href')[0]

                            print(f'---- {suppliers_class_4_name}')
                            print(suppliers_class_4_url)

                            temp_dict = {
                                'suppliers_class_1_name': suppliers_class_1_name,
                                'suppliers_class_2_name': suppliers_class_2_name,
                                'suppliers_class_3_name': suppliers_class_3_name,
                                'suppliers_class_3_url': 'https:' + suppliers_class_3_url,
                                'suppliers_class_4_name': suppliers_class_4_name,
                                'suppliers_class_4_url': 'https:' + suppliers_class_4_url,
                            }
                            suppliers_class_list.append(temp_dict)
                    else:
                        # 无第四级别
                        temp_dict = {
                            'suppliers_class_1_name': suppliers_class_1_name,
                            'suppliers_class_2_name': suppliers_class_2_name,
                            'suppliers_class_3_name': suppliers_class_3_name,
                            'suppliers_class_3_url': 'https:' + suppliers_class_3_url,
                            'suppliers_class_4_name': '',
                            'suppliers_class_4_url': '',
                        }
                        suppliers_class_list.append(temp_dict)
            # break
        # break

    print(f'suppliers_class_list len:{len(suppliers_class_list)}')

    with open(suppliers_class_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(suppliers_class_list))


def download_suppliers_list():
    """
    下载供应商列表
    :return:
    """
    with open(suppliers_class_list_json_path, 'r', encoding='utf8') as fp:
        data = json.load(fp)
    # print(data)
    for item_category in data:
        # print(item_category)
        suppliers_class_1_name = item_category.get('suppliers_class_1_name', '')
        suppliers_class_2_name = item_category.get('suppliers_class_2_name', '')
        suppliers_class_3_name = item_category.get('suppliers_class_3_name', '')
        suppliers_class_3_url = item_category.get('suppliers_class_3_url', '')
        suppliers_class_4_name = item_category.get('suppliers_class_4_name', '')
        suppliers_class_4_url = item_category.get('suppliers_class_4_url', '')

        # print(suppliers_class_1_name)
        # print(suppliers_class_2_name)
        # print(suppliers_class_3_name)
        # print(suppliers_class_3_url)
        # print(suppliers_class_4_name)
        # print(suppliers_class_4_url)

        if suppliers_class_4_name != '':
            url = suppliers_class_4_url
            dir_name = \
                f'{suppliers_class_1_name}_{suppliers_class_2_name}_{suppliers_class_3_name}_{suppliers_class_4_name}'\
                .replace('/', 'or')
        else:
            url = suppliers_class_3_url
            dir_name = f'{suppliers_class_1_name}_{suppliers_class_2_name}_{suppliers_class_3_name}'.replace('/', 'or')

        dir_path = os.path.join(suppliers_category_company_list_dir, dir_name)

        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        while True:
            file_name_short = url.split('-')[-1]
            # file_name_1 = url.replace('https://www.made-in-china.com/', '').replace('/', '_')
            # file_name_2 = url.replace('https://', '').replace('/', '_')
            file_path_short = os.path.join(dir_path, file_name_short)
            # file_path_1 = os.path.join(dir_path, file_name_1)
            # file_path_2 = os.path.join(dir_path, file_name_2)

            if os.path.exists(file_path_short) and os.path.getsize(file_path_short) > 10 * 2 << 10:
                print(file_path_short)
                with open(file_path_short, 'r', encoding='utf8') as fp:
                    content = fp.read()
            else:
                print(file_path_short)
                result = request.get(url, sleep_time=get_sleep_time_from_file())
                content = result.text

                with open(file_path_short, 'w', encoding='utf8') as fp:
                    fp.write(content)

            selector = etree.HTML(content)

            next_url_list = selector.xpath(r'.//a[@class="next"]/@href')

            # 是否有下一页
            if len(next_url_list) > 0:
                url = f'https:{next_url_list[0]}'
            else:
                break


def get_sleep_time_from_file():
    """
    获取睡眠时间
    :return:
    """
    with open(r'./settings.txt', 'r', encoding='utf8') as fp:
        lines = fp.readlines()

    sleep_time_float = 2.0

    for line in lines:
        line_split = line.split('=')

        if len(line_split) == 2:
            if line_split[0].strip() == 'sleep_time':
                sleep_time_str = line_split[1].strip()
                try:
                    sleep_time_float = Decimal(sleep_time_str)
                except Exception as e:
                    pass
                # print(sleep_time_float)
                return sleep_time_float
    # print(sleep_time_float)
    return sleep_time_float


def parse_company_list_pages():
    """
    解析公司列表页
    :return:
    """
    company_id = 1
    for dir_index, item_dir in enumerate(os.listdir(suppliers_category_company_list_dir)):
        company_list = list()
        # company_list_count = 0
        item_dir_path = os.path.join(suppliers_category_company_list_dir, item_dir)
        for item_file in os.listdir(item_dir_path):
            file_path = os.path.join(item_dir_path, item_file)
            print(file_path)

            with open(file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)
            node_view_list = selector.xpath(r'.//div[@class="search-list"]/div')

            for node_view in node_view_list:
                print('==================================================')
                temp_dict = dict()

                company_name = node_view.xpath(r'./h2[@class="company-name"]/a/text()')[0]
                company_url = 'https:' + node_view.xpath(r'./h2[@class="company-name"]/a/@href')[0]
                temp_dict['company_name'] = company_name
                temp_dict['company_url'] = company_url.replace('/360-Virtual-Tour.html', '')
                # print(f'company_name {company_name}')
                # print(f'company_url {company_url}')

                company_auth_view_list = node_view.xpath(r'.//div[@class="compnay-auth"]')
                if len(company_auth_view_list) > 0:
                    company_auth_view = company_auth_view_list[0]

                    company_member_list = company_auth_view.xpath(r'./span/img/@alt')
                    company_member = ''
                    if len(company_member_list) > 0:
                        company_member = company_member_list[0]
                    company_audited_list = company_auth_view.xpath(r'./span//a/img/@alt')
                    company_audited = ''
                    if len(company_audited_list) > 0:
                        company_audited = company_audited_list[0]
                    temp_dict['company_member'] = company_member
                    temp_dict['company_audited'] = company_audited
                    # print(f'company_member {company_member}')
                    # print(f'company_audited {company_audited}')

                company_info_view_list = node_view.xpath(r'.//div[@class="company-info"]')
                if len(company_info_view_list) > 0:
                    company_info_view = company_info_view_list[0]

                    company_tr_list = company_info_view.xpath(r'.//tr')
                    company_info_key_value_list = list()
                    for item_tr in company_tr_list:
                        company_td = item_tr.xpath(r'./td')
                        company_info_key = str(company_td[0].xpath(r'string()')).strip(':')
                        company_info_value = str(company_td[1].xpath(r'string()')).strip()
                        company_info_value_str = re.sub(re.compile(r'\s{2,}'), '', company_info_value)
                        company_info_key_value_list.append({
                            'company_info_key': company_info_key,
                            'company_info_value_str': company_info_value_str,
                        })
                        # print(f'company_info_key {company_info_key}')
                        # print(f'company_info_value {company_info_value_str}')
                    temp_dict['company_info'] = company_info_key_value_list

                company_class_list = item_dir.split('_')

                for class_index, class_item in enumerate(company_class_list):
                    temp_dict[f'class_{class_index + 1}'] = class_item

                # print(company_class_list)
                temp_dict['company_id'] = company_id
                company_id += 1
                print(company_id)
                print(temp_dict)
                company_list.append(temp_dict)
                # company_list_count += 1
                # print(company_list_count)
            # break
        # break

        with open(company_list_json_path.replace('.json', f'_{dir_index + 1}.json'), 'w', encoding='utf8') as fp:
            fp.write(json.dumps(company_list))


def rename_company_list_file():
    """
    重命名文件名
    :return:
    """
    for item_dir in os.listdir(suppliers_category_company_list_dir):
        item_dir_path = os.path.join(suppliers_category_company_list_dir, item_dir)
        for item_file in os.listdir(item_dir_path):
            if len(item_file) > 10:
                new_file_name = item_file.split('-')[-1]
                item_file_path = os.path.join(item_dir_path, item_file)
                new_file_path = os.path.join(item_dir_path, new_file_name)
                if os.path.isfile(item_file_path):
                    os.renames(item_file_path, new_file_path)
                    print(f'rename {item_file_path}')
            # break
        # break


def get_company_id_dir_name(company_id: int):
    """
    获取company id 所属的文件夹名称
    :param company_id:
    :return:
    """
    file_dir_name_start = company_id // id_interval * id_interval
    file_dir_name_end = file_dir_name_start + id_interval - 1
    file_dir_name = f'{file_dir_name_start}_{file_dir_name_end}'
    return file_dir_name


def move_company_page_file():
    """
    移动公司详情页
    :return:
    """

    for item_name in os.listdir(company_page_list_dir):
        item_path = os.path.join(company_page_list_dir, item_name)
        if os.path.isfile(item_path):
            name_id = item_name.replace('.html', '')
            file_dir_name = get_company_id_dir_name(int(name_id))
            file_dir_path = os.path.join(company_page_list_dir, file_dir_name)
            mkdir(file_dir_path)
            new_file_path = os.path.join(file_dir_path, item_name)
            # print(item_path, new_file_path)
            os.renames(item_path, new_file_path)


def download_all_company_page():
    """
    下载所有公司页面
    :return:
    """
    for item_file in os.listdir(company_list_json_dir):
        # print(item_file)
        item_file_path = os.path.join(company_list_json_dir, item_file)

        with open(item_file_path, 'r', encoding='utf8') as fp:
            company_list = json.load(fp)

        # print(company_list)

        this_page_url_and_path_list = list()

        for item_company in company_list:
            # print(item_company)
            company_url = dict(item_company).get('company_url')
            company_id = dict(item_company).get('company_id')

            company_file_name = str(company_id) + '.html'
            company_file_dir = get_company_id_dir_name(int(company_id))
            company_file_dir_path = os.path.join(company_page_list_dir, company_file_dir)
            mkdir(company_file_dir_path)
            company_file_path = os.path.join(company_file_dir_path, company_file_name)

            if 'showroom' not in company_url:
                # independent
                company_url += '/contact-info.html'

            # company_url = 'https://cnquenson.en.made-in-china.com/contact-info.html'
            # # company_url = 'https://www.made-in-china.com/showroom/mumu1314/'

            this_page_url_and_path_list.append((company_url, company_file_path))

            # content = etree.HTML(result.text)
            #
            # nav_ul_list = content.xpath(r'.//ul[@class="sr-nav-main"]')
            #
            # if len(nav_ul_list) > 0:
            #     # independent
            #     pass
            # else:
            #     # showroom
            #     pass

            # print(company_url)
            # break

        gevent_pool_requests(download_company_info_page, this_page_url_and_path_list, gevent_pool_size=2)
        # break


def download_company_info_page(company_url_and_path):
    """
    下载公司详情页
    :param company_url_and_path:
    :return:
    """
    company_url, company_file_path = company_url_and_path

    if os.path.exists(company_file_path) and os.path.getsize(company_file_path) > 10 * 2 << 10:
        print(company_file_path)
    else:
        result = request.get(company_url, sleep_time=get_sleep_time_from_file())

        if len(result.content) > 10 * 2 << 10:
            with open(company_file_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)
            print(company_url, company_file_path)
            print(f'page size: {len(result.content)}')
        else:
            print(company_url, company_file_path)
            print('!!!!!!!!!! page size < 10kb')
            # time.sleep(30)


def gevent_pool_requests(func, task_list, gevent_pool_size=10):
    """
    多协程请求
    :param func:
    :param task_list:
    :param gevent_pool_size:
    :return:
    """
    gevent_pool = pool.Pool(gevent_pool_size)
    result_list = gevent_pool.map(func, task_list)
    return result_list


def init():
    """
    初始化
    :return:
    """
    mkdir(suppliers_category_company_list_dir)
    mkdir(suppliers_discovery_all_group_list_dir)
    mkdir(company_list_json_dir)
    mkdir(company_page_list_dir)


def start():
    """
    入口
    :return:
    """
    # download_suppliers_category_group_list_html()
    # download_suppliers_category_url_html()
    # download_suppliers_list()
    # parse_company_list_pages()
    download_all_company_page()

    # rename_company_list_file()
    # move_company_page_file()


if __name__ == '__main__':
    init()
    start()
