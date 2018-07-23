#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/15 14:43
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_9.py
# @Software: PyCharm

from gevent import pool, monkey; monkey.patch_all()
import requests
import os
from lxml import etree
import settings
import json
import time
import traceback
import re
import random


search_text_list = ['pump', 'fabric', 'glass', 'clothing', 'embroidery', 'E-Liquid', 'rayon', 'jacquard', 'toy'
                    , 'furniture', 'textile+-clothing']
search_text = search_text_list[-1]

search_type_dict = {'product': 'PRODUCT',
                    'supplier': 'SUPPLIER'}
search_type = search_type_dict['supplier']

home_url = settings.company_home_url_1
search_url = home_url + r'/searchCompanies?' \
             r'acClassif=&localizationCode=&localizationLabel=&localizationType=&text=' + search_text + \
             r'&searchType=' + search_type

home_path = r'E:\work_all\topease\company_spider_9'

json_dir_path = os.path.join(home_path, 'json_dir')
company_detail_main_keyword_json_path = os.path.join(json_dir_path, search_text + '_main_keyword.json')
company_list_json_path = os.path.join(json_dir_path, search_text + '_company_list.json')
company_urls_list_json_path = os.path.join(json_dir_path, search_text + '_company_urls_list.json')

company_list_pages_dir_path = os.path.join(home_path, 'company_list_dir')
company_list_pages_product_dir_path = os.path.join(company_list_pages_dir_path, search_text)

company_detail_dir_path = os.path.join(home_path, 'company_detail_dir')
company_detail_product_dir_path = os.path.join(company_detail_dir_path, search_text)

first_html_path = os.path.join(company_list_pages_product_dir_path, search_text + r'_' + search_type + r'_0.html')

headers_list = \
    ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1',
     'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0',
     'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50',
     'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.9.168 Version/11.50',
     'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; ' +
        '.NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; Tablet PC 2.0; .NET4.0E)',
     'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; ' +
        '.NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)',
     'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB7.0)',
     'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
     'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)',
     'Mozilla/5.0 (Windows; U; Windows NT 6.1; ) AppleWebKit/534.12 (KHTML, like Gecko) Maxthon/3.0 Safari/534.12',
     'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; ' +
        '.NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)',
     'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; ' +
        '.NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E; ' +
        'SE 2.X MetaSr 1.0)',
     'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.3 (KHTML, like Gecko) ' +
        'Chrome/6.0.472.33 Safari/534.3 SE 2.X MetaSr 1.0',
     'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; ' +
        '.NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)',
     'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; ' +
        '.NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E)',
     'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; ' +
        '.NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.3; .NET4.0C; .NET4.0E) ' +
        'QQBrowser/6.9.11079.201',
     'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
     'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0'
     ]

pool_size = 20
page_total = -1
overwrite = False

sess = requests.session()

if not os.path.exists(company_list_pages_product_dir_path):
    os.mkdir(company_list_pages_product_dir_path)

if not os.path.exists(company_detail_product_dir_path):
    os.mkdir(company_detail_product_dir_path)

if not os.path.exists(json_dir_path):
    os.mkdir(json_dir_path)


def while_session_get(page_url, times=5000, sleep_time=0.2, new_sess=False):
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            if new_sess:
                global sess
                sess = requests.session()

            time.sleep(random.randint(5, 8) * sleep_time)
            result = sess.get(page_url, headers=get_header(), timeout=5)
            # result = requests.get(page_url, headers=headers, proxies=proxies, timeout=5)
        except Exception as e:
            if while_times < times:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url.replace(home_url, ''))
                continue
            else:
                raise e
        else:
            return result


def get_header():
    """
    获取header
    :return:
    """
    return {
            'User-Agent': random.choice(headers_list),
            'Connection': 'keep-alive'
            }


def download_frist_html():
    """
    下载首次请求的html页面
    :return:
    """
    while True:
        result = while_session_get(search_url)

        if len(result.content) < 5 * 1024:
            continue
        else:
            break

    with open(first_html_path, 'w', encoding='utf8') as fp:
        fp.write(result.text)

    print('first html download ---- OK')


def parse_first_html():
    """
    解析首次请求到的页面
    :return:
    """
    with open(first_html_path, 'r', encoding='utf8') as fp:
        content = fp.read()

    selector = etree.HTML(content)

    total_page_li = selector.xpath('//ul[@class="pagination paginatorDivId"]/li')

    global page_total

    if len(total_page_li) >= 2:
        try:
            page_total = int(total_page_li[-1].xpath('./a/text()')[0])
        except Exception as e:
            page_total = int(total_page_li[-2].xpath('./a/text()')[0])
            print(e)
    else:
        page_total = 1

    print(page_total)


# def download_company_list_pages():
#     """
#     获取该商品的所有公司列表(废弃)
#     :return:
#     """
#     url_page_postfix = r'/searchCompanies/scroll?tab=cmp&pageNbre='
#
#     for i in range(2, page_total):
#         url_item = home_url + url_page_postfix + str(i)
#         this_page_html_path = os.path.join(company_list_pages_product_dir_path, search_text + r'_' + search_type +
#                                            r'_' + str(i) + r'.html')
#
#         if (not overwrite) and os.path.exists(this_page_html_path):
#             print('page:' + str(i) + '-------- exist')
#             continue
#
#         result = while_session_get(url_item)
#
#         with open(this_page_html_path, 'w', encoding='utf8') as fp:
#             fp.write(result.text)
#
#         if os.path.getsize(this_page_html_path) < 125 * 1024:
#             print('page:' + str(i) + '-------- this page html size < 125k')
#         else:
#             print('page:' + str(i) + '-------- download OK')


def download_this_page_company_list(url):
    """
    下载当前页公司列表
    :return:
    """
    this_page_str = url[str(url).rfind('=') + 1:]
    this_page_html_path = os.path.join(company_list_pages_product_dir_path, search_text + r'_' + search_type + r'_' +
                                       this_page_str + r'.html')

    if os.path.exists(this_page_html_path) and os.path.getsize(this_page_html_path) >= 125 * 1024:
        print('page:' + this_page_str + '-------- exists')
    else:

        while True:
            result = while_session_get(url)

            if len(result.content) < 5 * 1024:
                continue
            else:
                break

        # result = while_session_get(url)

        with open(this_page_html_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)

        min_file_size = 125 * 1024
        if os.path.getsize(this_page_html_path) < min_file_size:
            print('page:' + this_page_str + '-------- this page html size < ' + str(min_file_size/1024)
                  + 'k  !!!!!!!!!!')
        else:
            print('page:' + this_page_str + '-------- download OK')


def gevent_pool_requests(func, urls, gevent_pool_size=pool_size):
    """
    多协程请求
    :param func:
    :param urls:
    :param gevent_pool_size:
    :return:
    """
    gevent_pool = pool.Pool(gevent_pool_size)
    result_list = gevent_pool.map(func, urls)
    return result_list


def download_all_company_list_htmls(while_times=3):
    """
    下载所有的公司列表页
    :return:
    """
    url_page_postfix = r'/searchCompanies/scroll?tab=cmp&pageNbre='
    urls = [home_url + url_page_postfix + str(i) for i in range(1, page_total + 1)]

    for i in range(while_times):
        print('第' + str(i + 1) + '轮爬取')
        gevent_pool_requests(download_this_page_company_list, urls)


def download_this_page_company_detail(url):
    """
    下载当前页公司详情页
    :param url:
    :return:
    """
    try:
        # print(url)
        if url is None:
            print('url is None')
            return

        url_split_list = str(url).split(r'/')
        company_id_str = url_split_list[-2]
        company_name_str = url_split_list[-3]
        company_detail_name = company_id_str + r'_' + company_name_str + r'_c' + r'.html'
        company_detail_path = os.path.join(company_detail_product_dir_path, company_detail_name)

        min_file_size = 106 * 1024
        error_file_size = 5 * 1024
        # min_file_size = error_file_size  # 5k以下才请求数据（按需放开条件）

        file_is_exists = os.path.exists(company_detail_path)

        if file_is_exists and os.path.getsize(company_detail_path) > min_file_size:
            # print('page:' + company_detail_name + '-------- exists')
            print('file exists -- big file')

        elif file_is_exists and detail_page_is_success(company_detail_path):
            # print('page:' + company_detail_name + '-------- exists')
            print('file exists -- small file')

        else:
            result = while_session_get(url, new_sess=True)

            if len(result.text) > error_file_size:
                with open(company_detail_path, 'w', encoding='utf8') as fp:
                    fp.write(result.text)

                if os.path.getsize(company_detail_path) < min_file_size:
                    # print('page:' + company_detail_name + '-------- this page html size < ' + str(min_file_size/1024)
                    #       + 'k  !!!!!!!!!!')
                    if detail_page_is_success(company_detail_path):
                        print('page:' + company_detail_name + '-------- download OK')
                    else:
                        return url
                else:
                    print('page:' + company_detail_name + '-------- download OK')
            else:
                return url
    except Exception as e:
        print(e)


def download_all_company_detail_htmls(while_times=3):
    """
    下载所有的公司详情页
    while_times：采集轮次数
    :return:
    """

    all_company_detail_urls_list = list()

    if os.path.exists(company_urls_list_json_path):
        with open(company_urls_list_json_path, 'r', encoding='utf8') as fp:
            all_company_detail_urls_list = json.loads(fp.read())
    else:
        count = 0
        pages = 0

        for item_file_name in os.listdir(company_list_pages_product_dir_path):
            # 单个公司列表页面

            file_path = os.path.join(company_list_pages_product_dir_path, item_file_name)

            if file_path == first_html_path:
                print('跳过页码页：', file_path)
                continue

            company_detail_urls = get_company_detail_urls_by_company_list_page(file_path)

            pages += 1
            count += len(company_detail_urls)

            all_company_detail_urls_list.extend(company_detail_urls)
            print('\r正在收集公司详情页url,pages:' + str(pages) + ',count:' + str(count), end='')

        print('\n', pages, count)

        with open(company_urls_list_json_path, 'w', encoding='utf8') as fp:
            fp.write(json.dumps(all_company_detail_urls_list))

    retry_urls_list = list()
    for i in range(while_times):
        if i == 0:
            retry_urls_list = all_company_detail_urls_list
        print('第' + str(i + 1) + '轮爬取, len(urls) = ' + str(len(retry_urls_list)))
        retry_urls_list = gevent_pool_requests(download_this_page_company_detail, retry_urls_list)
        retry_urls_list = [i for i in retry_urls_list if i]


def detail_page_is_success(company_detail_path):
    """
    详情页是否为成功页
    :return:
    """
    with open(company_detail_path, 'r', encoding='utf8') as fp:
        content = fp.read()

    seletor = etree.HTML(content)
    company_name_text = seletor.xpath('//div[@class="headerDetailsCompany"]//h1[@itemprop="name"]/text()')
    company_name = get_selector_text_string(company_name_text)
    return company_name != ''


def get_company_detail_urls_by_company_list_page(company_list_page_path):
    """
    通过公司列表页获取该页中所有跳转公司详情的url
    :param company_list_page_path:
    :return:
    """
    with open(company_list_page_path, 'r', encoding='utf8') as fp:
        content = fp.read()

    selector = etree.HTML(content)

    this_page_company_detail_urls = selector.xpath('//div[@class="infos"]/div[@class="details"]/h2/a/@href')

    this_page_company_detail_urls = [home_url + str(i).replace('\n', '') for i in this_page_company_detail_urls
                                     if str(i).startswith(r'/c/')]

    return this_page_company_detail_urls


def check_company_detail_keyword():
    """
    检查公司详情页各个关键字
    :return:
    """
    all_main_keyword_dict = dict()

    all_company_other_contact_span = dict()
    all_company_presentation = dict()
    all_company_keynumbers = dict()
    all_company_executives = dict()
    all_company_activities = dict()

    for file_name in os.listdir(company_detail_product_dir_path):
        _file_name = file_name.replace('.html', '')
        _company_id_str = file_name.split('_')[0]

        file_path = os.path.join(company_detail_product_dir_path, file_name)

        with open(file_path, 'r', encoding='utf8') as fp:
            content = fp.read()

        selector = etree.HTML(content)

        # 公司head信息
        # company_name = selector.xpath('//div[@class="headerDetailsCompany"]//h1[@itemprop="name"]/text()')
        # _company_name = get_selector_text_string(company_name)
        #
        # company_is_premium = selector.xpath('//div[@class="headerDetailsCompany"]/a/span/text()')
        # _company_is_premium = True if get_selector_text_string(company_is_premium) != '' else False
        #
        # company_street_address = selector.xpath('//div[@class="headerDetailsCompany"]'
        #                                         '//div[@class="addressCoordinates"]/p'
        #                                         '/span[@itemprop="streetAddress"]/text()')
        # _company_street_address = get_selector_text_string(company_street_address)
        #
        # company_city_address = selector.xpath('//div[@class="headerDetailsCompany"]'
        #                                       '//div[@class="addressCoordinates"]/p/text()')
        # _company_city_address = get_selector_text_string(company_city_address, index=1)
        #
        # company_country_address = selector.xpath('//div[@class="headerDetailsCompany"]'
        #                                          '//div[@class="addressCoordinates"]/p'
        #                                          '/span[@itemprop="addressCountry"]/text()')
        # _company_country_address = get_selector_text_string(company_country_address)
        #
        # company_phone = selector.xpath('//div[@class="headerDetailsCompany"]'
        #                                '//div[@class="contactButton"]/a/input/@value')
        # _company_phone = get_selector_text_string(company_phone)

        # 其他联系信息
        # company_other_contact_span = selector.xpath('//div[@class="headerDetailsCompany"]'
        #                                             '//div[@class="headerRowCoordinates"]'
        #                                             '//div[@class="coordinate-item"]/a/span')

        company_presentation = selector.xpath('//div[@class="tab-content"]/div[@id="presentation"]'
                                              '/div[contains(@class,"item")]/div')

        company_keynumbers = selector.xpath('//div[@class="tab-content"]/div[@id="keynumbers"]'
                                            '/div[contains(@class,"item")]/div')

        # company_executives = selector.xpath('//div[@class="tab-content"]/div[@id="executives"]'
        #                                     '/div[@class="TabContacts"]/div[contains(@class,"item")]'
        #                                     '/div')
        company_executives = selector.xpath('//div[@class="tab-content"]/div[@id="executives"]'
                                            '/div[@class="TabContacts"]/div')

        # company_activities = selector.xpath('//div[@class="tab-content"]/div[@id="activities"]'
        #                                     '/div[@class="TabProducts"]/div[contains(@class,"item")]')
        company_activities = selector.xpath('//div[@class="tab-content"]/div[@id="activities"]'
                                            '/div[@class="TabProducts"]/div')

        print(_file_name)
        # print(_company_id_str)
        # print(_company_name)
        # print(_company_is_premium)
        # print(_company_street_address)
        # print(_company_city_address)
        # print(_company_country_address)
        # print(_company_phone)

        # temp_company_other_contact_span = dict([(str(item_span.text).replace('\xa0', ' ').strip(), etree.tostring(item_span.getparent())) for item_span in company_other_contact_span])
        temp_company_presentation = dict([('Company Summary' if str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip().startswith('Company Summary') else str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), file_name) for item in company_presentation])
        temp_company_keynumbers = dict([(str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), file_name) for item in company_keynumbers])
        temp_company_executives = dict([(str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), file_name) for item in company_executives])
        temp_company_activities = dict([(str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), file_name) for item in company_activities])

        # for temp_key in temp_company_other_contact_span:
        #     if temp_key not in all_company_other_contact_span:
        #         all_company_other_contact_span[temp_key] = file_name

        for temp_key, temp_value in temp_company_presentation.items():
            if temp_key not in all_company_presentation:
                all_company_presentation[temp_key] = temp_value

        for temp_key, temp_value in temp_company_keynumbers.items():
            if temp_key not in all_company_keynumbers:
                all_company_keynumbers[temp_key] = temp_value

        for temp_key, temp_value in temp_company_executives.items():
            if temp_key not in all_company_executives:
                all_company_executives[temp_key] = temp_value

        for temp_key, temp_value in temp_company_activities.items():
            if temp_key not in all_company_activities:
                all_company_activities[temp_key] = temp_value

        # break

    # all_main_keyword_dict['company_other_contact_span'] = all_company_other_contact_span
    all_main_keyword_dict['company_presentation'] = all_company_presentation
    all_main_keyword_dict['company_keynumbers'] = all_company_keynumbers
    all_main_keyword_dict['company_executives'] = all_company_executives
    all_main_keyword_dict['company_activities'] = all_company_activities

    with open(company_detail_main_keyword_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(all_main_keyword_dict))


def get_selector_text_string(selector_text_list, index=0):
    """
    获取选择器text中字符串
    :param selector_text_list: 选择出的text列表
    :param index: 需要选择的索引
    :return:
    """
    return str(selector_text_list[index]).strip() if len(selector_text_list) > index else ''


def parse_all_company_detail():
    """
    解析所有的公司详情页
    :return:
    """
    debug = False
    debug_size = 10

    company_list = list()
    for file_name in os.listdir(company_detail_product_dir_path):
        print('\r' + file_name, end='')
        try:
            _file_name = file_name.replace('.html', '')
            _company_id_str = file_name.split('_')[0]

            file_path = os.path.join(company_detail_product_dir_path, file_name)

            # # 测试代码
            # if debug:
            #     file_path = r'E:/work_all/topease/company_spider_9/company_detail_dir/pump/' \
            #                 r'ae200013_al-sagar-engineering-company-llc_c.html'

            with open(file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)

            # 公司国家缩写、公司定位信息
            company_country_short = selector.xpath('//input[@id="company-country"]/@value')
            _company_country_short = get_selector_text_string(company_country_short)

            company_latitude = selector.xpath('//input[@id="company-latitude"]/@value')
            _company_latitude = get_selector_text_string(company_latitude)

            company_longitude = selector.xpath('//input[@id="company-longitude"]/@value')
            _company_longitude = get_selector_text_string(company_longitude)

            # 公司head信息
            company_name = selector.xpath('//div[@class="headerDetailsCompany"]//h1[@itemprop="name"]/text()')
            _company_name = get_selector_text_string(company_name)

            company_is_premium = selector.xpath('//div[@class="headerDetailsCompany"]/a/span/text()')
            _company_is_premium = 1 if get_selector_text_string(company_is_premium) != '' else 0

            company_all_address = selector.xpath('//div[@class="headerDetailsCompany"]'
                                                 '//div[@class="addressCoordinates"]/p')
            _company_all_address_node = company_all_address[0] if len(company_all_address) > 0 else None
            _company_all_address = clean_spaces(_company_all_address_node.xpath('string()')) if _company_all_address_node is not None else ''

            company_street_address = selector.xpath('//div[@class="headerDetailsCompany"]'
                                                    '//div[@class="addressCoordinates"]/p'
                                                    '/span[@itemprop="streetAddress"]')
            _company_street_address_node = company_street_address[0] if len(company_street_address) > 0 else None
            _company_street_address = clean_spaces(_company_street_address_node.xpath('string()')) if _company_street_address_node is not None else ''

            company_city_address = selector.xpath('//div[@class="headerDetailsCompany"]'
                                                  '//div[@class="addressCoordinates"]/p/text()')
            _company_city_address = get_selector_text_string(company_city_address, index=1)

            company_country_address = selector.xpath('//div[@class="headerDetailsCompany"]'
                                                     '//div[@class="addressCoordinates"]/p'
                                                     '/span[@itemprop="addressCountry"]/text()')
            _company_country_address = get_selector_text_string(company_country_address)

            company_phone = selector.xpath('//div[@class="headerDetailsCompany"]'
                                           '//div[@class="contactButton"]/a/input/@value')
            _company_phone = get_selector_text_string(company_phone)

            # 其他联系信息
            company_website = selector.xpath('//div[@class="headerDetailsCompany"]'
                                             '//div[@class="headerRowCoordinates"]'
                                             '//div[@class="coordinate-item"]/a[@id="website"]/@href')

            _company_website = get_selector_text_string(company_website)

            #
            # 公司内部信息

            company_presentation = selector.xpath('//div[@class="tab-content"]/div[@id="presentation"]'
                                                  '/div[contains(@class,"item")]/div')

            company_keynumbers = selector.xpath('//div[@class="tab-content"]/div[@id="keynumbers"]'
                                                '/div[contains(@class,"item")]/div')

            # company_executives = selector.xpath('//div[@class="tab-content"]/div[@id="executives"]'
            #                                     '/div[@class="TabContacts"]/div[contains(@class,"item")]'
            #                                     '/div')
            company_executives = selector.xpath('//div[@class="tab-content"]/div[@id="executives"]'
                                                '/div[@class="TabContacts"]/div')

            # company_activities = selector.xpath('//div[@class="tab-content"]/div[@id="activities"]'
            #                                     '/div[@class="TabProducts"]/div[contains(@class,"item")]')
            company_activities = selector.xpath('//div[@class="tab-content"]/div[@id="activities"]'
                                                '/div[@class="TabProducts"]/div')

            # print('===========================')

            # print(_file_name)
            # print(_company_id_str)
            # print(_company_name)
            # print(_company_is_premium)
            # print(_company_all_address)
            # print(_company_street_address)
            # print(_company_city_address)
            # print(_company_country_address)
            # print(_company_phone)
            # print(_company_website)
            # print(_company_country_short)
            # print(_company_latitude)
            # print(_company_longitude)

            temp_company_presentation = dict([('Company Summary' if str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip().startswith('Company Summary') else str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), [item.xpath('./div')[0], file_name]) for item in company_presentation])
            temp_company_keynumbers = dict([(str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), [item.xpath('./div')[0], file_name]) for item in company_keynumbers])
            temp_company_executives = dict([(str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), [item.xpath('./div[@class="presentation"]')[0], file_name]) for item in company_executives])
            temp_company_activities = dict([(str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), [item.xpath('./div')[0], file_name]) for item in company_activities])

            # print(temp_company_presentation)
            # print(temp_company_keynumbers)
            # print(temp_company_executives)
            # print(temp_company_activities)

            # print('------- presentation --------')
            presentation_dict = dict()
            for _key, _value in temp_company_presentation.items():
                tag, v = parse_item_div(_key, _value[0])
                if tag is not None:
                    presentation_dict[tag] = v
            # print(presentation_dict)

            # print('------- keynumbers --------')
            keynumbers_dict = dict()
            for _key, _value in temp_company_keynumbers.items():
                tag, v = parse_item_div(_key, _value[0])
                if tag is not None:
                    keynumbers_dict[tag] = v
            # print(keynumbers_dict)

            # print('------- executives -------')
            executives_dict = dict()
            for _key, _value in temp_company_executives.items():
                tag, v = parse_item_div(_key, _value[0])
                if tag is not None:
                    executives_dict[tag] = v
            # print(executives_dict)

            # print('------- activities -------')
            activities_dict = dict()
            for _key, _value in temp_company_activities.items():
                tag, v = parse_item_div(_key, _value[0])
                if tag is not None:
                    activities_dict[tag] = v
            # print(activities_dict)

            company_dict = {
                'file_name': _file_name,
                'company_id_str': _company_id_str,
                'company_name': _company_name,
                'company_is_premium': _company_is_premium,
                'company_country_short': _company_country_short,
                'company_latitude': _company_latitude,
                'company_longitude': _company_longitude,
                'company_all_address': _company_all_address,
                'company_street_address': _company_street_address,
                'company_city_address': _company_city_address,
                'company_country_address': _company_country_address,
                'company_phone': _company_phone,
                'company_website': _company_website,
                'company_presentation': presentation_dict,
                'company_keynumbers': keynumbers_dict,
                'company_executives': executives_dict,
                'company_activities': activities_dict
            }

            company_list.append(company_dict)

            # print(json.dumps(company_dict))

        except Exception as e:
            print(traceback.format_exc())
            if debug:
                raise e

        if debug:
            if len(company_list) > debug_size:
                break

    print()
    print(len(company_list))

    with open(company_list_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(company_list))


def parse_item_div(tag, item_div):
    """
    解析单条目div的路由
    :param tag: 当前html节点的标记
    :param item_div: 当前html节点
    :return:
    """
    # presentation
    if tag == 'Company Summary' or tag == 'Presentation':
        return 'company_summary', clean_wrong_charter(clean_spaces(item_div.xpath('./span')[0].xpath('string()') if len(item_div.xpath('./span')) > 0 else '', '\n'))

    elif tag == 'General Information':
        td_list = item_div.xpath('.//tr/td')
        td_text_list = [','.join(td_node.xpath('./text()')) if len(list(td_node)) == 0 else ','.join(td_node.xpath('./a/text()')) for td_node in td_list]
        td_text_list = [str(i).strip() for i in td_text_list]
        key_list = [td_text_item for idx, td_text_item in enumerate(td_text_list) if idx & 1 == 0]
        value_list = [td_text_item for idx, td_text_item in enumerate(td_text_list) if idx & 1 == 1]

        if len(key_list) != len(value_list):
            raise Exception(tag + ': len(key_list) != len(value_list)')

        temp_dict = dict()
        for _k, _v in zip(key_list, value_list):
            _k = clean_spaces(clean_wrong_charter(_k))
            _v = clean_spaces(clean_wrong_charter(_v))
            temp_dict[_k] = (temp_dict.get(_k, '') + ',' + _v) if temp_dict.get(_k, '') != '' else _v
        return 'company_general_info', temp_dict

    elif tag == 'Banks':
        li_list = item_div.xpath('./ul/li')
        li_text_list = [str(li.xpath('string()')).strip() for li in li_list]
        return 'Banks', li_text_list

    elif tag == 'Export' or tag == 'Import':
        span_list = item_div.xpath('./div/span')
        span_text_list = [str(span.xpath('string()')).strip() for span in span_list]
        key_list = [span_text_item.rstrip(' :') for idx, span_text_item in enumerate(span_text_list) if idx & 1 == 0]
        value_list = [span_text_item for idx, span_text_item in enumerate(span_text_list) if idx & 1 == 1]

        if len(key_list) != len(value_list):
            raise Exception(tag + ': len(key_list) != len(value_list)')

        temp_dict = dict()
        for _k, _v in zip(key_list, value_list):
            _k = clean_spaces(clean_wrong_charter(_k))
            _v = clean_spaces(clean_wrong_charter(_v))
            temp_dict[_k] = (temp_dict.get(_k, '') + ',' + _v) if temp_dict.get(_k, '') != '' else _v
        return tag, temp_dict

    elif tag == 'Certifications':
        p_list = item_div.xpath('./ul/li/p')
        p_text_list = [str(p.xpath('string()')).strip() for p in p_list]
        key_list = [p_text_item for idx, p_text_item in enumerate(p_text_list) if idx & 1 == 0]
        value_list = [p_text_item for idx, p_text_item in enumerate(p_text_list) if idx & 1 == 1]

        if len(key_list) != len(value_list):
            raise Exception(tag + ': len(key_list) != len(value_list)')

        temp_list = list()
        for _k, _v in zip(key_list, value_list):
            _k = clean_spaces(clean_wrong_charter(_k))
            _v = clean_spaces(clean_wrong_charter(_v))
            if _k != '':
                temp_dict = dict()
                temp_dict[_k] = _v
                temp_list.append(temp_dict)

        return 'Certifications', temp_list

    elif tag == 'Brands':
        li_list = item_div.xpath('./ul/li')
        li_text_list = [clean_spaces(li.xpath('string()')) for li in li_list]
        return 'Brands', li_text_list

    elif tag == 'Associations':
        strong_text_list = item_div.xpath('./ul/li')
        strong_text_list = [str(li.xpath('string()')).strip() for li in strong_text_list]
        return 'Associations', strong_text_list

    elif tag == 'Products':
        a_list = item_div.xpath('./div[contains(@class,"products")]/div/div[contains(@class,"product")]/ul/li/div[contains(@class,"product_content")]/a')
        temp_list = list()
        for a_node in a_list:
            product_desc = ','.join(a_node.xpath('./p/@title'))
            temp_list.append(product_desc)
        return 'Products', temp_list

    elif tag == 'Company catalogues':
        catalog_href_list = item_div.xpath('.//ul/li/a/@href')
        return 'Company catalogues', catalog_href_list

    # keynumbers
    elif tag == 'Employees' or tag == 'Turnover':
        p_list = item_div.xpath('./ul/li/p')
        p_text_list = [str(p.xpath('string()')).strip() for p in p_list]
        key_list = [p_text_item for idx, p_text_item in enumerate(p_text_list) if idx & 1 == 0]
        value_list = [p_text_item for idx, p_text_item in enumerate(p_text_list) if idx & 1 == 1]

        if len(key_list) != len(value_list):
            raise Exception(tag + ': len(key_list) != len(value_list)')

        temp_list = list()
        for _k, _v in zip(key_list, value_list):
            _k = clean_spaces(clean_wrong_charter(_k))
            _v = clean_spaces(clean_wrong_charter(_v))
            if _k != '':
                temp_dict = dict()
                temp_dict[_k] = _v
                temp_list.append(temp_dict)

        return tag, temp_list

    # executives
    elif tag == 'Executive information':
        li_div_list = item_div.xpath('./ul/li/div')
        temp_list = list()
        for li_div_node in li_div_list:
            p_list = li_div_node.xpath('./div/p')
            p_text_list = [str(p.xpath('string()')).strip() for p in p_list]
            if len(p_text_list) == 2:
                name = clean_wrong_charter(clean_spaces(p_text_list[0]))
                job = clean_wrong_charter(clean_spaces(p_text_list[1]))
                temp_dict = {
                    'name': name,
                    'job': job
                }
                temp_list.append(temp_dict)
        return 'Executive information', temp_list

    # activities
    elif tag == 'Activities' or tag == 'Main activities' or tag == 'Secondary activities':
        li_list = item_div.xpath('./div[@class="jstree-classic"]/ul/li')
        temp_list = list()
        for item_li in li_list:
            li_a_text_list = item_li.xpath('./a/text()')
            li_a_text = clean_wrong_charter(clean_spaces(''.join(li_a_text_list)))

            li_ul_li_a_text_list = item_li.xpath('./ul/li/a/text()')
            li_ul_li_a_text_list = [clean_wrong_charter(clean_spaces(i)) for i in li_ul_li_a_text_list]

            temp_dict = {
                li_a_text: li_ul_li_a_text_list
            }
            temp_list.append(temp_dict)

        tag = 'Main activities' if tag == 'Activities' else tag
        return tag, temp_list

    elif tag == 'Other classifications (for some countries)':
        p_list = item_div.xpath('./div/p')
        if len(p_list) > 0:
            p_node = p_list[0]
            key_list = p_node.xpath('./strong/text()')
            value_list = p_node.xpath('./span/text()')

            temp_list = list()
            for _k, _v in zip(key_list, value_list):
                _k = clean_spaces(clean_wrong_charter(str(_k).rstrip(' :')))
                _v = clean_spaces(clean_wrong_charter(_v))
                if _k != '':
                    temp_dict = dict()
                    temp_dict[_k] = _v
                    temp_list.append(temp_dict)
        return 'Other classifications', temp_list

    # [other]
    else:
        print()
        print('this key is no exists : ', tag)
        print('-----------------------------')
        return None, tag


def clean_spaces(str_text, replace_str=' '):
    """
    清楚字符串中连续的空格
    :param str_text:
    :param replace_str:
    :return:
    """
    return re.sub(r'\s{2,}', replace_str, str(str_text)).strip()


def clean_wrong_charter(str_text):
    """
    清楚错误字符
    :param str_text:
    :return:
    """
    return str(str_text).replace('\xa0', ' ').strip()


def read_product_company_list_json():
    """
    读取产品json
    :return:
    """
    with open(company_list_json_path, 'r', encoding='utf8') as fp:
        data = fp.read()
    return json.loads(data)


def get_company_list_json():
    """
    获取公司列表json
    :return:
    """
    data_json = read_product_company_list_json()
    data_json_len = len(data_json)
    print(data_json_len)
    print(data_json[:20] if data_json_len > 20 else data_json)


def start():
    """
    开始采集
    :return:
    """

    # # 1.下载公司列表页
    # download_frist_html()
    # parse_first_html()
    # # download_company_list_pages() # 废弃
    # download_all_company_list_htmls(while_times=50)

    # 2.下载公司详情页
    download_all_company_detail_htmls(while_times=50)

    # 3.解析公司详情页（先检查含有字段）
    # check_company_detail_keyword()
    # parse_all_company_detail()

    # 4.读取该产品的公司列表json（查看）
    # get_company_list_json()


if __name__ == '__main__':
    start()
