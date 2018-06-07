#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/5/15 14:43
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_spider_9.py
# @Software: PyCharm

from gevent import pool, monkey
monkey.patch_all()
import requests
import os
from lxml import etree
import settings
import json
import time
import traceback

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}

search_text_list = ['pump', 'fabric', 'glass']
search_text = search_text_list[0]

search_type_dict = {'product': 'PRODUCT',
                    'supplier': 'SUPPLIER'}
search_type = search_type_dict['supplier']

home_url = settings.company_home_url_1
search_url = home_url + r'/searchCompanies?' \
             r'acClassif=&localizationCode=&localizationLabel=&localizationType=&text=' + search_text + \
             r'&searchType=' + search_type

home_path = r'E:\work_all\topease\company_spider_9'

json_dir_path = os.path.join(home_path, 'json_dir')
company_detail_main_keyword_json_path = os.path.join(json_dir_path, 'main_keyword.json')

company_list_pages_dir_path = os.path.join(home_path, 'company_list_dir')
company_list_pages_product_dir_path = os.path.join(company_list_pages_dir_path, search_text)

company_detail_dir_path = os.path.join(home_path, 'company_detail_dir')
company_detail_product_dir_path = os.path.join(company_detail_dir_path, search_text)

first_html_path = os.path.join(company_list_pages_product_dir_path, search_text + r'_' + search_type + r'_0.html')


pool_size = 1500
page_total = -1
overwrite = False

sess = requests.session()

if not os.path.exists(company_list_pages_product_dir_path):
    os.mkdir(company_list_pages_product_dir_path)

if not os.path.exists(company_detail_product_dir_path):
    os.mkdir(company_detail_product_dir_path)


def while_session_get(page_url, times=5000):
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            result = sess.get(page_url, headers=headers, timeout=30)
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


def download_frist_html():
    """
    下载首次请求的html页面
    :return:
    """
    result = while_session_get(search_url)

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
        result = while_session_get(url)

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
        url_split_list = str(url).split(r'/')
        company_id_str = url_split_list[-2]
        company_name_str = url_split_list[-3]
        company_detail_name = company_id_str + r'_' + company_name_str + r'_c' + r'.html'
        company_detail_path = os.path.join(company_detail_product_dir_path, company_detail_name)

        min_file_size = 106 * 1024
        if os.path.exists(company_detail_path) and os.path.getsize(company_detail_path) > min_file_size:
            print('page:' + company_detail_name + '-------- exists')
        else:
            result = while_session_get(url)

            with open(company_detail_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)

            if os.path.getsize(company_detail_path) < min_file_size:
                print('page:' + company_detail_name + '-------- this page html size < ' + str(min_file_size/1024)
                      + 'k  !!!!!!!!!!')
            else:
                print('page:' + company_detail_name + '-------- download OK')
    except Exception as e:
        print(e)


def download_all_company_detail_htmls(while_times=3):
    """
    下载所有的公司详情页
    while_times：采集轮次数
    :return:
    """
    count = 0
    pages = 0
    all_company_detail_urls_list = list()
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

    for i in range(while_times):
        print('第' + str(i + 1) + '轮爬取')
        gevent_pool_requests(download_this_page_company_detail, all_company_detail_urls_list)


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
        # _company_is_premium = True if get_selector_text_string(company_is_premium) != 'none' else False
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
    return str(selector_text_list[index]).strip() if len(selector_text_list) > index else 'none'


def parse_all_company_detail():
    """
    解析所有的公司详情页
    :return:
    """
    for file_name in os.listdir(company_detail_product_dir_path):
        try:
            _file_name = file_name.replace('.html', '')
            _company_id_str = file_name.split('_')[0]

            file_path = os.path.join(company_detail_product_dir_path, file_name)

            ## 测试代码
            file_path = r'E:/work_all/topease/company_spider_9/company_detail_dir/fabric/ae203629_new-medical-centre-trading-llc_c.html'
            ##

            with open(file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)

            # 公司head信息
            company_name = selector.xpath('//div[@class="headerDetailsCompany"]//h1[@itemprop="name"]/text()')
            _company_name = get_selector_text_string(company_name)

            company_is_premium = selector.xpath('//div[@class="headerDetailsCompany"]/a/span/text()')
            _company_is_premium = True if get_selector_text_string(company_is_premium) != 'none' else False

            company_street_address = selector.xpath('//div[@class="headerDetailsCompany"]'
                                                    '//div[@class="addressCoordinates"]/p'
                                                    '/span[@itemprop="streetAddress"]/text()')
            _company_street_address = get_selector_text_string(company_street_address)

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

            print('===========================')

            print(_file_name)
            print(_company_id_str)
            print(_company_name)
            print(_company_is_premium)
            print(_company_street_address)
            print(_company_city_address)
            print(_company_country_address)
            print(_company_phone)
            print(_company_website)

            print('---------------------------')

            temp_company_presentation = dict([('Company Summary' if str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip().startswith('Company Summary') else str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), [item.xpath('./div')[0], file_name]) for item in company_presentation])
            temp_company_keynumbers = dict([(str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), [item.xpath('./div')[0], file_name]) for item in company_keynumbers])
            temp_company_executives = dict([(str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), [item.xpath('./div')[0], file_name]) for item in company_executives])
            temp_company_activities = dict([(str(item.xpath('./h3/text()')[0]).replace('\xa0', ' ').strip(), [item.xpath('./div')[0], file_name]) for item in company_activities])

            # print(temp_company_presentation)
            # print(temp_company_keynumbers)
            # print(temp_company_executives)
            # print(temp_company_activities)
            #
            # print('-----------------------------')

            print('------- presentation --------')
            for _key, _value in temp_company_presentation.items():
                print('$--$--$--$')
                print(_key, ':', _value)
                print(parse_item_div(_key, _value[0]))

            print('------- keynumbers --------')
            for _key, _value in temp_company_keynumbers.items():
                print(_key, ':', _value)

            print('------- executives -------')
            for _key, _value in temp_company_executives.items():
                print(_key, ':', _value)

            print('------- activities -------')
            for _key, _value in temp_company_activities.items():
                print(_key, ':', _value)

            print('-----------------------------')

        except Exception:
            print(traceback.format_exc())

        break


def parse_item_div(tag, item_div):
    """
    解析单条目div的路由
    :param tag: 当前html节点的标记
    :param item_div: 当前html节点
    :return:
    """
    # presentation
    if tag == 'Company Summary':
        return {'company_summary': (item_div.xpath('./span')[0].xpath('string()') if len(item_div.xpath('./span')) > 0 else 'none')}
    elif tag == 'General Information':
        td_list = item_div.xpath('.//tr/td')
        td_text_list = [','.join(td_node.xpath('./text()')) if len(list(td_node)) == 0 else ','.join(td_node.xpath('./a/text()')) for td_node in td_list]
        td_text_list = [str(i).strip() for i in td_text_list]
        key_list = [td_text_item for idx, td_text_item in enumerate(td_text_list) if idx & 1 == 0]
        value_list = [td_text_item for idx, td_text_item in enumerate(td_text_list) if idx & 1 == 1]
        temp_dict = dict()
        for _k, _v in zip(key_list, value_list):
            temp_dict[_k] = _v
        return {'company_general_info': temp_dict}
    elif tag == 'Banks':
        li_list = item_div.xpath('./ul/li')
        li_text_list = [str(li.xpath('string()')).strip() for li in li_list]
        return {'Banks': li_text_list}
    elif tag == 'Export':
        pass
    elif tag == 'Import':
        pass
    elif tag == 'Certifications':
        pass
    elif tag == 'Brands':
        pass
    elif tag == 'Associations':
        pass
    elif tag == 'Products':
        pass
    elif tag == 'Company catalogues':
        pass

    # keynumbers
    elif tag == 'Employees':
        pass
    elif tag == 'Turnover':
        pass

    # executives
    elif tag == 'Executive information':
        pass

    # activities
    elif tag == 'Activities':
        pass
    elif tag == 'Main activities':
        pass
    elif tag == 'Secondary activities':
        pass
    elif tag == 'Other classifications (for some countries)':
        pass

    # [other]
    else:
        return None


def start():
    """
    开始采集
    :return:
    """

    # # 1.下载公司列表页
    # download_frist_html()
    # parse_first_html()
    # # download_company_list_pages() # 废弃
    # download_all_company_list_htmls(while_times=5)

    # 2.下载公司详情页
    # download_all_company_detail_htmls(while_times=5)

    # 3.解析公司详情页
    # check_company_detail_keyword()
    parse_all_company_detail()


if __name__ == '__main__':
    start()
