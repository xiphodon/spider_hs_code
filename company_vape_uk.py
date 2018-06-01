#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/1 15:43
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : company_vape_uk.py
# @Software: PyCharm

# 电子烟、烟油    英国
import rakuten_spider_7
import os
from lxml import etree
import json

target_url = r'https://www.gov.uk/government/publications/tobacco-products-and-e-cigarette-cross-border-sales-registration/list-of-registered-retailers#e-cigarette-retailers'

home_path = r'E:\work_all\topease\company_vape_uk'

gov_uk_vape_html_path = os.path.join(home_path, 'gov_uk_vape.html')
gov_uk_vape_json_path = os.path.join(home_path, 'gov_uk_vape.json')


def down_load_gov_uk_vape_html():
    """
    下载英国政府烟油（相关）页面
    :return:
    """
    result = rakuten_spider_7.while_requests_get(target_url)
    with open(gov_uk_vape_html_path, 'w', encoding='utf8') as fp:
        fp.write(result.text)


def read_gov_uk_vape_file():
    """
    读取英国政府烟油（相关）文件
    :return:
    """
    if os.path.exists(gov_uk_vape_html_path):
        with open(gov_uk_vape_html_path, 'r', encoding='utf8') as fp:
            content = fp.read()
            return content
    else:
        raise IOError('file is not exists')


def parse_gov_uk_vape_file():
    """
    解析英国政府烟油（相关）文件
    :return:
    """
    gov_uk_vape_company_list = list()

    content = read_gov_uk_vape_file()

    seletor = etree.HTML(content)
    table_title_list = seletor.xpath('//div[@class="govspeak"]/h2')
    table_list = seletor.xpath('//div[@class="govspeak"]/table')

    table_head_list = None
    for item_title, item_table in zip(table_title_list, table_list):
        # 单个表格
        title_text = item_title.xpath('./text()')[0]
        # print(title_text)

        table_tr = item_table.xpath('.//tr')
        for index, item_table_tr in enumerate(table_tr):
            # 表格单行

            if index == 0:
                # th
                if table_head_list is None:
                    table_head_list = item_table_tr.xpath('./th/text()')
            else:
                # td
                table_content = item_table_tr.xpath('./td')

                temp_dict = dict()

                for idx, item_table_tr_td in enumerate(table_content):
                    # 表格该行单列
                    if table_head_list[idx] == 'Website':
                        item_table_tr_td_text = ','.join(item_table_tr_td.xpath('./a/text()'))
                    else:
                        item_table_tr_td_text = item_table_tr_td.xpath('string()').strip()

                    temp_dict[table_head_list[idx]] = item_table_tr_td_text
                    # print(item_table_tr_td_text)

                temp_dict['company_type'] = title_text
                # print(temp_dict)
                gov_uk_vape_company_list.append(temp_dict)
            # print()
        # print('==========')
    print(len(gov_uk_vape_company_list))

    with open(gov_uk_vape_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(gov_uk_vape_company_list))


def start():
    """
    入口
    :return:
    """
    # down_load_gov_uk_vape_html()
    parse_gov_uk_vape_file()


if __name__ == '__main__':
    start()
