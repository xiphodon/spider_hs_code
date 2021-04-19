#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/3/19 16:40
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : 912688_spider.py
# @Software: PyCharm
import hashlib
import json
import os
import pprint
import re
import time
from typing import Union

from lxml import etree

from WhileRequests import WhileRequests
from base_spider import BaseSpider


class SouHaoHuoSpider(BaseSpider):
    """
    搜好货爬虫
    """
    home_url = r'https://www.912688.com/'
    company_lib_url = r'https://www.912688.com/gongsi/'

    home_dir = r'E:\souhaohuo'
    home_page_path = os.path.join(home_dir, 'home.html')
    company_class_index_json_path = os.path.join(home_dir, 'company_class_index.json')

    company_list_pages_home_dir_path = os.path.join(home_dir, 'company_list_pages')
    company_list_info_json_path = os.path.join(home_dir, 'company_list_info.json')
    company_list_info_unique_json_path = os.path.join(home_dir, 'company_list_info_unique.json')

    company_pages_dir_path = os.path.join(home_dir, 'company_pages')
    company_pages_json_dir_path = os.path.join(home_dir, 'company_pages_json')
    company_info_json_dir = os.path.join(home_dir, 'company_info_json')

    def __init__(self, force_save=False):
        """
        初始化
        :param force_save:
        """
        self.force_save = force_save

    def download_company_lib_home_page(self):
        """
        下载公司库首页
        :return:
        """
        if os.path.exists(self.home_page_path):
            print('company lib home page already exists!')
            return
        requests = WhileRequests()
        result = requests.get(self.company_lib_url, request_times=3, sleep_time=1)
        result.encoding = 'utf8'
        with open(self.home_page_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)
        print('company lib home page download OK!')

    def parse_company_lib_home_page(self):
        """
        解析公司库首页
        :return:
        """
        assert os.path.exists(self.home_page_path), 'company lib home page is not exists!'

        if os.path.exists(self.company_class_index_json_path):
            print('company class index json already exists!')
            return

        with open(self.home_page_path, 'r', encoding='utf8') as fp:
            file_content = fp.read()

        index_list = []

        selector = etree.HTML(file_content)
        tab_div_list = selector.xpath('//div[@class="base-div jc"]')
        md5 = hashlib.md5()
        for tab_div in tab_div_list:
            tab_dl_list = tab_div.xpath('./dl')
            for tab_dl in tab_dl_list:
                class_1_text = tab_dl.xpath('.//h2/text()')[0]
                a_href_list = tab_dl.xpath('.//a/@href')
                class_2_text_list = tab_dl.xpath('.//a/text()')
                # print(class_1_text)
                # print(a_href_list)
                # print(class_2_text_list)
                for class_2_text, class_2_href in zip(class_2_text_list, a_href_list):
                    md5.update(str(class_2_href).encode(encoding='utf8'))
                    href_md5 = md5.hexdigest()
                    index_list.append({
                        'href_md5': href_md5,
                        'class_1': class_1_text,
                        'class_2': class_2_text,
                        'href': class_2_href
                    })
            # break
        with open(self.company_class_index_json_path, 'w', encoding='utf8') as fp:
            fp.write(json.dumps(index_list))
        print('company class index json build OK!')

    def download_company_list_page(self):
        """
        下载公司列表页
        :return:
        """
        assert os.path.exists(self.company_class_index_json_path), 'company class index json is not exists!'

        with open(self.company_class_index_json_path, 'r', encoding='utf8') as fp:
            company_class_index_list = json.load(fp)

        if not os.path.exists(self.company_list_pages_home_dir_path):
            os.mkdir(self.company_list_pages_home_dir_path)

        requests = WhileRequests()

        for company_class_index in company_class_index_list:
            print(company_class_index)

            company_class_dir_path = os.path.join(
                self.company_list_pages_home_dir_path,
                f"{company_class_index['class_1']}_{company_class_index['class_2']}".replace('/', '、')
            )
            if not os.path.exists(company_class_dir_path):
                os.mkdir(company_class_dir_path)

            over_file_path = os.path.join(company_class_dir_path, '_over.txt')
            if not self.force_save and os.path.exists(over_file_path):
                print('-- 发现采集完毕标记文件，跳过该分类 --')
                continue

            page_no = 1
            while True:
                # 采集公司列表
                href = f"{company_class_index['href'][:-5]}-mod-page{page_no}.html"
                print(f'request {company_class_dir_path} {href}')
                sleep_time = self.random_float(0.2, 0.4) if page_no != 1 else self.random_float(1, 1.2)
                result = requests.get(href, request_times=3, sleep_time=sleep_time)
                result.encoding = 'utf8'

                selector = etree.HTML(result.text)
                if page_no == 1 and not self.force_save:
                    page_size_str_list = selector.xpath('//div[@class="s-mod-page mb30"]/span[@class="total"]/text()')
                    if len(page_size_str_list) == 0:
                        print('该分类无内容')
                        self.create_file(over_file_path, '0')
                        break
                    page_size_str = page_size_str_list[0][1:-1]
                    page_size_int = int(page_size_str)
                    print(f'该类别总页码数：{page_size_int}')
                    if len(os.listdir(company_class_dir_path)) == page_size_int:
                        print('-- 该类别下所有页面已下载过--')
                        self.create_file(over_file_path, page_size_str)
                        break

                file_path = os.path.join(company_class_dir_path, f'{page_no:03}.html')
                self.create_file(file_path, result.text)

                next_a = selector.xpath('//a[@class="page_next page-n" and @data-b="page"]')
                if len(next_a) > 0:
                    page_no += 1
                else:
                    # 页面已遍历完毕，增加标记文件
                    self.create_file(over_file_path, str(page_no))
                    break
            # break

    def company_list_page_stat(self):
        """
        公司列表页面统计
        :return:
        """
        all_list_count = 0
        for dir_name in os.listdir(self.company_list_pages_home_dir_path):
            dir_path = os.path.join(self.company_list_pages_home_dir_path, dir_name)
            current_dir_list_count = 0
            for file_name in os.listdir(dir_path):
                if not file_name.startswith('_'):
                    current_dir_list_count += 1
                    all_list_count += 1
            print(f'{dir_name} 列表共计 {current_dir_list_count}页')
        print(f'--- 所有列表共计 {all_list_count} 页 ---')

    def parse_company_list_page_to_json(self):
        """
        解析公司列表页面，并生成json文件
        :return:
        """
        for dir_name in os.listdir(self.company_list_pages_home_dir_path):
            dir_path = os.path.join(self.company_list_pages_home_dir_path, dir_name)
            json_file_path = os.path.join(dir_path, '_company_info_list.json')
            if os.path.exists(json_file_path):
                # 该目录已解析过，跳过
                print(f'### {dir_name} 已解析')
                print('-----------------------------------------------------')

                continue

            company_info_list = list()
            for file_name in os.listdir(dir_path):
                if file_name.startswith('_'):
                    # 跳过特殊文件
                    continue
                print(f'### {dir_name} {file_name} 正在解析...')
                print('-----------------------------------------------------')

                file_path = os.path.join(dir_path, file_name)
                with open(file_path, 'r', encoding='utf8') as fp:
                    content = fp.read()

                selector = etree.HTML(content)
                company_intorduce_div_list = selector.xpath('.//div[@class="company-introduce"]')
                for item in company_intorduce_div_list:
                    item_company_dict = dict()
                    item_company_dict['c_name'] = self.data_list_get_first(
                        item.xpath('.//span[@class="redTitle"]/text()'))
                    item_company_dict['c_tags'] = item.xpath('.//em/@title')
                    company_li_list = item.xpath('.//ul[@class="company-i-detail"]/li')
                    for li in company_li_list:
                        key = re.sub(r'\s+', '', self.data_list_get_first(li.xpath('./text()'))).strip('：')
                        value_span_list = li.xpath('./span')
                        if len(value_span_list) > 0:
                            value_span = value_span_list[0]
                            value_text = re.sub(r'\s{2,}', ' ', value_span.xpath('string(.)')).strip()
                            value_title = self.data_list_get_first(value_span.xpath('./@title')).strip()
                            if key == '主营产品':
                                value_text = value_text.split(' ') if value_text else ''
                                value_title = value_title.split(' ') if value_title else ''
                            value = value_title if value_title else value_text
                        else:
                            value = ''
                        # print(key, value)
                        # print(f'|{key}|_|{value}|')
                        item_company_dict.setdefault('c_intro', dict())[key] = value

                    item_company_dict['c_url'] = self.data_list_get_first(
                        item.xpath('.//a[@class="list-item-button L mr15"]/@href'))
                    item_company_dict['c_contact_url'] = self.data_list_get_first(
                        item.xpath('.//a[@class="list-item-button list-item-see L"]/@href'))

                    # pprint.pprint(item_company_dict)
                    for v in item_company_dict.values():
                        print(v)
                    print('-----------------------------------------------------')
                    company_info_list.append(item_company_dict)

                print(f'### {dir_name} {file_name} 解析完成')
                print('-----------------------------------------------------')
                # break

            with open(json_file_path, 'w', encoding='utf8') as fp:
                json.dump(company_info_list, fp)
            # break

        print('### 全部解析完成 ###')

    def merge_company_list_json(self):
        """
        合并公司列表json数据
        :return:
        """
        if os.path.exists(self.company_list_info_json_path):
            print('数据文件已存在')
            return

        company_list_info_list = list()
        for dir_name in os.listdir(self.company_list_pages_home_dir_path):
            dir_path = os.path.join(self.company_list_pages_home_dir_path, dir_name)
            json_file_path = os.path.join(dir_path, '_company_info_list.json')
            if not os.path.exists(json_file_path):
                continue

            print(f'{dir_name} 数据合并中...')
            with open(json_file_path, 'r', encoding='utf8') as fp:
                company_list_info_shunk_list = json.load(fp)
            for item in company_list_info_shunk_list:
                item['class'] = dir_name
                company_list_info_list.append(item)
            print(f'{dir_name} 数据合并完成 {len(company_list_info_shunk_list)} 条')

        print(f'数据已全部合并，数据文件生成中...')
        with open(self.company_list_info_json_path, 'w', encoding='utf8') as fp:
            json.dump(company_list_info_list, fp)
        print(f'数据文件创建完成')

    def process_company_list_json(self):
        """
        处理公司列表页面生成的公司数据json
        :return:
        """
        assert os.path.exists(self.company_list_info_json_path), f'company list info json is not exists!'

        print('原数据读取中...')
        with open(self.company_list_info_json_path, 'r', encoding='utf8') as fp:
            company_list_info_list = json.load(fp)

        print(f'原数据读取完毕，共计 {len(company_list_info_list)} 条，准备开始去重...')
        unique_company_list_info_dict = dict()
        for item in company_list_info_list:
            company_home_url = item['c_url']
            class_name = item['class']
            if company_home_url not in unique_company_list_info_dict:
                del item['class']
                unique_company_list_info_dict[company_home_url] = {
                    'class_set': set(),
                    'company_list_info': item
                }
            unique_company_list_info_dict[company_home_url]['class_set'].add(class_name)
            print(f'\r{item["c_name"]}', end='')

        print(f'\r原数据去重完毕，数据共计 {len(unique_company_list_info_dict)} 条，去重数据整理中...')
        unique_company_list_info_list = list()
        for i, item in enumerate(unique_company_list_info_dict.values(), start=1):
            company_list_info = item['company_list_info']
            company_list_info['classes'] = list(item['class_set'])
            company_list_info['id'] = str(i)
            unique_company_list_info_list.append(company_list_info)
            print(f'\r{company_list_info["c_name"]}', end='')

        print('\r去重数据整理完毕，正在生成数据文件...')
        with open(self.company_list_info_unique_json_path, 'w', encoding='utf8') as fp:
            json.dump(unique_company_list_info_list, fp)
        print('数据文件创建完成')

    def get_unique_company_list_info_data(self, head: Union[int, type(None)] = 10):
        """
        查看去重的公司列表信息数据
        :param head: 查看前 head 条数据
        :return:
        """
        assert os.path.exists(self.company_list_info_unique_json_path), f'company list info unique json is not exists!'

        with open(self.company_list_info_unique_json_path, 'r', encoding='utf8') as fp:
            company_list_info_unique_list = json.load(fp)

        return company_list_info_unique_list[:head] if head else company_list_info_unique_list

    def view_unique_company_list_info_data_head(self, head=5):
        """
        查看数据头部
        :param head:
        :return:
        """
        data_list = self.get_unique_company_list_info_data(head=head)
        for i in data_list:
            pprint.pprint(i)

    def download_company_home_page(self):
        """
        下载公司主页
        :return:
        """
        print('下载公司详情页...')
        self.mkdir(self.company_pages_dir_path)
        company_list = self.get_unique_company_list_info_data(head=None)
        time_stack = list()

        company_list_len = len(company_list)
        for i, item in enumerate(company_list, start=1):
            time_start = time.time()
            item: dict = item

            company_id = item['id']
            url = f"{item['c_url']}/company.html"

            file_path = os.path.join(self.company_pages_dir_path, f'{company_id}.html')

            if os.path.exists(file_path):
                # print(f'{url} {company_id} 已采集 {self.draw_data_progress(i, company_list_len)}')
                print(f'\r{self.draw_data_progress(i, company_list_len)}', end='')
                continue

            # print(f'{url} {company_id} 正在采集...')
            requests = WhileRequests()
            result = requests.get(url, request_times=50, sleep_time=self.random_float(0, 0.1))
            result.encoding = 'utf8'
            with open(file_path, 'w', encoding='utf8') as fp:
                fp.write(result.text)

            time_end = time.time()
            if len(time_stack) >= 100:
                time_stack.pop(0)
            time_stack.append(time_end - time_start)

            # print(f'{url} {company_id} 采集完成 {self.draw_data_progress(i, company_list_len)}')
            print(
                f'\r{self.draw_data_progress(i, company_list_len)} {round(len(time_stack) / max(sum(time_stack), 0.01) * 60, 2):.2f}条/分',
                end='')
            # break
        print('\n任务采集完成')

    def parse_company_pages(self):
        """
        解析公司页面
        :return:
        """
        assert os.path.exists(self.company_pages_dir_path), 'company_pages dir is not exists'
        self.mkdir(self.company_pages_json_dir_path)

        key_words_list = ['姓名', '电话', '地区']

        file_list = os.listdir(self.company_pages_dir_path)
        len_file_list = len(file_list)
        for i, file_name in enumerate(file_list, start=1):
            print(f'\r{self.draw_data_progress(i, len_file_list)}', end='')
            # file_name = '105397.html'
            file_path = os.path.join(self.company_pages_dir_path, file_name)
            file_json_path = os.path.join(self.company_pages_json_dir_path, file_name.replace('.html', '.json'))
            if os.path.exists(file_json_path):
                continue

            if not os.path.exists(file_path):
                continue

            company_id = file_name[:-5]
            # print(company_id)
            with open(file_path, 'r', encoding='utf8') as fp:
                content = fp.read()

            selector = etree.HTML(content)

            member_li_list = selector.xpath('.//div[@class="member-info"]/ul/li')

            contact_info_list = list()
            for li in member_li_list:
                li_text: str = li.xpath('string(.)')
                for key_words in key_words_list:
                    if key_words in li_text:
                        contact_info_list.append(li_text.strip())
            # print(contact_info_list)

            company_info_text_list = selector.xpath(
                './/div[contains(@class, "company-info")]//p[@class="content"]/text()')
            if len(company_info_text_list) == 0:
                # 会员页面
                company_info_text_list = selector.xpath('.//div[contains(@class, "shop-comp-ins")]/text()')
            company_intr_text = self.clean_text(self.data_list_get_first(company_info_text_list))
            # print(company_intr_text)

            register_info_dict = dict()
            register_info_tr_list = selector.xpath('.//div[contains(@class, "register-info")]/div//tr')
            if len(register_info_tr_list) > 0:
                # 普通用户 工商信息
                for tr in register_info_tr_list:
                    td_key_list = tr.xpath('.//td[@class="param"]')
                    td_value_list = tr.xpath('.//td[@class="val"]')
                    if len(td_key_list) == len(td_value_list):
                        for k, v in zip(td_key_list, td_value_list):
                            register_info_dict[self.clean_text(k.xpath('string(.)'))] = self.clean_text(
                                v.xpath('string(.)'))

            base_info_dict = dict()
            base_info_tr_list = selector.xpath('.//div[contains(@class, "base-info")]/div//tr')
            if len(base_info_tr_list) > 0:
                # 普通用户 基本信息
                for tr in base_info_tr_list:
                    td_key_list = tr.xpath('.//td[@class="param"]')
                    td_value_list = tr.xpath('.//td[@class="val"]')
                    if len(td_key_list) == len(td_value_list):
                        for k, v in zip(td_key_list, td_value_list):
                            base_info_dict[self.clean_text(k.xpath('string(.)'))] = self.clean_text(
                                v.xpath('string(.)'))

            if len(register_info_dict) == 0 and len(base_info_dict) == 0:
                shop_section_div_list = selector.xpath('.//div[contains(@class, "shop-section")]')
                if len(shop_section_div_list) > 0:
                    # 会员用户
                    for div in shop_section_div_list:
                        title_list = div.xpath('./div[@class="shop-section-tit"]/span/text()')
                        title = self.clean_text(self.data_list_get_first(title_list))
                        tr_list = div.xpath('.//tr')
                        for tr in tr_list:
                            td_key_list = tr.xpath('.//td[@class="table-gray aC"]')
                            td_value_list = tr.xpath('.//td[@class="table-tdcon"]')
                            if len(td_key_list) == len(td_value_list):
                                for k, v in zip(td_key_list, td_value_list):
                                    if '企业详细信息' in title:
                                        register_info_dict[self.clean_text(k.xpath('string(.)'))] = self.clean_text(
                                            v.xpath('string(.)'))
                                    if '企业基本信息' in title:
                                        base_info_dict[self.clean_text(k.xpath('string(.)'))] = self.clean_text(
                                            v.xpath('string(.)'))

            # pprint.pprint(base_info_dict)
            # pprint.pprint(register_info_dict)

            temp_dict = {
                'id': company_id,
                'contact_info': contact_info_list,
                'company_intr': company_intr_text,
                'base_info': base_info_dict,
                'register_info': register_info_dict
            }

            with open(file_json_path, 'w', encoding='utf8') as fp:
                json.dump(temp_dict, fp)

            # break
        print('\n解析结束')

    def merge_page_list_info_company_info(self):
        """
        合并列表数据与公司详情数据
        :return:
        """
        self.mkdir(self.company_info_json_dir)

        data_list = self.get_unique_company_list_info_data(head=None)
        temp_data_list = list()
        i = 0
        for item in data_list:
            i += 1
            print(f'\r{self.draw_data_progress(i, len(data_list))}', end='')
            item: dict
            company_id = item['id']
            file_path = os.path.join(self.company_pages_json_dir_path, f'{company_id}.json')
            with open(file_path, 'r', encoding='utf8') as fp:
                item_json = json.load(fp)
            item.update(item_json)
            temp_data_list.append(item)
            if i % 10000 == 0:
                data_part_path = os.path.join(self.company_info_json_dir, f'{i - 9999}_{i}.json')
                with open(data_part_path, 'w', encoding='utf8') as fp:
                    json.dump(temp_data_list, fp)
                temp_data_list.clear()
        if len(temp_data_list) > 0:
            data_part_path = os.path.join(self.company_info_json_dir, f'{i - len(temp_data_list) + 1}_{i}.json')
            with open(data_part_path, 'w', encoding='utf8') as fp:
                json.dump(temp_data_list, fp)


if __name__ == '__main__':
    shh_spider = SouHaoHuoSpider()
    # shh_spider.download_company_lib_home_page()
    # shh_spider.parse_company_lib_home_page()
    # shh_spider.download_company_list_page()
    # shh_spider.company_list_page_stat()
    # shh_spider.parse_company_list_page_to_json()
    # shh_spider.merge_company_list_json()
    # shh_spider.process_company_list_json()
    # shh_spider.get_unique_company_list_info_data()
    # shh_spider.view_unique_company_list_info_data_head()
    # shh_spider.download_company_home_page()
    # shh_spider.parse_company_pages()
    shh_spider.merge_page_list_info_company_info()
