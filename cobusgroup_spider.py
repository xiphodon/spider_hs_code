#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/10/21 10:55
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : cobusgroup_spider.py
# @Software: PyCharm
import json
from pathlib import Path
from pprint import pprint

import requests
from lxml import etree
import pandas as pd

from base_spider import BaseSpider
import http.client
http.client.HTTPConnection._http_vsn = 10
http.client.HTTPConnection._http_vsn_str = 'HTTP/1.0'


class CobusGroupSpider(BaseSpider):
    """
    cobusgroup spider
    """

    home_path = Path('E:\cobus_group')

    headers = {
        'Host': 'www.cobusgroup.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': '84',
        'Origin': 'https://www.cobusgroup.com',
        'Connection': 'keep-alive',
        'Referer': 'https://www.cobusgroup.com/simple-resultados-sal',
        'Cookie': 'PHPSESSID=hi7h6b5ftktnt8u2ko6l8g7if2; _ga=GA1.2.731453947.1634781850; ldkRefererTracking=direct; _gid=GA1.2.473033089.1635115603; _gat_leadakiTracker=1; _gat=1',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1'
    }

    def __init__(self):
        """
        init
        """
        super(BaseSpider, self).__init__()
        self.home_path.mkdir(exist_ok=True)
        self.sess = requests.session()

    def download_list_page(self, current_keyword_dir, dir_name, page_size, page_count, page_i):
        """
        下载列表页面
        :param current_keyword_dir:
        :param dir_name:
        :param page_size:
        :param page_count:
        :param page_i:
        :return:
        """
        file_path = current_keyword_dir / f'{page_i}.html'
        if file_path.exists():
            return

        url = 'https://www.cobusgroup.com/simple-resultados-sal'
        data = {
            'paginador': '1',
            'number_debut': f'{(page_i - 1) * page_size}',
            'sig': f'{page_i}',
            'page_total': f'{page_count}',
            'page_actual': f'simple-resultados-sal{page_i}'
        }

        if page_i > 10 and page_i % 10 == 1:
            # 增加请求参数
            data['kkk'] = f'{page_i}'
            data['ii'] = f'{(page_i - 1) * page_size}'

        # result = self.sess.post(url, data=data, headers=self.headers, timeout=180, stream=True)
        result = requests.post(url, data=data, headers=self.headers, timeout=360, stream=True)
        with open(file_path, 'w', encoding='utf8') as fp:
            fp.write(result.text)
        print(f'{dir_name}  {page_i} / {page_count} {len(result.content) / 1024: .0f}kb OK')

    def download_list_pages(self, country, dir_name, page_size, page_count):
        """
        下载当前所有列表页面
        :param country:
        :param dir_name:
        :param page_size:
        :param page_count:
        :return:
        """
        current_keyword_dir = self.home_path / country / dir_name
        current_keyword_dir.mkdir(exist_ok=True)

        for i in range(1, page_count + 1, 1):
            self.download_list_page(current_keyword_dir, dir_name, page_size, page_count, i)

    def extract_html_data(self, country):
        """
        抽取页面数据
        :return:
        """
        if country == '萨尔瓦多':
            # 萨尔瓦多数据抽取
            country_dir = self.home_path / country
            json_path = country_dir / 'result.json'
            result_list = list()
            for i, dir_path in enumerate(country_dir.iterdir(), start=1):
                # print(i, dir_name.as_posix())
                key_word = dir_path.stem
                for j, file_path in enumerate(dir_path.iterdir(), start=1):
                    print(j, file_path.as_posix())

                    with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                        content = fp.read()

                    selector = etree.HTML(content)
                    # print(selector.xpath('.//table[@id="tabla_resultados"]'))
                    table_view_list = selector.xpath('.//table[@id="tabla_resultados"]')
                    if len(table_view_list) != 2:
                        continue
                    table_view = table_view_list[0]
                    # print(etree.tostring(table_view))
                    data_list = table_view.xpath('.//tr/td[contains(@class, "border_bot")]')
                    # print(len(data_list))

                    tariff_position_list = list()
                    date_list = list()
                    total_cif_list = list()
                    total_weight_list = list()
                    destination_country_list = list()

                    # Tariff position, Date, Total Cif, Total Weight, Destination country
                    for n in range(len(data_list)):
                        text_value = self.clean_text(self.data_list_get_first(data_list[n].xpath('./text()')))
                        if n % 6 == 1:
                            tariff_position_list.append(text_value)
                        if n % 6 == 2:
                            date_list.append(text_value)
                        if n % 6 == 3:
                            total_cif_list.append(text_value)
                        if n % 6 == 4:
                            total_weight_list.append(text_value)
                        if n % 6 == 5:
                            destination_country_list.append(text_value)
                    for tariff_position, date, total_cif, total_weight, destination_country in zip(tariff_position_list, date_list, total_cif_list, total_weight_list, destination_country_list):
                        temp_dict = {
                            'country': country,
                            'keyword': key_word,
                            'tariff_position': tariff_position,
                            'date': date,
                            'total_cif': total_cif,
                            'total_weight': total_weight,
                            'destination_country': destination_country
                        }
                        result_list.append(temp_dict)
            with open(json_path.as_posix(), 'w', encoding='utf8') as fp:
                json.dump(result_list, fp)

        else:
            ...

    def json2csv(self, json_path: Path, target_path: Path):
        """
        json(data_list) to csv
        :param file_path:
        :return:
        """
        if not json_path.exists():
            return

        df = pd.read_json(json_path)
        # print(df.head())
        df.to_csv(target_path)

    def merge_argentina_data(self, import_or_export):
        """
        合并阿根廷数据
        :param import_or_export: 进口或出口
        :return:
        """
        data_dir_home = Path('E:\cobus_group\阿根廷')
        im_ex_dir = data_dir_home / import_or_export
        data_type = im_ex_dir.name
        data_title = list()
        df = None
        for date_dir in im_ex_dir.iterdir():
            if date_dir.is_file():
                continue
            date_time = date_dir.name
            for kw in date_dir.iterdir():
                keyword = kw.name
                for company_dir in kw.iterdir():
                    company_name = company_dir.name
                    for file_path in company_dir.iterdir():
                        file_name = file_path.name
                        if file_name == '_over.txt':
                            continue
                        with open(file_path.as_posix(), 'r', encoding='utf8') as fp:
                            try:
                                item_data = json.load(fp)
                            except Exception as e:
                                continue
                        # print(item_data)
                        # print(file_name)
                        item_data_dict = dict()
                        if len(data_title) == 0:
                            for item_dict in item_data:
                                for k, v in item_dict.items():
                                    if k == 'k':
                                        data_title.append(v)
                            data_title.append('_country')
                            data_title.append('_date_time')
                            data_title.append('_type')
                            data_title.append('_kw')

                        if df is None:
                            df = pd.DataFrame(columns=data_title)

                        for item_dict in item_data:
                            # print(item_dict)
                            if 'k' in item_dict:
                                item_data_dict[item_dict['k']] = item_dict['v']
                            elif 'a_k' in item_dict:
                                item_data_dict['detail_' + item_dict['a_k']] = item_dict['a_v']
                        item_data_dict['_country'] = '阿根廷'
                        item_data_dict['_date_time'] = date_time
                        item_data_dict['_type'] = data_type
                        item_data_dict['_kw'] = keyword
                        # print(data_title)

                        if 'Posición Arancelaria' in item_data_dict and item_data_dict['Posición Arancelaria'][0] == keyword[0]:
                            print(data_type, keyword, item_data_dict)
                            df = df.append([item_data_dict], ignore_index=True)
                # break
            # break
        # df.to_json((im_ex_dir / 'data.json').as_posix())
        # df.to_excel((im_ex_dir / 'data.xlsx').as_posix())
        df.to_csv((im_ex_dir / 'data.csv').as_posix())




if __name__ == '__main__':
    cgs = CobusGroupSpider()
    # cgs.download_list_pages('萨尔瓦多', '1%', 200, 100)
    # cgs.extract_html_data('萨尔瓦多')
    # cgs.json2csv(Path(r'E:\cobus_group\萨尔瓦多\result.json'), Path(r'E:\cobus_group\萨尔瓦多\result.csv'))
    # cgs.merge_argentina_data('进口')
    cgs.merge_argentina_data('出口')
