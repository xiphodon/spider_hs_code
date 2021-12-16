#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/11/25 10:39
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : OpenDataBotSpider.py
# @Software: PyCharm
import json
from collections import OrderedDict
from pathlib import Path
from pprint import pprint

import pandas as pd
from lxml import etree

from WhileRequests import WhileRequests
from base_spider import BaseSpider, DataProgress


class OpenDataBotSpider(BaseSpider):
    """
    open data bot spider
    """
    home_url = f'https://opendatabot.ua/c/'
    home_dir = Path(r'E:\open_data_dot')
    u_ids_json_path = home_dir / 'u_ids.json'
    u_pages_dir_path = home_dir / 'u_pages_dir'
    u_company_json_path = home_dir / 'u_company.json'

    def __init__(self):
        super(BaseSpider, self).__init__()
        # 乌克兰ids
        self.u_ids_list = list()
        self.request = WhileRequests()

    def read_excel(self):
        """
        读取excel数据
        :return:
        """
        excel_dir_path = Path(r'E:\乌克兰土耳其id')
        u_id_set = set()
        for file_path in excel_dir_path.iterdir():
            # print(file_path.as_posix())
            if file_path.name.startswith('乌克兰'):
                df = pd.read_excel(file_path.as_posix(), dtype=str)
                # print(df.head())
                columns = df.columns
                if 'IDNO_OF_IMPORTER' in columns:
                    col_name = 'IDNO_OF_IMPORTER'
                else:
                    col_name = 'IDNO_OF_A_SENDER'
                item_list = df[col_name].values
                # print(item_list)
                u_id_set.update(item_list)
        with open(self.u_ids_json_path.as_posix(), 'w', encoding='utf8') as fp:
            json.dump(list(u_id_set), fp)

    def load_ids_json_data(self):
        """
        加载ids json数据
        :return:
        """
        with open(self.u_ids_json_path.as_posix(), 'r', encoding='utf8') as fp:
            self.u_ids_list = json.load(fp)

    def gevent_downloas_pages(self):
        """
        多协程下载
        :return:
        """
        self.u_pages_dir_path.mkdir(exist_ok=True)
        dp = DataProgress()
        _datas = [[i, _id, len(self.u_ids_list), dp] for i, _id in enumerate(self.u_ids_list, start=1)]
        self.gevent_pool_requests(self.download_pages, _datas, gevent_pool_size=20)

    def download_pages(self, datas):
        """
        下载页面
        :param datas:
        :return:
        """
        i, item_id, ids_len, dp = datas

        dp.print_data_progress(i, ids_len)
        file_path = self.u_pages_dir_path / f'{item_id}.html'
        if file_path.exists():
            return
        url = f'{self.home_url}{item_id}'
        result = self.request.get(url)
        with open(file_path.as_posix(), 'w', encoding='utf8') as fp:
            fp.write(result.text)

    def parse_html_pages(self):
        """
        解析html页面
        :return:
        """
        all_data_k_list = list()
        data_list = list()
        for i, file_path in enumerate(self.u_pages_dir_path.iterdir(), start=1):
            if file_path.stat().st_size < 8000:
                continue
            print(i, file_path.as_posix())
            item_data_dict = OrderedDict()
            content = file_path.read_text(encoding='utf8')
            selector = etree.HTML(content)

            # 基础信息
            data_item_list = selector.xpath('.//div[@class="container"]//div[@class="hyphenations"]')
            for data_item in data_item_list:
                data_text_list = data_item.xpath('.//*[@data-v-ea5a7018]/text()')
                # print(data_text_list)
                if len(data_text_list) == 2:
                    data_k, data_v = self.clean_text(data_text_list[0]), self.clean_text(data_text_list[1])
                    all_data_k_list.append(data_k) if data_k not in all_data_k_list else ...
                    item_data_dict[data_k] = data_v

            # 2020年财务报表
            fs_2020_tr_list = selector.xpath('.//div[@data-v-4583ce6e]//table[@class="table"]//tr')
            for fs_2020_tr in fs_2020_tr_list:
                fs_text_list = fs_2020_tr.xpath('./td/text()')
                if len(fs_text_list) == 2:
                    fs_k_text, fs_v_text = self.clean_text(fs_text_list[0]), self.clean_text(fs_text_list[1])
                    all_data_k_list.append(fs_k_text) if fs_k_text not in all_data_k_list else ...
                    item_data_dict[fs_k_text] = fs_v_text

            # 注册登记变更记录
            edit_log_tr_list = selector.xpath('.//table[@class="table" and @data-v-6ebfea86]/tbody/tr')
            edit_log_list = list()
            for edit_log_tr in edit_log_tr_list:
                edit_td_list = edit_log_tr.xpath('./td')
                # print(edit_td_list)
                edit_log_list.append(dict(zip(['Дата', 'Зміна', 'Було', 'Стало'], [self.data_list_get_first(edit_td.xpath('./text()')) for edit_td in edit_td_list])))
            item_data_dict['Історія змін реєстраційної інформації'] = edit_log_list

            # 增值税
            col_v = self.data_list_get_first(selector.xpath('.//div[@class="row px-4" and @data-v-4583ce6e]/div[@class="col"]/p/text()'))
            item_data_dict['Платник ПДВ'] = col_v

            # 法院登记
            record_num = self.data_list_get_first(selector.xpath('.//a[@data-v-4d1e13de]/text()'))
            item_data_dict['Судовий реєстр'] = record_num

            # 问答
            qa_view_list = selector.xpath('.//div[@class="card" and @data-v-865746a6]')
            qa_list = list()
            for qa_view in qa_view_list:
                q_text = self.clean_text(self.data_list_get_first(qa_view.xpath('.//button[@type="button" and @data-v-865746a6]/text()')))
                a_text = self.clean_text(self.data_list_get_first(qa_view.xpath('.//p[@data-v-865746a6]/text()')))
                qa_list.append({
                    'q': q_text,
                    'a': a_text
                })

            item_data_dict['qa_list'] = qa_list

            # pprint(item_data_dict)
            data_list.append(item_data_dict)

            break
        print(len(data_list))
        self.u_company_json_path.write_text(json.dumps(data_list), encoding='utf8')
        # with open(self.u_company_json_path.as_posix(), 'w', encoding='utf8') as fp:
        #     json.dump(data_list, fp)


if __name__ == '__main__':
    odbs = OpenDataBotSpider()
    # odbs.read_excel()
    # odbs.load_ids_json_data()
    # odbs.download_pages()
    # odbs.gevent_downloas_pages()
    odbs.parse_html_pages()

