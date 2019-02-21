#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/2/20 11:40
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : yousheng_spider.py
# @Software: PyCharm

import WhileRequests
from lxml import etree
import json
import os
import time

home_url = 'https://www.ysts8.com'

# 三体1/2/3 有声小说资源
source_url_list = ['https://www.ysts8.com/Yshtml/Ys7698.html',
                   'https://www.ysts8.com/Yshtml/Ys7841.html',
                   'https://www.ysts8.com/Yshtml/Ys9109.html']

home_path = r'E:\three body'

for i in range(len(source_url_list)):
    dir_path = os.path.join(home_path, f'three body {i+1}')
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)

request = WhileRequests.WhileRequests()


def start():
    """
    入口
    :return:
    """
    for i in range(len(source_url_list)):
        dir_path = os.path.join(home_path, f'three body {i+1}')

        result = request.get(source_url_list[i])
        result.encoding = 'gbk'
        # print(result.text)

        selector = etree.HTML(result.text)

        mp3_url_list = selector.xpath(r'.//div[@class="current"]/div[@class="ny_l"]/ul/li/a/@href')
        mp3_name_list = selector.xpath(r'.//div[@class="current"]/div[@class="ny_l"]/ul/li/a/@title')

        for _name,_url in zip(mp3_name_list, mp3_url_list):
            item_mp3_url = home_url + _url

            res = request.get(item_mp3_url)
            res.encoding = 'gbk'
            sele = etree.HTML(res.text)

            iframe_src = sele.xpath(r'.//iframe/@src')[0]
            # print(iframe_src)

            iframe_url = home_url + iframe_src
            iframe_res = request.get(iframe_url)
            iframe_res.encoding = 'gbk'
            # print(iframe_res.text)

            iframe_sele = etree.HTML(iframe_res.text)
            iframe_script = iframe_sele.xpath(r'.//script')
            # print(iframe_script)

            script_view = iframe_script[-1]
            script_str = str(script_view.xpath('string(.)'))
            # print(script_str)

            var_index_start = script_str.find('var preurl;')
            var_index_end = script_str.find('function next()')

            var_lines_str = script_str[var_index_start: var_index_end]
            # print(var_lines_str)

            var_lines_list = var_lines_str.strip().split('\r\n')
            # print(var_lines_list)

            var_str_1 = var_lines_list[-2] + var_lines_list[-1]
            # print(var_str_1)

            var_index_start_2 = script_str.find('$(this).jPlayer("setMedia", {')
            var_index_end_2 = script_str.find('}).jPlayer("play"); ')

            var_lines_str_2 = script_str[var_index_start_2: var_index_end_2]
            # print(var_lines_str_2)

            var_lines_list_2 = var_lines_str_2.strip().split('\r\n')
            # print(var_lines_list_2)

            var_str_2 = var_lines_list_2[-1].strip().replace('mp3:', '', 1)
            # print(var_str_2)

            exec(var_str_1)
            mp3 = eval(var_str_2)
            print(mp3)

            _res = request.get(mp3)
            file_path = os.path.join(dir_path, _name)
            if not os.path.exists(file_path):
                with open(os.path.join(dir_path, _name), 'wb') as fp:
                    fp.write(_res.content)
            # break
            time.sleep(1)


if __name__ == '__main__':
    start()
