#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/3/7 14:07
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : lianjia.py
# @Software: PyCharm

import requests
import json


def start():
    """
    入口
    :return:
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
        'Connection': 'keep-alive'
    }

    # result = requests.get('https://sh.lianjia.com/ershoufang/107100958167.html', headers=headers)
    #
    # with open(r'C:\Users\topeasecpb\Desktop\lianjia.html', 'w', encoding='utf8') as fp:
    #     fp.write(result.text)

    # 一级分类
    tag_list = ['交通设施', '金融', '医疗', '教育培训', '购物', '美食', '休闲娱乐']

    ak = 'G4fVjlQXHFfBpv7OILhjoGYiqLFa0S1Y'
    url = f'http://api.map.baidu.com/place/v2/search?' \
          f'q={"$".join(tag_list)}&' \
          f'location=31.233459,121.476516&' \
          f'radius=2000&output=xml&' \
          f'ak={ak}&' \
          f'scope=2&' \
          f'output=json&' \
          f'filter=sort_name:distance|sort_rule:1&' \
          f'page_size=20&' \
          f'page_num=0'

    # f'tag=交通设施$金融&' \
    result = requests.get(url, headers=headers)
    content = result.text
    print(content)

    # content_json = json.loads(content)
    # print(content_json)


if __name__ == '__main__':
    start()
