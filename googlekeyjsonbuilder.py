#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/4/15 18:36
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : googlekeyjsonbuilder.py
# @Software: PyCharm

import json


def start():
    """
    入口
    :return:
    """
    file_path = r'C:\Users\topeasecpb\Desktop\googlekey\googlekey.txt'
    key_list = list()
    with open(file_path, 'r', encoding='utf8') as fp:
        for line in fp.readlines():
            line = str(line).strip()
            key_list.append(line)

    save_file_path = r'C:\Users\topeasecpb\Desktop\googlekey\all_valid_google_key_list_3.json'
    with open(save_file_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(key_list))


if __name__ == '__main__':
    start()
