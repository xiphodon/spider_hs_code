#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/12 13:39
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : rakuten_test.py
# @Software: PyCharm

import requests
import random
# requests.adapters.DEFAULT_RETRIES = 5


def func_1():
    """
    测试方法1
    :return:
    """
    headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
                'Connection': 'keep-alive'
            }

    url = 'https://search.rakuten.co.jp/search/mall/モップ/?p=1'
    # url = 'https://www.baidu.com'
    # sess = requests.session()

    result = requests.get(url, headers=headers, timeout=10)
    # result = sess.get(url, headers=headers, timeout=10)
    print(result.text)


def func_02(times=1000000):
    """
    酒鬼喝酒问题
    :return:
    """
    in_bar_rate = 0.9

    random_min = 0
    random_max = 10000
    random_range = random_max - random_min

    tag_list = list()
    for i in range(times):
        random_tag = random.randint(random_min, random_max)

        if random_tag < random_range * in_bar_rate:
            # 选择去酒吧
            bar_tag = random_tag % 3
            temp_dict = {
                'go_bar': True,
                'which_bar': bar_tag
            }
        else:
            # 选择在家
            temp_dict = {
                'go_bar': False,
                'which_bar': None
            }
        tag_list.append(temp_dict)

    # 1、统计去酒吧频率、统计选择各个酒吧频率
    # 2、酒鬼不在前两家酒吧，求在第三家酒吧的几率
    go_bar_count = 0
    go_bar_0_count = 0
    go_bar_1_count = 0
    go_bar_2_count = 0
    not_go_bar_count = 0

    a_in_bar_3_count = 0
    a_not_in_bar_3_count = 0
    for i in tag_list:
        is_go_bar = i['go_bar']
        which_bar = i['which_bar']

        # 统计
        if is_go_bar is True:
            go_bar_count += 1
        else:
            not_go_bar_count += 1

        if which_bar == 0:
            go_bar_0_count += 1
        elif which_bar == 1:
            go_bar_1_count += 1
        elif which_bar == 2:
            go_bar_2_count += 1
        else:
            pass

        # 求解
        if which_bar not in (0, 1):
            # 酒鬼不在前两家酒吧
            if which_bar == 2:
                a_in_bar_3_count += 1
            else:
                a_not_in_bar_3_count += 1

    a_in_bar_3_rate = a_in_bar_3_count/(a_in_bar_3_count + a_not_in_bar_3_count)
    a_not_in_bar_3_rate = a_not_in_bar_3_count/(a_in_bar_3_count + a_not_in_bar_3_count)

    print('随机数值统计:')
    print(f'  随机次数:{times}')
    print(f'  决定去酒吧的次数:{go_bar_count}')
    print(f'    去酒吧1的次数:{go_bar_0_count}')
    print(f'    去酒吧2的次数:{go_bar_1_count}')
    print(f'    去酒吧3的次数:{go_bar_2_count}')
    print(f'  决定不去酒吧的次数:{not_go_bar_count}')

    print('=' * 30)

    print('酒鬼不在前两家酒吧，求在第三家酒吧的几率:')
    print(f'  不在酒吧1、2，在酒吧3的次数:{a_in_bar_3_count},几率:{a_in_bar_3_rate}')
    print(f'  不在酒吧1、2，不在酒吧3的次数:{a_not_in_bar_3_count},几率:{a_not_in_bar_3_rate}')


if __name__ == '__main__':
    func_02()
