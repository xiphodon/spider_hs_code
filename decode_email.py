#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/11/11 9:02
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : decode_email.py
# @Software: PyCharm
import html
from urllib import parse

text = 'b3c5d8ddc0dcc1d4c6dfd2f3c3c1dcc7dcddded2dadf9dd0dcde'


def r(e, t):
    """
    取两个字符，并转为16进制数字
    :param e:
    :param t:
    :return:
    """
    _r = e[t: t + 2]
    _ri = int(_r, 16)
    print(_r, _ri)
    return _ri


def de_email(n, c):
    """
    解密邮件
    :param n:
    :param c:
    :return:
    """
    o = ''
    a = r(n, c)
    for i in range(2, len(n), 2):
        m = r(n, i) ^ a
        o += chr(m)
    t = parse.unquote(html.escape(o))
    print(t)


if __name__ == '__main__':
    de_email('2254494c514d5045574e436252504d564d4c4f434b4e0c414d4f', 0)