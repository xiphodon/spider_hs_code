#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/5/15 9:13
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : namedtuple_test.py
# @Software: PyCharm
import json
from collections import namedtuple

Human = namedtuple('Human', ['name', 'age', 'job'])

user = Human('WK', '20', 'teacher')

print(user)

user = user._replace(age='31')

print(user)

print(user._asdict())

print(json.dumps(user._asdict()))
