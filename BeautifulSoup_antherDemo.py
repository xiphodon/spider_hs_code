#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/6 11:02
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : BeautifulSoup_antherDemo.py
# @Software: PyCharm

from bs4 import BeautifulSoup

# soup = BeautifulSoup(None, 'lxml')
soup = BeautifulSoup(None, 'html.parser')

# 选择a标签，其属性中存在myname的所有标签
soup.select("a[myname]")
# 选择a标签，其属性href=http://example.com/lacie的所有标签
soup.select("a[href='http://example.com/lacie']")
# 选择a标签，其href属性以http开头
soup.select('a[href^="http"]')
# 选择a标签，其href属性以lacie结尾
soup.select('a[href$="lacie"]')
# 选择a标签，其href属性包含.com
soup.select('a[href*=".com"]')
# 从html中排除某标签，此时soup中不再有script标签
[s.extract() for s in soup('script')]
# 如果想排除多个呢
[s.extract() for s in soup(['script', 'fram'])]