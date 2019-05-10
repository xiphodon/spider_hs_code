#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/5/10 9:45
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : coroutine_test.py
# @Software: PyCharm

"""协程测试"""


def consumer():
    print('==== c_A ====')
    r = ''
    while True:
        print('==== c_B ====')
        n = yield r
        print('==== c_C ====')
        if not n:
            return
        print('[CONSUMER] Consuming %s...' % n)
        r = '200 OK'
        print('==== c_D ====')


def produce(c):
    print('==== p_A ====')
    r = c.send(None)
    print('[PRODUCER] c.send(None) %s...' % r)
    n = 0
    print('==== p_B ====')
    while n < 5:
        n = n + 1
        print('[PRODUCER] Producing %s...' % n)
        print('==== p_C ====')
        r = c.send(n)
        print('==== p_D ====')
        print('[PRODUCER] Consumer return: %s' % r)
    c.close()
    print('==== p_E ====')


def start_1():
    c = consumer()
    produce(c)


def generator_1():
    total = 0
    while True:
        x = yield
        print('加', x)
        if not x:
            return total
        total += x


def generator_2():  # 委托生成器
    while True:
        print('while True')
        total = yield from generator_1()  # 子生成器
        print('加和总数是:', total)


def start_2():  # 调用方
    g1 = generator_1()
    g1.send(None)
    g1.send(2)
    g1.send(3)
    g1.send(None)


def start_3():
    g2 = generator_2()
    g2.send(None)
    g2.send(2)
    g2.send(3)
    g2.send(None)


if __name__ == '__main__':
    # start_1()
    # start_2()
    start_3()
