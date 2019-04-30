#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/4/30 16:38
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : tail_call.py
# @Software: PyCharm

# 尾递归注解实现
import sys

# 修改递归栈上限
# sys.setrecursionlimit(10000)


class TailRecurseException(BaseException):
    def __init__(self, args, kwargs):
        self.args = args
        self.kwargs = kwargs


def tail_call_optimized(g):
    """
    This function decorates a function with tail call
    optimization. It does this by throwing an exception
    if it is it's own grandparent, and catching such
    exceptions to fake the tail call optimization.

    This function fails if the decorated
    function recurses in a non-tail context.
    """

    def func(*args, **kwargs):
        f = sys._getframe()
        # 为什么是grandparent, 函数默认的第一层递归是父调用,
        # 对于尾递归, 不希望产生新的函数调用(即:祖父调用),
        # 所以这里抛出异常, 拿到参数, 退出被修饰函数的递归调用栈!(后面有动图分析)
        if f.f_back and f.f_back.f_back \
                and f.f_back.f_back.f_code == f.f_code:
            # 抛出异常
            raise TailRecurseException(args, kwargs)
        else:
            while 1:
                try:
                    return g(*args, **kwargs)
                except TailRecurseException as e:
                    # 捕获异常, 拿到参数, 退出被修饰函数的递归调用栈
                    args = e.args
                    kwargs = e.kwargs

    func.__doc__ = g.__doc__
    return func


# @tail_call_optimized 仅对尾递归实现有效
def factorial_1(n: int) -> int:
    """
    阶乘_1
    :param n:
    :return:
    """
    if not isinstance(n, int):
        raise TypeError('The value must be of integer type')
    if n < 0:
        raise ValueError('The value must be positive')

    if n == 0:
        return 1
    else:
        return n * factorial_1(n-1)


def factorial_2(n: int) -> int:
    """
    阶乘_2
    :param n:
    :return:
    """
    if not isinstance(n, int):
        raise TypeError('The value must be of integer type')
    if n < 0:
        raise ValueError('The value must be positive')

    @tail_call_optimized
    def factorial_inner(m: int, value: int=1) -> int:
        """
        阶乘内部实现
        (尾递归)
        :param m:
        :param value:
        :return:
        """
        if m == 0:
            return value
        else:
            return factorial_inner(m-1, m*value)

    return factorial_inner(n)


if __name__ == '__main__':
    print(factorial_1(5))
    print(factorial_2(1200))
