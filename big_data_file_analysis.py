#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/31 16:08
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : big_data_file_analysis.py
# @Software: PyCharm

import pymssql
import settings


def myspace_data_to_sqlserver(is_debug=True, skip_line=0, debug_data_size=10000):
    """
    myspace数据入库
    :param is_debug: 是否debug模式
    :param skip_line: 跳过数据行数
    :param debug_data_size: debug模式测试数据条数
    :return:
    """
    # 连接数据库
    conn = pymssql.connect(host=settings.host, user=settings.user, password=settings.password,
                           database=settings.database_2, charset=settings.charset)
    cur = conn.cursor()
    if not cur:
        raise (NameError, "数据库连接失败")
    else:
        print('数据库连接成功')

    bulk_size = 1000
    bulk_count = 0
    count = 0
    error_count = 0
    with open(settings.file_3_path, 'r', encoding='utf8') as fp:
        for line in fp:

            print('=' * 50)
            count += 1
            print(count)

            if is_debug:
                if count > debug_data_size:
                    break

            if skip_line >= count:
                continue

            line = line.strip('\n')

            item_user_list = line.split(':')
            item_user_list_len = len(item_user_list)

            # print(item_user_list_len)
            print(item_user_list)

            if item_user_list_len == 5:
                _user_id = item_user_list[0].strip()
                _user_email = item_user_list[1].lower().strip()
                _user_name = item_user_list[2].strip()

                user_email = ''
                user_domain = ''
                user_name = ''
                data_from = 'myspace'

                if '@' in _user_email and '.' in _user_email:
                    # 格式化邮箱
                    if ' ' in _user_email:
                        # email拆出正确邮箱部分
                        _user_email_list = _user_email.split(' ')

                        # 多空格合并并拆分
                        temp_email = ''.join(_user_email_list)

                        stop_word = ''
                        stop_word_list = ['.com', '.org', '.net']

                        for item_stop_word in stop_word_list:
                            if item_stop_word in temp_email:
                                stop_word = item_stop_word
                                break

                        email_is_ok = False
                        if stop_word != '':
                            last_dot_index = temp_email.rfind('.')
                            stop_word_index = temp_email.rfind(stop_word)
                            stop_word_len = len(stop_word)

                            if last_dot_index == stop_word_index:
                                # 以停止词结尾
                                user_email = temp_email[:stop_word_index + stop_word_len]
                                email_is_ok = True
                            else:
                                user_email = temp_email
                        else:
                            user_email = temp_email

                        if not email_is_ok:
                            locked_index = temp_email.rfind('locked')
                            if locked_index > 0:
                                user_email = temp_email[:locked_index]

                        # print(f'{"*" * 30} {_user_email}')
                    else:
                        user_email = _user_email
                    # print(user_email)
                else:
                    # print(f'>> user_email error: {_user_email}')
                    continue

                if user_email == '':
                    continue
                else:
                    user_domain = user_email.split('@')[-1]

                if _user_name != _user_id and _user_name != '':
                    user_name = _user_name

                # 检查数据长度
                if len(user_email) >= 128:
                    continue
                if user_name != '':
                    if len(user_name) >= 128:
                        user_name = ''

                print(user_email)
                print(user_domain)
                print(user_name)

                # 邮箱、域名、用户名 入库
                try:
                    sql_str = "insert into Mail2(Email, domain, username, data_from) values(N'%s',N'%s',N'%s',N'%s')"\
                        % (user_email, user_domain, user_name, data_from)
                    cur.execute(sql_str.encode('utf8'))

                    bulk_count += 1

                    if bulk_count >= bulk_size:
                        conn.commit()
                        bulk_count = 0
                except Exception as e:
                    error_count += 1
                    print(e, count, error_count)
                    print(sql_str)
    try:
        conn.commit()
    except Exception as e:
        print(e)
    conn.close()


def start():
    """
    程序入口
    :return:
    """
    myspace_data_to_sqlserver(is_debug=True, skip_line=0)


if __name__ == '__main__':
    start()
