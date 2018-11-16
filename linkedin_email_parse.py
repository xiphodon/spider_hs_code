#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/11/11 12:15
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : linkedin_email_parse.py
# @Software: PyCharm

import os
import settings
import json
import pymssql

linkedin_email_dir = settings.linkedin_email_dir
linkedin_email_json_path = os.path.join(linkedin_email_dir, 'linkedin_email.json')
linkedin_email_json_path_pre = os.path.join(linkedin_email_dir, 'linkedin_email')

part_item_size=4000 * 10000


def parse_email_file(file_path):
    """
    解析email文件
    :param file_path:
    :return:
    """
    email_list = list()
    with open(file_path, 'r', encoding='utf8') as fp:
        for _line in fp:
            line = _line.strip()
            email = ''
            try:
                email = line.split(':')[0]
            except Exception as e:
                print(e)

            if email.lower() == 'null':
                continue

            if '@' not in email:
                continue

            # print(email)
            email_list.append(email)
    return email_list


def traversal_email_files():
    """
    遍历email文件
    :return:
    """
    email_list = list()
    for item_file_name in os.listdir(linkedin_email_dir):
        item_file_name_pre = item_file_name.split('.')[0]
        item_file_name_suf = item_file_name.split('.')[-1]
        if item_file_name_pre in [str(i) for i in range(1, 10)]:
            continue

        if item_file_name_suf != 'txt':
            continue

        item_file_path = os.path.join(linkedin_email_dir, item_file_name)
        print(item_file_path)

        item_email_list = parse_email_file(item_file_path)
        email_list.extend(item_email_list)
        print(len(item_email_list))

        # break

    print(f'total email len = {len(email_list)}')
    with open(linkedin_email_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(email_list))


def read_email_json(json_path):
    """
    读取email json数据
    :param json_path:
    :return:
    """
    with open(json_path, 'r', encoding='utf8') as fp:
        email_list = json.load(fp)
    return email_list


def read_email_json_part(parts_index):
    """
    读取part_index对应的数据部分
    :param parts_index:
    :return:
    """
    with open(f'{linkedin_email_json_path_pre}_{str(parts_index)}.json', 'r', encoding='utf8') as fp:
        email_list = json.load(fp)
    return email_list


def split_email_json():
    """
    切分email_json大文件为多个json小文件
    :param part_item_size:
    :return:
    """
    data = read_email_json(linkedin_email_json_path)
    len_data = len(data)
    start_index = 0
    end_index = part_item_size
    parts_index = 0
    while True:
        if end_index < len_data:
            item_data = data[start_index:end_index]
        else:
            item_data = data[start_index:]

        with open(f'{linkedin_email_json_path_pre}_{str(parts_index)}.json', 'w', encoding='utf8') as fp:
            fp.write(json.dumps(item_data))

        parts_index += 1
        start_index = parts_index * part_item_size
        end_index = start_index + part_item_size

        if start_index >= len_data:
            break


def save_email_to_sqlserver():
    """
    保存email到数据库
    :return:
    """
    conn = pymssql.connect(host=settings.host, user=settings.user, password=settings.password,
                           database=settings.database_2, charset=settings.charset)
    cur = conn.cursor()

    if not cur:
        raise (NameError, "数据库连接失败")
    else:
        print('数据库连接成功')

    save_email(conn, cur)

    conn.close()


def save_email(conn, cur):
    """
    保存email
    :param conn:
    :param cur:
    :return:
    """
    data = read_email_json_part(1)
    print('len(data)=', len(data))

    continue_index = 0  # 断点续传index

    count = 0
    error_count = 0

    bulk_count = 0
    bulk_size = 500

    for i, email in enumerate(data):
        if i < continue_index:
            continue
        try:
            sql_str = "insert into linkedin_email_temp(email) values(N'%s')" % (email, )
            cur.execute(sql_str.encode('utf8'))
            # conn.commit()
            count += 1
            bulk_count += 1
            if bulk_count >= bulk_size:
                conn.commit()
                bulk_count = 0
                print('index: ', i, ', OK', ',当前 count:', count, ', error_count:', error_count)

        except Exception as e:
            error_count += 1
            print(e, error_count, i, '===================')
            # raise e

        # if count > 20:
        #     break
        # break
    conn.commit()


if __name__ == '__main__':
    # traversal_email_files()
    # print(read_email_json(linkedin_email_json_path)[:20])
    # split_email_json()
    save_email_to_sqlserver()
