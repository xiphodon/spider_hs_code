#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/4/29 9:50
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : clues.py
# @Software: PyCharm
import json
from typing import Optional

import pandas as pd
import pymssql
import settings
import hashlib


def read_excel_data(file_path):
    """
    读取excel数据
    :param file_path:
    :return:
    """
    remarks_dict = dict()
    excel_data = pd.read_excel(file_path)
    for i in range(len(excel_data)):
        row = excel_data.iloc[i]
        remarks_dict[row['info']] = row['remarks']
    print(remarks_dict)
    print(len(remarks_dict))
    return remarks_dict


def read_db_data():
    """
    读取数据库数据
    :return:
    """
    conn = pymssql.connect(host=settings.clues_host, user=settings.clues_user, password=settings.clues_password,
                           database=settings.clues_database, charset=settings.charset)
    cur = conn.cursor()

    if not cur:
        raise (NameError, "数据库连接失败")
    else:
        print('数据库连接成功')

    remarks_dict = read_excel_data(r'./data/clues.xlsx')

    sql_str = 'select id, company_name, phone_info from oa_sales_leads'
    cur.execute(sql_str.encode('utf8'))
    db_data = cur.fetchall()

    for item in db_data:
        company_id = item[0]
        company_name_and_phone = '-'.join(item[1:])
        md5_info = hashlib.md5(company_name_and_phone.encode('utf8')).hexdigest()

        if md5_info in remarks_dict:
            remarks = remarks_dict[md5_info]
            sql_str = f'update oa_sales_leads set remarks=\'{remarks}\' where id={company_id}'
            # print(sql_str)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            print(company_id, " OK")


def read_clues_db():
    """
    读取销售线索
    :return:
    """
    conn = pymssql.connect(host=settings.clues_host, user=settings.clues_user, password=settings.clues_password,
                           database=settings.clues_database, charset=settings.charset)
    cur = conn.cursor()

    if not cur:
        raise (NameError, "数据库连接失败")
    else:
        print('数据库连接成功')

    sql_str = 'select id, contact_info, clean_user_id, clean_date ' \
              'from oa_sales_leads where clean_status=2 and id not in ' \
              '(select distinct(sl_id) from oa_sales_leads_clean_contacts)'

    cur.execute(sql_str.encode('utf8'))
    db_data = cur.fetchall()

    clues = list()
    for item in db_data:
        item_id, contact_info, clean_user_id, clean_date = item

        item_dict = dict()

        contact_info_split = str(contact_info).split(',')
        contact_info_list = list()
        for item_contact in contact_info_split:
            contact_dict = dict()
            item_contact = item_contact.strip()
            item_contact_split = item_contact.split(':')
            if len(item_contact_split) == 2:
                contact_type = item_contact_split[0]
                contact_desc = item_contact_split[1]
                contact_desc_split = contact_desc.split('|')
                if len(contact_desc_split) == 3:
                    contact_name = contact_desc_split[0]
                    contact_phone = contact_desc_split[1]
                    clean_verify = contact_desc_split[2]

                    contact_dict['contact_type'] = contact_type.strip()
                    contact_dict['contact_name'] = contact_name.strip()
                    contact_dict['contact_phone'] = contact_phone.strip()
                    contact_dict['clean_verify'] = clean_verify.strip()
                    contact_info_list.append(contact_dict)
        if len(contact_info_list) > 0:
            item_dict['id'] = item_id
            item_dict['clean_user_id'] = clean_user_id
            item_dict['clean_date'] = clean_date.strip()
            item_dict['contact_info_list'] = contact_info_list
            clues.append(item_dict)
            print(item_dict)
    if len(clues) > 0:
        with open('./data/clues.txt', 'w', encoding='utf8') as fp:
            json.dump(clues, fp)
    print(f'线索共计{len(clues)}条')


def read_clues_json_file():
    """
    读取销售线索json文件
    :return:
    """

    contact_type_dict = {
        '法人': 1,
        '股东': 2,
        '外贸经理': 3,
        '外贸业务员': 4,
        '财务': 5,
        '其他': 6
    }

    clean_verify_dict = {
        '已验证': 1,
        '未验证': 0
    }

    with open('./data/clues.txt', 'r', encoding='utf8') as fp:
        clues = json.load(fp)

    conn = pymssql.connect(host=settings.clues_host_t, user=settings.clues_user_t, password=settings.clues_password_t,
                           database=settings.clues_database_t, charset=settings.charset)
    # conn = pymssql.connect(host=settings.clues_host, user=settings.clues_user, password=settings.clues_password,
    #                        database=settings.clues_database, charset=settings.charset)
    cur = conn.cursor()
    if not cur:
        raise (NameError, "数据库连接失败")
    else:
        print('数据库连接成功')

    count = 0
    for item in clues:
        sl_id = int(item['id'])
        create_by = int(item['clean_user_id'])
        create_date = item['clean_date']
        contact_info_list = item['contact_info_list']

        for contact in contact_info_list:
            contact_type = int(contact_type_dict[contact['contact_type']])
            contact_name = str(contact['contact_name']).strip('-')
            contact_phone = contact['contact_phone']
            clean_verify = int(clean_verify_dict[contact['clean_verify']])

            print(sl_id, create_by, create_date, contact_type, contact_name, contact_phone, clean_verify)

            sql_str = (
                "insert into oa_sales_leads_clean_contacts" +
                "(sl_id, contact_type, contact_name, contact_phone, " +
                "clean_verify, sales_verify, clean_verify_date, sales_verify_date, " +
                "create_by, create_date, update_by, update_date, " +
                "remarks, del_flag, clean_verify_by, sales_verify_by) " +
                "values(" +
                "%d, %d, %s, %s, " +
                "%d, %d, %s, %s, " +
                "%d, %s, %d, %s," +
                "%s, %s, %d, %d)"
            ).replace("'", "''")

            params = (
                sl_id, contact_type, contact_name, contact_phone,
                clean_verify, None, create_date if clean_verify == 1 else None, None,
                create_by, create_date, create_by, create_date,
                None, 0, create_by if clean_verify == 1 else None, None
            )
            print(sql_str)
            print(params)
            cur.execute(sql_str.encode('utf8'), params)
            conn.commit()
            count += 1
    print(f"count: {count}")


if __name__ == '__main__':
    # data = read_excel_data(r'./data/clues.xlsx')
    # read_db_data()
    # read_clues_db()
    read_clues_json_file()
