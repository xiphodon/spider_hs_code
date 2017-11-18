#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/17 15:05
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : conn_sql_server.py
# @Software: PyCharm

import pymssql
import settings
import time
import company_spider_3
import company_spider_4
import company_spider_5
import company_spider_6


def save_to_sql_server():
    """
    存数据至sql——server
    :return:
    """
    conn = pymssql.connect(host=settings.host, user=settings.user, password=settings.password,
                           database=settings.database, charset=settings.charset)
    cur = conn.cursor()

    if not cur:
        raise (NameError, "数据库连接失败")

    save_spider_3_data_to_db(conn, cur)
    # cur.execute("select top 1 company_id from Company order by ID desc")
    # print(cur.fetchone()[0])
    #
    # for i in range(300, 400):
    #     cur.execute("insert into Company(Industry,Name,Introduction,company_id) values('chanye%d','公司名称%d','gongsi介绍%d',%d)" % (i, i, i, i))
    #     conn.commit()

    conn.close()


def save_spider_3_data_to_db(conn, cur):
    """
    存储spider3数据至数据库
    :return:
    """
    data_3 = company_spider_3.read_company_desc_list_has_web_json()
    print('data OK **************')

    error_count = 0

    cur.execute("select top 1 company_id from Company order by ID desc")
    last_company_id_int = cur.fetchone()

    is_find = False
    if last_company_id_int is None:
        is_find = True

    count = 0
    for item_dict in data_3:
        # if count == 100:
        #     break

        company_id = int(item_dict.get('company_id', '-1'))

        # if company_id != 1000189:
        #     continue
        # print(item_dict)

        if not is_find:
            if company_id == last_company_id_int[0]:
                print(company_id)
                is_find = True
                continue
            else:
                continue

        Name = item_dict.get('company_name', '').replace("'", "''")
        Description = item_dict.get('company_description', '').replace("'", "''")
        Country = item_dict.get('company_country', '').replace("'", "''")
        City = item_dict.get('company_city', '').replace("'", "''")
        Address = item_dict.get('company_adrress', '').replace("'", "''")
        Website = item_dict.get('company_web_origin', '').replace("'", "''")
        MainProduct = (','.join(item_dict.get('company_product', []))).replace("'", "''")
        Telephone = item_dict.get('company_telephone', '').replace("'", "''")
        Type = 'buyer or seller'

        try:
            sql_str = "insert into Company(company_id,Name,Description,Country,City,Address,Website," +\
                      "MainProduct,Telephone,Type) values('%d','%s','%s','%s','%s','%s','%s','%s','%s','%s')"\
                      % (company_id, Name, Description, Country, City, Address, Website, MainProduct, Telephone, Type)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_id, 'OK')
            # time.sleep(0.005)
        except Exception as e:
            error_count += 1
            print(e, error_count, company_id, '===================')
            # raise e
    # conn.commit()
    print('count:', count, ', error_count:', error_count)






def save_spider_4_data_to_db():
    """
    存储spider4数据至数据库
    :return:
    """


def save_spider_5_data_to_db():
    """
    存储spider5数据至数据库
    :return:
    """


def save_spider_6_data_to_db():
    """
    存储spider6数据至数据库
    :return:
    """


if __name__ == '__main__':
    save_to_sql_server()
