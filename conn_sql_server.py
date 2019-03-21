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
import json
import merge_all_spider_data
import rakuten_spider_7
import company_spider_9
import rakuten_spider_10
import os
import datetime
import requests


# # settings.py 文件
# host = '192.168.19.110'
# user = 'sasa'
# password = 'sasa19990909!@#'
# database = 'LTCYT'
# charset = 'utf8'


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
    else:
        print('数据库连接成功')

    # save_spider_3_data_to_db(conn, cur)
    # save_spider_4_data_to_db(conn, cur)
    # save_spider_5_data_to_db(conn, cur)
    # save_spider_6_data_to_db(conn, cur)

    # save_spider_all_data_to_db(conn, cur)

    # save_rakuten_spider_shop_info_to_db(conn, cur)
    # save_rakuten_spider_products_info_to_db(conn, cur)

    # save_rakuten_spider_finally_shop_to_db(conn, cur)

    # google_key insert
    # save_google_key_to_db(conn, cur)
    check_del_google_key_to_db(conn, cur)

    # save_rakuten_spider_key_to_db(conn, cur)

    # save_kompass_company_spider_to_db(conn, cur)

    conn.close()


def check_del_google_key_to_db(conn, cur):
    """
    检查
    :param conn:
    :param cur:
    :return:
    """

    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
        'Connection': 'keep-alive'
    }

    def check_google_key(google_key, sleep_time=0):
        """
        检查googlekey是否可用
        :return:
        """
        time.sleep(sleep_time)

        # google_key = 'AIzaSyAUsnERWvgUrNKQy4YvHAaeg99HdhJLpTM'
        result = requests.get(f'https://maps.googleapis.com/maps/api/place/nearbysearch/json?'
                              f'location=-33.8670522,151.1957362&radius=5000&types=food'
                              f'&key={google_key}', headers=headers)
        status = json.loads(result.text).get('status').strip()
        print(status)

        if status == 'OK':
            return True
        else:
            return False

    # check google key流程
    sql_str = 'select ID, Name, CreateTime from GoogleKey'
    cur.execute(sql_str.encode('utf8'))
    server_google_key_tuple = cur.fetchall()

    delete_count = 0

    for item_data in server_google_key_tuple:
        print(f'=========={item_data}===============')
        item_data_id = item_data[0]
        item_data_google_key = item_data[1]

        try:
            if check_google_key(item_data_google_key, sleep_time=0.2) \
                    and check_google_key(item_data_google_key, sleep_time=0.2):
                print(f'{item_data} --- OK')
            else:
                if check_google_key(item_data_google_key, sleep_time=0.2):
                    continue
                # delete this data
                sql_del_str = f'DELETE FROM GoogleKey WHERE ID = {item_data_id}'
                cur.execute(sql_del_str)
                conn.commit()
                delete_count += 1
                print(f'delete ---- {sql_del_str}')
        except Exception as e:
            print(f'--- error --- {e}')
    print(f'delete number: {delete_count}')


def save_google_key_to_db(conn, cur):
    """
    保存google key 到数据库
    :param conn:
    :param cur:
    :return:
    """
    # 获取本地key集
    json_path = r'E:\googlekey\all_valid_google_key_list_2.json'
    with open(json_path, 'r', encoding='utf8') as fp:
        data = json.load(fp)

    # 获取服务器key集
    sql_str = 'select Name from GoogleKey'
    cur.execute(sql_str.encode('utf8'))
    server_google_key_tuple = cur.fetchall()
    server_google_key_list = [i[0] for i in server_google_key_tuple]

    print(f'data_len: {len(data)}')
    print(f'server_data_len: {len(server_google_key_list)}')

    # 去重
    new_google_key_set = set(data) - set(server_google_key_list)
    print(f'new_key_len: {len(new_google_key_set)}')

    # 可用新集
    new_data = list(new_google_key_set)

    for item_key in new_data:
        try:
            create_time = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
            sql_str = "insert into GoogleKey(Name, CreateTime) values(N'%s',N'%s')" % (item_key, create_time)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            print(sql_str)
        except Exception as e:
            print(f'--------- error --------- {e}')
            break


def save_rakuten_spider_shop_info_to_db(conn, cur):
    """
    存储乐天spider商家信息至数据库
    :param conn:
    :param cur:
    :return:
    """
    data = rakuten_spider_7.read_shop_info_json()
    print('data', 'OK')

    cur.execute("select top 1 company_md5 from rakuten_company_2018_05 order by id desc")
    server_company_md5 = cur.fetchone()
    print(server_company_md5)

    is_find = False
    if server_company_md5 is None:
        is_find = True

    error_count = 0
    count = 0

    for item_dict in data:

        company_md5 = item_dict.get('company_md5', '').replace("'", "''")

        if not is_find:
            if company_md5 == server_company_md5[0]:
                # print(company_md5)
                is_find = True
                continue
            else:
                continue

        company_href = item_dict.get('company_href', '').replace("'", "''")
        company_name = item_dict.get('company_name', '').replace("'", "''")
        company_address = item_dict.get('company_address', '').replace("'", "''")
        company_tel = item_dict.get('company_tel', '').replace("'", "''")
        company_fax = item_dict.get('company_fax', '').replace("'", "''")
        company_representative = item_dict.get('company_representative', '').replace("'", "''")
        company_operator = item_dict.get('company_operator', '').replace("'", "''")
        company_security_officer = item_dict.get('company_security_officer', '').replace("'", "''")
        company_email = item_dict.get('company_email', '').replace("'", "''")

        sql_str = None
        try:
            sql_str = "insert into rakuten_company_2018_05(company_md5,company_href,company_name," \
                      "company_address,company_tel,company_fax," \
                      "company_representative,company_operator,company_security_officer,company_email)" \
                      " values(N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s')" \
                      % (company_md5, company_href, company_name, company_address, company_tel, company_fax, company_representative,
                         company_operator, company_security_officer, company_email)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_md5, 'OK', ',当前 count:', count, ', error_count:', error_count)
            # time.sleep(0.005)
        except Exception as e:
            error_count += 1
            print(e, error_count, company_md5, '===================')
            print(sql_str)

            print(len(company_md5))
            print(len(company_href))
            print(len(company_name))
            print(len(company_address))
            print(len(company_tel))
            print(len(company_fax))
            print(len(company_representative))
            print(len(company_operator))
            print(len(company_security_officer))
            print(len(company_email))
            # raise e
        # break
    # conn.commit()
    print('count:', count, ', error_count:', error_count)


def save_rakuten_spider_products_info_to_db(conn, cur):
    """
    存储乐天spider产品信息至数据库
    :param conn:
    :param cur:
    :return:
    """
    data = rakuten_spider_7.read_products_json()
    print('data', 'OK')

    cur.execute("select top 1 product_href from rakuten_product_2018_05 order by id desc")
    server_product_href = cur.fetchone()
    print(server_product_href)

    is_find = False
    if server_product_href is None:
        is_find = True

    error_count = 0
    count = 0

    for item_dict in data:

        product_href = item_dict.get('product_href', '').replace("'", "''")

        if not is_find:
            if product_href == server_product_href[0]:
                # print(company_md5)
                is_find = True
                continue
            else:
                continue

        shop_md5 = item_dict.get('shop_md5', '').replace("'", "''")
        shop_name = item_dict.get('shop_name', '').replace("'", "''")
        shop_href = item_dict.get('shop_href', '').replace("'", "''")
        product_title = item_dict.get('product_title', '').replace("'", "''")

        product_price = item_dict.get('product_price', '').replace("'", "''")
        product_score = item_dict.get('product_score', '').replace("'", "''")
        product_legend = item_dict.get('product_legend', '').replace("'", "''")
        product_type_first = item_dict.get('product_type_first', '').replace("'", "''")
        product_type_second = item_dict.get('product_type_second', '').replace("'", "''")

        sql_str = None
        try:
            sql_str = "insert into rakuten_product_2018_05(shop_name,shop_href,product_title," \
                      "product_href,product_price," \
                      "product_score,product_legend,product_type_first,product_type_second,shop_md5)" \
                      " values(N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s')" \
                      % (shop_name, shop_href, product_title, product_href, product_price, product_score,
                         product_legend, product_type_first, product_type_second, shop_md5)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(product_href, 'OK', ',当前 count:', count, ', error_count:', error_count)
            # time.sleep(0.005)
        except Exception as e:
            error_count += 1
            print(e, error_count, product_href, '===================')
            print(sql_str)

            print(len(shop_name))
            print(len(shop_href))
            print(len(product_title))
            print(len(product_href))
            print(len(product_price))
            print(len(product_score))
            print(len(product_legend))
            print(len(product_type_first))
            print(len(product_type_second))
            print(len(shop_md5))
            # raise e

        # break
    # conn.commit()
    print('count:', count, ', error_count:', error_count)


def save_rakuten_spider_key_to_db(conn, cur):
    """
    乐天数据（根据关键字爬取）导入数据库
    :param conn:
    :param cur:
    :return:
    """
    rakuten_spider = rakuten_spider_10.Main().spider
    data_path = os.path.join(rakuten_spider.settings.key_json_dir_path, 'shop_info_list.json')
    with open(data_path, 'r', encoding='utf8') as fp:
        data = json.load(fp)

    key_word_list = rakuten_spider.settings.key_word_list

    error_insert_count = 0
    error_update_count = 0
    insert_count = 0
    update_count = 0
    count = 0

    for item_dict in data:
        # 查这一家店是否已存储
        # 已存储：读出，查询关键字列表是否出现在company_product_desc字段，
        #        若有则什么也不做，若无则添加新关键字至该字段，并更新数据（更新时间）
        # 未存储：直接插库
        print('=' * 30)

        company_website = item_dict.get('company_website', '').replace("'", "''")
        company_website = str(company_website).strip()

        if company_website == '':
            continue

        # [id], \
        # [company_name], \
        # [company_product_type], \
        # [company_product_desc], \
        # [company_address], \
        # [company_website], \
        # [company_tel], \
        # [company_fax], \
        # [company_email], \
        # [company_representative], \
        # [company_operator], \
        # [company_security_officer], \
        # [create_datetime], \
        # [update_datetime], \
        # [update_version]

        cur.execute("select top 1 [id], [company_name], [company_product_type], "
                    "[company_product_desc], [company_address], [company_website], [company_tel], "
                    "[company_fax], [company_email], [company_representative], [company_operator], "
                    "[company_security_officer], [create_datetime], [update_datetime], [update_version] "
                    "from ffd_cbec_company where data_origin_id='1' and company_website=N'%s'"
                    % (company_website,))
        server_data = cur.fetchone()

        company_country_id = 103
        company_product_desc = ','.join(key_word_list)
        company_product_type = ''
        company_name = item_dict.get('company_name', '').replace("'", "''")
        company_address = item_dict.get('company_address', '').replace("'", "''")
        company_tel = item_dict.get('company_tel', '').replace("'", "''")
        company_fax = item_dict.get('company_fax', '').replace("'", "''")
        company_representative = item_dict.get('company_representative', '').replace("'", "''")
        company_operator = item_dict.get('company_operator', '').replace("'", "''")
        company_security_officer = item_dict.get('company_security_officer', '').replace("'", "''")
        company_email = item_dict.get('company_email', '').replace("'", "''")

        data_origin_id = 1  # rakuten
        create_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        update_datetime = create_datetime
        update_version = 1

        if server_data is None:
            # 这一家数据库不存在
            # 直接插库

            # print(f'new data, server DB has not this data: {company_website}')

            if str(company_name).strip() == '':
                continue

            sql_str = None
            try:
                sql_str = "insert into ffd_cbec_company(data_origin_id,company_website,company_product_desc, " \
                          "company_product_type, company_name, company_address, company_tel, company_fax," \
                          "company_representative, company_operator, company_security_officer, company_email," \
                          "create_datetime, update_datetime, update_version, company_country_id)" \
                          " values(N'%d',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s'," \
                          "N'%s',N'%s',N'%s',N'%d')" \
                          % (data_origin_id, company_website, company_product_desc, company_product_type,
                             company_name, company_address, company_tel, company_fax, company_representative,
                             company_operator, company_security_officer, company_email, create_datetime,
                             update_datetime, update_version, company_country_id)
                cur.execute(sql_str.encode('utf8'))
                conn.commit()
                insert_count += 1
                count += 1
                print(f'insert {company_website} OK, now insert_count:{insert_count}, '
                      f'error_insert_count:{error_insert_count}')
                # time.sleep(0.005)
            except Exception as e:
                error_insert_count += 1
                print(e, error_insert_count, company_website, '++++++ insert error ++++++')
                print(sql_str)
            # break
        else:
            # 这一家在数据库
            # 查询关键字列表是否出现在company_product_desc字段，若有则什么也不做

            # print(f'old data, server DB has this data: {company_website}')
            server_company_product_desc = server_data[3]
            is_server_company_product_desc_has_key_word = False
            for item_key in key_word_list:
                if item_key in str(server_company_product_desc).split(','):
                    is_server_company_product_desc_has_key_word = True
                    break
                # if str(server_company_product_desc).find(item_key) >= 0:
                #     is_server_company_product_desc_has_key_word = True
                #     break
            if is_server_company_product_desc_has_key_word:
                # 服务器重复数据中已包含现关键词
                print('pass')
                # print(f'do nothing, server data has this key word')
                continue
            else:
                # 服务器重复数据中不包含现关键词，添加新关键字至该字段，并更新数据（更新联系方式、时间）

                # print(item_dict)
                # print(server_data)

                server_id = server_data[0]
                server_company_name = server_data[1].replace("'", "''")
                server_company_product_type = server_data[2].replace("'", "''")
                server_company_product_desc = server_data[3].replace("'", "''")
                server_company_address = server_data[4].replace("'", "''")
                # server_company_website = server_data[5].replace("'", "''")
                server_company_tel = server_data[6].replace("'", "''")
                server_company_fax = server_data[7].replace("'", "''")
                server_company_email = server_data[8].replace("'", "''")
                server_company_representative = server_data[9].replace("'", "''")
                server_company_operator = server_data[10].replace("'", "''")
                server_company_security_officer = server_data[11].replace("'", "''")
                # server_create_datetime = server_data[12]
                # server_update_datetime = server_data[13]
                server_update_version = server_data[14]

                # 更新旧数据字段
                company_name = comparison_between_old_and_new_data(server_company_name, company_name)
                company_product_type = comparison_between_old_and_new_data(server_company_product_type,
                                                                           company_product_type)
                company_product_desc = f'{server_company_product_desc},{company_product_desc}'.strip(',')
                company_address = comparison_between_old_and_new_data(server_company_address, company_address)
                # company_website = comparison_between_old_and_new_data(server_company_website, company_website)
                company_tel = comparison_between_old_and_new_data(server_company_tel, company_tel)
                company_fax = comparison_between_old_and_new_data(server_company_fax, company_fax)
                company_email = rakuten_merge_email(server_company_email, company_email)
                company_representative = comparison_between_old_and_new_data(server_company_representative,
                                                                             company_representative)
                company_operator = comparison_between_old_and_new_data(server_company_operator, company_operator)
                company_security_officer = comparison_between_old_and_new_data(server_company_security_officer,
                                                                               company_security_officer)
                # create_datetime = server_create_datetime
                update_datetime = update_datetime
                update_version = int(server_update_version) + 1 if server_update_version is not None else 2

                sql_str = None
                try:
                    sql_str = f"update ffd_cbec_company set " \
                              f"company_name=N'{company_name}', " \
                              f"company_product_type=N'{company_product_type}', " \
                              f"company_product_desc=N'{company_product_desc}', " \
                              f"company_address=N'{company_address}', " \
                              f"company_tel=N'{company_tel}', " \
                              f"company_fax=N'{company_fax}', " \
                              f"company_email=N'{company_email}', " \
                              f"company_representative=N'{company_representative}', " \
                              f"company_operator=N'{company_operator}', " \
                              f"company_security_officer=N'{company_security_officer}', " \
                              f"update_datetime=N'{update_datetime}', " \
                              f"update_version=N'{update_version}' " \
                              f"where id={server_id}"
                    # print(sql_str)
                    cur.execute(sql_str.encode('utf8'))
                    conn.commit()
                    update_count += 1
                    count += 1
                    print(f'update {company_website} OK, now update_count:{update_count}, '
                          f'error_update_count:{error_update_count}')
                except Exception as e:
                    error_update_count += 1
                    print(e, error_update_count, company_website, '++++++ update error ++++++')
                    print(sql_str)

                print(server_id)
                # break
        # break
    print(f'count:{count}, insert_conut:{insert_count}, update_count:{update_count}, '
          f'error_insert_count:{error_insert_count}, error_update_count:{error_update_count}')


def comparison_between_old_and_new_data(old_value, new_value):
    """
    新旧数据对比，优先选择有值数据
    :param old_value: 服务器端旧数据
    :param new_value: 新采集数据
    :return:
    """
    return old_value if new_value is None or new_value == '' else new_value


def rakuten_merge_email(old_value, new_value):
    """
    乐天，合并邮箱字段
    :param old_value:
    :param new_value:
    :return:
    """
    if old_value is None:
        old_value = ''
    if new_value is None:
        new_value = ''
    return ','.join(set(f'{old_value},{new_value}'.split(',')))


def save_rakuten_spider_finally_shop_to_db(conn, cur):
    """
    乐天数据整合商店公司库至数据库
    :param conn:
    :param cur:
    :return:
    """
    data = rakuten_spider_7.read_finally_shop_info_json()

    print('data', 'OK')

    cur.execute("select top 1 company_website from ffd_cbec_company where data_origin_id='1' order by id desc")
    server_company_website = cur.fetchone()
    print(server_company_website)

    is_find = False
    if server_company_website is None:
        is_find = True

    error_count = 0
    count = 0

    for item_dict in data:

        company_website = item_dict.get('company_website', '').replace("'", "''")

        if not is_find:
            if company_website == server_company_website[0]:
                # print(company_md5)
                is_find = True
                continue
            else:
                continue

        data_origin_id = '1'
        company_product_desc = item_dict.get('company_product_desc', '').replace("'", "''")
        company_product_type = item_dict.get('company_product_type', '').replace("'", "''")
        company_name = item_dict.get('company_name', '').replace("'", "''")
        company_address = item_dict.get('company_address', '').replace("'", "''")
        company_tel = item_dict.get('company_tel', '').replace("'", "''")
        company_fax = item_dict.get('company_fax', '').replace("'", "''")
        company_representative = item_dict.get('company_representative', '').replace("'", "''")
        company_operator = item_dict.get('company_operator', '').replace("'", "''")
        company_security_officer = item_dict.get('company_security_officer', '').replace("'", "''")
        company_email = item_dict.get('company_email', '').replace("'", "''")

        sql_str = None
        try:
            sql_str = "insert into ffd_cbec_company(data_origin_id,company_website,company_product_desc, " \
                      "company_product_type, company_name, company_address, company_tel, company_fax," \
                      "company_representative, company_operator, company_security_officer, company_email)" \
                      " values(N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s')" \
                      % (data_origin_id, company_website, company_product_desc, company_product_type,
                         company_name, company_address,
                         company_tel, company_fax, company_representative, company_operator,
                         company_security_officer, company_email)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_website, 'OK', ',当前 count:', count, ', error_count:', error_count)
            # time.sleep(0.005)
        except Exception as e:
            error_count += 1
            print(e, error_count, company_website, '===================')
            print(sql_str)

            print(len(company_website))
            print(len(company_product_desc))
            print(len(company_product_type))
            print(len(company_name))
            print(len(company_address))
            print(len(company_tel))
            print(len(company_fax))
            print(len(company_representative))
            print(len(company_operator))
            print(len(company_security_officer))
            print(len(company_email))
            # raise e
        # break
    # conn.commit()
    print('count:', count, ', error_count:', error_count)


def save_spider_all_data_to_db(conn, cur):
    """
    存储spider所有数据至数据库
    :return:
    """
    data = merge_all_spider_data.read_all_company_json()
    print('data OK **************')

    error_count = 0

    cur.execute("select top 1 company_id from Company3 order by ID desc")
    last_company_id_int = cur.fetchone()

    is_find = False
    if last_company_id_int is None:
        is_find = True

    count = 0
    for item_dict in data:

        item_dict = check_dict(item_dict)
        company_id = int(item_dict.get('company_id', ''))

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

        Industry = item_dict.get('Industry', '').replace("'", "''")
        Name = item_dict.get('Name', '').replace("'", "''")
        Introduction = item_dict.get('Introduction', '').replace("'", "''")
        Description = item_dict.get('Description', '').replace("'", "''")
        Description_cn = item_dict.get('Description_cn', '').replace("'", "''")
        Country = item_dict.get('Country', '').replace("'", "''")
        Country_cn = item_dict.get('Country_cn', '').replace("'", "''")
        City = item_dict.get('City', '').replace("'", "''")
        Address = item_dict.get('Address', '').replace("'", "''")
        Website = item_dict.get('Website', '').replace("'", "''")
        MainProduct = item_dict.get('MainProduct', '').replace("'", "''")
        Email = item_dict.get('Email', '').replace("'", "''")
        Fax = item_dict.get('Fax', '').replace("'", "''")
        Telephone = item_dict.get('Telephone', '').replace("'", "''")
        CustomerPhone = item_dict.get('CustomerPhone', '').replace("'", "''")
        SalesVolume = item_dict.get('SalesVolume', '').replace("'", "''")
        MainMarkets = item_dict.get('MainMarkets', '').replace("'", "''")
        PostCode = item_dict.get('PostCode', '').replace("'", "''")
        BusinessType = item_dict.get('BusinessType', '').replace("'", "''")
        YearStartExporting = item_dict.get('YearStartExporting', '').replace("'", "''")
        ContactPerson = item_dict.get('ContactPerson', '').replace("'", "''")
        JobTitle = item_dict.get('JobTitle', '').replace("'", "''")
        OfficeAddress_Detail = item_dict.get('OfficeAddress_Detail', '').replace("'", "''")
        Department = item_dict.get('Department', '').replace("'", "''")
        TradeCapacity = item_dict.get('TradeCapacity', '').replace("'", "''")
        ProductionCapacity = item_dict.get('ProductionCapacity', '').replace("'", "''")
        AverageLeadTime = item_dict.get('AverageLeadTime', '').replace("'", "''")
        ContractManufacturing = item_dict.get('ContractManufacturing', '').replace("'", "''")
        RegisteredCapital = item_dict.get('RegisteredCapital', '').replace("'", "''")
        RDCapacity = item_dict.get('RDCapacity', '').replace("'", "''")
        LegalRepresentative = item_dict.get('LegalRepresentative', '').replace("'", "''")
        QCStaff = item_dict.get('QCStaff', '').replace("'", "''")
        QualityControl = item_dict.get('QualityControl', '').replace("'", "''")
        YearEstablished = item_dict.get('YearEstablished', '').replace("'", "''")
        Certificates = item_dict.get('Certificates', '').replace("'", "''")
        Revenue = item_dict.get('Revenue', '').replace("'", "''")
        NumberOfEmployess = item_dict.get('NumberOfEmployess', '').replace("'", "''")
        PurchaseProduct = item_dict.get('PurchaseProduct', '').replace("'", "''")
        Type = 'buyer or seller'

        try:
            sql_str = "insert into Company3(company_id,Industry,Name,Introduction,Description,Description_cn,Country," \
                      "Country_cn,City,Address,Website," \
                      "MainProduct,Email,Fax,Telephone,CustomerPhone,SalesVolume,MainMarkets,PostCode,BusinessType," \
                      "YearStartExporting,ContactPerson,JobTitle,OfficeAddress_Detail,Department,TradeCapacity," \
                      "ProductionCapacity,AverageLeadTime,ContractManufacturing,RegisteredCapital,RDCapacity," \
                      "LegalRepresentative,QCStaff,QualityControl,YearEstablished,Certificates,Revenue," \
                      "NumberOfEmployess,PurchaseProduct,Type) values(N'%d',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s', \
                      N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s', N'%s',N'%s',\
                      N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s', \
                      N'%s',N'%s',N'%s')" \
                      % (company_id, Industry, Name, Introduction, Description, Description_cn, Country, Country_cn,
                         City, Address, Website, MainProduct, Email, Fax, Telephone, CustomerPhone, SalesVolume,
                         MainMarkets, PostCode, BusinessType, YearStartExporting, ContactPerson, JobTitle,
                         OfficeAddress_Detail, Department, TradeCapacity, ProductionCapacity,AverageLeadTime,
                         ContractManufacturing, RegisteredCapital, RDCapacity, LegalRepresentative, QCStaff,
                         QualityControl, YearEstablished, Certificates, Revenue, NumberOfEmployess, PurchaseProduct,
                         Type)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_id, 'OK', ',当前 count:', count, ', error_count:', error_count)

        except Exception as e:
            error_count += 1
            print(e, error_count, company_id, '===================')
            # raise e

        # finally:
        #     print(json.dumps(item_dict))
        #     break
    # conn.commit()
    print('count:', count, ', error_count:', error_count)


def save_spider_3_data_to_db(conn, cur):
    """
    存储spider3数据至数据库
    :return:
    """
    data_3 = company_spider_3.read_company_desc_list_has_web_json()
    print('data OK **************')

    error_count = 0

    cur.execute("select top 1 company_id from Company0 order by ID desc")
    last_company_id_int = cur.fetchone()

    is_find = False
    if last_company_id_int is None:
        is_find = True

    count = 0
    for item_dict in data_3:
        item_dict = check_dict(item_dict)

        company_id = int(item_dict.get('company_id', ''))
####################################################################
        # if company_id != 121332:
        #     continue
        # print(item_dict)

        if not is_find:
            if company_id == last_company_id_int[0]:
                print(company_id)
                is_find = True
                continue
            else:
                continue
####################################################################
        Name = item_dict.get('company_name', '').replace("'", "''")
        Description = item_dict.get('company_description', '').replace("'", "''")
        Country = item_dict.get('company_country', '').replace("'", "''")
        City = item_dict.get('company_city', '').replace("'", "''")
        Address = item_dict.get('company_adrress', '').replace("'", "''")
        Website = item_dict.get('company_web_origin', '').replace("'", "''")
        MainProduct = item_dict.get('company_product', '').replace("'", "''")
        Telephone = item_dict.get('company_telephone', '').replace("'", "''")
        Type = 'buyer or seller'

        try:
            sql_str = "insert into Company0(company_id,Name,Description,Country,City,Address,Website," +\
                      "MainProduct,Telephone,Type) values(N'%d',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s')"\
                      % (company_id, Name, Description, Country, City, Address, Website, MainProduct, Telephone, Type)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_id, 'OK', ',当前 count:', count, ', error_count:', error_count)
            # time.sleep(0.005)
        except Exception as e:
            error_count += 1
            print(e, error_count, company_id, '===================')
            # raise e
    # conn.commit()
    print('count:', count, ', error_count:', error_count)


def save_spider_4_data_to_db(conn, cur):
    """
    存储spider4数据至数据库
    :return:
    """
    data_4 = company_spider_4.read_company_desc_has_phone_str_json()
    print('data OK **************')

    base_company_id = 1000 * 10000

    error_count = 0

    cur.execute("select top 1 company_id from Company0 order by ID desc")
    last_company_id_int = cur.fetchone()

    is_find = False
    if last_company_id_int is None:
        is_find = True
    elif last_company_id_int[0] < base_company_id:
        is_find = True

    count = 0
    for item_dict in data_4:

        item_dict = check_dict(item_dict)
        company_id = int(item_dict.get('company_id', '')) + base_company_id

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
        Description = item_dict.get('company_desc', '').replace("'", "''")
        Country = item_dict.get('Country/Region', '').replace("'", "''")
        City = item_dict.get('Location', '').replace("'", "''")
        Address = item_dict.get('Address', '').replace("'", "''")
        Website = item_dict.get('Website', '').replace("'", "''")
        MainProduct = item_dict.get('Main Products', '').replace("'", "''")
        Fax = item_dict.get('Fax Number', '').replace("'", "''")
        Telephone = item_dict.get('Telephone', '').replace("'", "''")
        CustomerPhone = item_dict.get('Mobilephone', '').replace("'", "''")
        SalesVolume = item_dict.get('Total Annual Sales Volume', '').replace("'", "''")
        MainMarkets = item_dict.get('Main Markets', '').replace("'", "''")
        PostCode = item_dict.get('Zip/Post Code', '').replace("'", "''")
        BusinessType = item_dict.get('Business Type', '').replace("'", "''")
        YearStartExporting = item_dict.get('Year Start Exporting', '').replace("'", "''")
        ContactPerson = item_dict.get('Contact Person', '').replace("'", "''")
        JobTitle = item_dict.get('Job Title', '').replace("'", "''")
        OfficeAddress_Detail = item_dict.get('Operational Address', '').replace("'", "''")
        Department = item_dict.get('Department', '').replace("'", "''")
        TradeCapacity = item_dict.get('Trade Capacity', '').replace("'", "''")
        ProductionCapacity = item_dict.get('Production Capacity', '').replace("'", "''")
        AverageLeadTime = item_dict.get('Average Lead Time', '').replace("'", "''")
        ContractManufacturing = item_dict.get('Contract Manufacturing', '').replace("'", "''")
        RegisteredCapital = item_dict.get('Registered Capital', '').replace("'", "''")
        RDCapacity = item_dict.get('R&D; Capacity', '').replace("'", "''")
        LegalRepresentative = item_dict.get('Legal Representative / CEO', '').replace("'", "''")
        QCStaff = item_dict.get('No. of QC Staff', '').replace("'", "''")
        QualityControl = item_dict.get('Quality Control', '').replace("'", "''")
        YearEstablished = item_dict.get('Year Established', '').replace("'", "''")
        Certificates = item_dict.get('Certificates', '').replace("'", "''")
        Revenue = item_dict.get('Total Revenue', '').replace("'", "''")
        NumberOfEmployess = item_dict.get('Number Of Employess', '').replace("'", "''")
        Type = 'buyer or seller'

        try:
            sql_str = "insert into Company0(company_id,Name,Description,Country,City,Address,Website," \
                      "MainProduct,Fax,Telephone,CustomerPhone,SalesVolume,MainMarkets,PostCode,BusinessType," \
                      "YearStartExporting,ContactPerson,JobTitle,OfficeAddress_Detail,Department,TradeCapacity," \
                      "ProductionCapacity,AverageLeadTime,ContractManufacturing,RegisteredCapital,RDCapacity," \
                      "LegalRepresentative,QCStaff,QualityControl,YearEstablished,Certificates,Revenue," \
                      "NumberOfEmployess,Type) values(N'%d',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s'," \
                      "N'%s',N'%s',N'%s',N'%s', \
                      N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s'," \
                      "N'%s',N'%s',N'%s',N'%s',N'%s',N'%s', \
                      N'%s',N'%s',N'%s')" \
                      % (company_id, Name, Description, Country, City, Address, Website, MainProduct, Fax, Telephone,
                         CustomerPhone, SalesVolume, MainMarkets, PostCode, BusinessType, YearStartExporting,
                         ContactPerson, JobTitle, OfficeAddress_Detail, Department, TradeCapacity, ProductionCapacity,
                         AverageLeadTime, ContractManufacturing, RegisteredCapital, RDCapacity, LegalRepresentative,
                         QCStaff, QualityControl, YearEstablished, Certificates, Revenue, NumberOfEmployess, Type)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_id, 'OK', ',当前 count:', count, ', error_count:', error_count)

        except Exception as e:
            error_count += 1
            print(e, error_count, company_id, '===================')
            # raise e

        # finally:
        #     print(json.dumps(item_dict))
        #     break
    # conn.commit()
    print('count:', count, ', error_count:', error_count)


def save_spider_5_data_to_db(conn, cur):
    """
    存储spider5数据至数据库
    :return:
    """
    data_5 = company_spider_5.read_company_desc_list_json()
    print('data OK **************')

    base_company_id = 2000 * 10000

    error_count = 0

    cur.execute("SELECT TOP 1 company_id FROM Company0 ORDER BY ID DESC")
    last_company_id_int = cur.fetchone()

    is_find = False
    if last_company_id_int is None:
        is_find = True
    elif last_company_id_int[0] < base_company_id:
        is_find = True

    count = 0
    for item_dict in data_5:
        item_dict = check_dict(item_dict)

        company_id = int(item_dict.get('company_id', '')) + base_company_id

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

        Industry = item_dict.get('category', '').replace("'", "''")
        Name = item_dict.get('company_name', '').replace("'", "''")
        Introduction = item_dict.get('company_introduction', '').replace("'", "''")
        Description = item_dict.get('company_desc_en', '').replace("'", "''")
        Description_cn = item_dict.get('company_desc_cn', '').replace("'", "''")
        Country = item_dict.get('country_or_area_en', '').replace("'", "''")
        Country_cn = item_dict.get('country_or_area_cn', '').replace("'", "''")
        Address = item_dict.get('company_address', '').replace("'", "''")
        Website = item_dict.get('company_web', '').replace("'", "''")
        Email = item_dict.get('company_email', '').replace("'", "''")
        Fax = item_dict.get('company_fax', '').replace("'", "''")
        Telephone = item_dict.get('company_telephone', '').replace("'", "''")
        PostCode = item_dict.get('post_code', '').replace("'", "''")
        ContactPerson = item_dict.get('contact_person', '').replace("'", "''")
        NumberOfEmployess = item_dict.get('employee_count', '').replace("'", "''")
        Type = 'buyer'

        try:
            sql_str = "insert into Company0(company_id,Industry,Name,Introduction,Description,Description_cn," \
                      "Country,Country_cn,Address,Website,Email,Fax,Telephone,PostCode,ContactPerson," \
                      "NumberOfEmployess,Type) values(N'%d',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s'," \
                      "N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s')" \
                      % (company_id, Industry, Name, Introduction, Description, Description_cn, Country, Country_cn,
                         Address, Website, Email, Fax, Telephone, PostCode, ContactPerson, NumberOfEmployess, Type)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_id, 'OK', ',当前 count:', count, ', error_count:', error_count)

        except Exception as e:
            error_count += 1
            print(e, error_count, company_id, '===================')
            # raise e

        # finally:
        #     print(json.dumps(item_dict))
        #     break
    # conn.commit()
    print('count:', count, ', error_count:', error_count)


def save_spider_6_data_to_db(conn, cur):
    """
    存储spider6数据至数据库
    :return:
    """
    data_6 = company_spider_6.read_company_desc_list_json()
    print('data OK **************')

    base_company_id = 3000 * 10000

    error_count = 0

    cur.execute("SELECT TOP 1 company_id FROM Company0 ORDER BY ID DESC")
    last_company_id_int = cur.fetchone()

    is_find = False
    if last_company_id_int is None:
        is_find = True
    elif last_company_id_int[0] < base_company_id:
        is_find = True

    count = 0
    for item_dict in data_6:

        item_dict = check_dict(item_dict)

        company_id = int(item_dict.get('company_id', '0')) + base_company_id

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

        Industry = item_dict.get('industry_category', '').replace("'", "''")
        Name_1 = item_dict.get('Company', '').replace("'", "''")
        Name_2 = item_dict.get('Company Name (公司名称)', '').replace("'", "''")
        Name = Name_1 if Name_1 != '' else Name_2
        Description = item_dict.get('content_desc_en', '').replace("'", "''")
        Description_cn = item_dict.get('content_desc_cn', '').replace("'", "''")
        Country = item_dict.get('Country(国家地区)', '').replace("'", "''")
        Address_1 = item_dict.get('Address', '').replace("'", "''")
        Address_2 = item_dict.get('Address (地址)', '').replace("'", "''")
        Address = Address_1 if Address_1 != '' else Address_2
        Website_1 = item_dict.get('Website', '').replace("'", "''")
        Website_2 = item_dict.get('Website (网站)', '').replace("'", "''")
        Website = Website_1 if Website_1 != '' else Website_2
        Email = item_dict.get('Email Address (电子邮件)', '').replace("'", "''")
        Fax_1 = item_dict.get('Fax', '').replace("'", "''")
        Fax_2 = item_dict.get('Fax (传真)', '').replace("'", "''")
        Fax = Fax_1 if Fax_1 != '' else Fax_2
        Telephone_1 = item_dict.get('Tel', '').replace("'", "''")
        Telephone_2 = item_dict.get('Tel (电话)', '').replace("'", "''")
        Telephone = Telephone_1 if Telephone_1 != '' else Telephone_2
        ContactPerson_1 = item_dict.get('Contact Person', '').replace("'", "''")
        ContactPerson_2 = item_dict.get('Contact Person (联系人)', '').replace("'", "''")
        ContactPerson = ContactPerson_1 if ContactPerson_1 != '' else ContactPerson_2
        PurchaseProduct = item_dict.get('Purchase Product (采购产品)', '').replace("'", "''")
        Type = 'buyer'

        try:
            sql_str = "insert into Company0(company_id,Industry,Name,Description,Description_cn," \
                      "Country,Address,Website,Email,Fax,Telephone,ContactPerson,PurchaseProduct," \
                      "Type) values(N'%d',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s'," \
                      "N'%s',N'%s',N'%s',N'%s')" \
                      % (company_id, Industry, Name, Description, Description_cn, Country,
                         Address, Website, Email, Fax, Telephone, ContactPerson, PurchaseProduct, Type)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_id, 'OK', ',当前 count:', count, ', error_count:', error_count)

        except Exception as e:
            error_count += 1
            print(e, error_count, company_id, '===================')
            # raise e

        # finally:
        #     print(json.dumps(item_dict))
        #     break
    # conn.commit()
    print('count:', count, ', error_count:', error_count)


def check_str(_str):
    """
    检查字符串 \n, \xa0, \xc2, \u3000 等特殊字符
    :param _str:
    :return:
    """
    problem_str_list = ['\n', '\xa0', '\xc2', '\u3000', '<br />', '&nbsp;']
    for item_problem in problem_str_list:
        _str = _str.replace(item_problem, ' ')

    strip_str_list = [',', ' ']
    for item_strip in strip_str_list:
        _str = _str.strip(item_strip)
    return _str


def check_dict(_dict):
    """
    检查字典，修正字符串，列表等数据
    :param _dict:
    :return:
    """
    for key, value in _dict.items():
        if isinstance(value, list):
            value = ','.join(value)

        if isinstance(value, str):
            _dict[key] = check_str(value)

    return _dict


def check_problem_data():
    """
    问题数据检查
    :return:
    """
    data = company_spider_3.read_company_desc_list_has_web_json()
    # data = company_spider_4.read_company_desc_has_phone_str_json()
    for item_dict in data:
        company_id = int(item_dict.get('company_id', '0'))
        # 837345 121332 232092
        if company_id == 797306:
            print(item_dict)
            print(check_dict(item_dict))
            break


def check_kompass_company_is_exists(conn, cur, company_id_str):
    """
    检查kompass该公司是否存在
    :return:
    """
    sql = "SELECT TOP 1 company_id_str FROM ffd_b2b_company WHERE company_id_str='{0}'".format(company_id_str,)
    # print(sql)
    cur.execute(sql)
    server_company_id_str = cur.fetchone()

    if server_company_id_str is None:
        return False

    return True


def save_kompass_company_spider_to_db(conn, cur):
    """
    kompass公司信息存储至sqlserver
    :param conn:
    :param cur:
    :return:
    """
    data = company_spider_9.read_product_company_list_json()
    print('len(data)=', len(data))

    count = 0
    error_count = 0
    error_id_list = list()
    exists_id_list = list()
    none_name_list = list()
    for i in data:
        i = dict(i)

        company_id_str = i.get('company_id_str', '').replace("'", "''")
        company_name = i.get('company_name', '').replace("'", "''")

        if company_name == '':
            none_name_list.append(company_id_str)
            continue

        if check_kompass_company_is_exists(conn, cur, company_id_str):
            exists_id_list.append(company_id_str)
            continue

        file_name = i.get('file_name', '').replace("'", "''")
        company_is_premium = i.get('company_is_premium', 0)
        company_all_address = i.get('company_all_address', '').replace("'", "''")
        company_street_address = i.get('company_street_address', '').replace("'", "''")
        company_city_address = i.get('company_city_address', '').replace("'", "''")
        company_country_address = i.get('company_country_address', '').replace("'", "''")
        company_phone = i.get('company_phone', '').replace("'", "''")
        company_country_short = i.get('company_country_short', '').replace("'", "''")
        company_latitude = i.get('company_latitude', '').replace("'", "''")
        company_longitude = i.get('company_longitude', '').replace("'", "''")

        company_presentation = i.get('company_presentation', dict())
        company_website = company_presentation.get('company_general_info', dict()).get('Website', '').replace("'", "''")
        _company_website_2 = i.get('company_website', '').replace("'", "''")
        company_summary = company_presentation.get('company_summary', '').replace("'", "''")
        company_fax = company_presentation.get('company_general_info', dict()).get('Fax', '').replace("'", "''")

        company_presentation.pop('company_summary', '')
        company_keynumbers = i.get('company_keynumbers', dict())
        company_executives = i.get('company_executives', dict())
        company_activities = i.get('company_activities', dict())

        company_presentation = json.dumps(company_presentation).replace("'", "''")
        company_keynumbers = json.dumps(company_keynumbers).replace("'", "''")
        company_executives = json.dumps(company_executives).replace("'", "''")
        company_activities = json.dumps(company_activities).replace("'", "''")

        ###

        data_origin_id = 2  # kompass
        create_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        update_datetime = create_datetime
        update_version = 1
        create_tag = company_spider_9.search_text

        # print(data_origin_id)
        # print(file_name)
        # print(company_id_str)
        # print(company_name)
        # print(company_is_premium)
        # print(company_all_address)
        # print(company_street_address)
        # print(company_city_address)
        # print(company_country_address)
        # print(company_phone)
        # print(company_website)
        # print(company_summary)
        # print(company_fax)
        # print(company_presentation)
        # print(company_keynumbers)
        # print(company_executives)
        # print(company_activities)
        # print(create_datetime)
        # print(update_datetime)
        # print(update_version)
        # print(create_tag)

        try:
            sql_str = "insert into ffd_b2b_company(data_origin_id,file_name,company_id_str,company_name," \
                      "company_is_premium,company_all_address,company_street_address,company_city_address," \
                      "company_country_address,company_phone,company_website,company_summary,company_fax," \
                      "company_presentation,company_keynumbers,company_executives,company_activities," \
                      "create_datetime,update_datetime,update_version,create_tag,company_country_short," \
                      "company_latitude,company_longitude) " \
                      "values(N'%d',N'%s',N'%s',N'%s',N'%d',N'%s',N'%s',N'%s',N'%s',N'%s'," \
                      "N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%d', N'%s'," \
                      "N'%s', N'%s', N'%s')" \
                      % (data_origin_id, file_name, company_id_str, company_name
                         , company_is_premium, company_all_address, company_street_address, company_city_address
                         , company_country_address, company_phone, company_website, company_summary, company_fax
                         , company_presentation, company_keynumbers, company_executives, company_activities
                         , create_datetime, update_datetime, update_version, create_tag, company_country_short
                         , company_latitude, company_longitude)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_id_str, create_tag, 'OK', ',当前 count:', count, ', error_count:', error_count)

        except Exception as e:
            error_count += 1
            print(e, error_count, company_id_str, create_tag, '===================')
            error_id_list.append(company_id_str)
            # raise e

            # if count > 20:
            #     break
        # break
    print('异常数据id列表：', len(error_id_list), error_id_list)
    print('已存在数据id列表：', len(exists_id_list), exists_id_list)
    print('公司名空的id列表：', len(none_name_list), none_name_list)


if __name__ == '__main__':
    save_to_sql_server()
    # check_problem_data()
