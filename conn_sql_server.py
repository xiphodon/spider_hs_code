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

    # save_spider_3_data_to_db(conn, cur)
    save_spider_4_data_to_db(conn, cur)

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


def save_spider_4_data_to_db(conn, cur):
    """
    存储spider4数据至数据库
    :return:
    """
    data_4 = company_spider_4.read_company_desc_has_phone_str_json()
    print('data OK **************')

    base_company_id = 1000 * 10000

    error_count = 0

    cur.execute("select top 1 company_id from Company order by ID desc")
    last_company_id_int = cur.fetchone()

    is_find = False
    if last_company_id_int is None:
        is_find = True
    elif last_company_id_int[0] < base_company_id:
        is_find = True

    count = 0
    for item_dict in data_4:
        # if count == 100:
        #     break

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
            sql_str = "insert into Company(company_id,Name,Description,Country,City,Address,Website," + \
                      "MainProduct,Fax,Telephone,CustomerPhone,SalesVolume,MainMarkets,PostCode,BusinessType," + \
                      "YearStartExporting,ContactPerson,JobTitle,OfficeAddress_Detail,Department,TradeCapacity," + \
                      "ProductionCapacity,AverageLeadTime,ContractManufacturing,RegisteredCapital,RDCapacity," + \
                      "LegalRepresentative,QCStaff,QualityControl,YearEstablished,Certificates,Revenue," + \
                      "NumberOfEmployess,Type) values('%d','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
                      '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', \
                      '%s','%s','%s')" \
                      % (company_id, Name, Description, Country, City, Address, Website, MainProduct, Fax, Telephone,
                         CustomerPhone, SalesVolume, MainMarkets, PostCode, BusinessType, YearStartExporting,
                         ContactPerson, JobTitle, OfficeAddress_Detail, Department, TradeCapacity, ProductionCapacity,
                         AverageLeadTime, ContractManufacturing, RegisteredCapital, RDCapacity, LegalRepresentative,
                         QCStaff, QualityControl, YearEstablished, Certificates, Revenue, NumberOfEmployess, Type)
            cur.execute(sql_str.encode('utf8'))
            conn.commit()
            count += 1
            print(company_id, 'OK')

        except Exception as e:
            error_count += 1
            print(e, error_count, company_id, '===================')
            # raise e

        # finally:
        #     print(json.dumps(item_dict))
        #     break
    # conn.commit()
    print('count:', count, ', error_count:', error_count)


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
