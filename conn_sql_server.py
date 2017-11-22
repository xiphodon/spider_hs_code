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

    # save_spider_3_data_to_db(conn, cur)
    # save_spider_4_data_to_db(conn, cur)
    # save_spider_5_data_to_db(conn, cur)
    save_spider_6_data_to_db(conn, cur)

    conn.close()


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
                      "NumberOfEmployess,Type) values(N'%d',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s', \
                      N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s', \
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
                      "NumberOfEmployess,Type) values(N'%d',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s'," \
                      "N'%s',N'%s',N'%s',N'%s',N'%s')" \
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
                      "Type) values(N'%d',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s')" \
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


if __name__ == '__main__':
    save_to_sql_server()
    # check_problem_data()
