#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/22 9:35
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : merge_all_spider_dada.py
# @Software: PyCharm

import company_spider_3
import company_spider_4
import company_spider_5
import company_spider_6
import json
import conn_sql_server
import os

home_path = r'E:\work_all\topease\company_spider_merge'
all_company_json_path = os.path.join(home_path, r'all_company_data.json')
all_company_json_path_1 = os.path.join(home_path, r'all_company_data_1.json')
all_company_json_path_2 = os.path.join(home_path, r'all_company_data_2.json')

# company_id 计数标记
company_id = 0


def merge_all_json_data():
    """
    合并所有的json数据，统一格式
    :return:
    """

    all_data_json = []

    merge_spider_3_data(all_data_json)
    merge_spider_5_data(all_data_json)

    # OOM 异常

    # with open(all_company_json_path_1, 'w', encoding='utf8') as fp:
    #     fp.write(json.dumps(all_data_json))
    #
    # all_data_json = []

    merge_spider_4_data(all_data_json)
    merge_spider_6_data(all_data_json)

    # with open(all_company_json_path_2, 'w', encoding='utf8') as fp:
    #     fp.write(json.dumps(all_data_json))

    with open(all_company_json_path, 'w', encoding='utf8') as fp:
        fp.write('[')
        for item_dict in all_data_json:
            fp.write(json.dumps(item_dict) + ',')
        fp.seek(fp.tell()-1, 0)
        fp.write(']')


def merge_all_json_data_2():
    """
    合并all_data_part_1 和 all_data_part_2数据，统一格式
    :return:
    """
    all_data_json = []

    merge_spider_all_data_1(all_data_json)
    print('merge_spider_all_data_1', 'OK')
    merge_spider_all_data_2(all_data_json)
    print('merge_spider_all_data_2', 'OK')

    with open(all_company_json_path, 'w', encoding='utf8') as fp:
        fp.write('[')
        for item_dict in all_data_json:
            fp.write(json.dumps(item_dict) + ',')
        fp.seek(fp.tell()-1, 0)
        fp.write(']')


def read_all_company_json():
    """
    读取合并好的所有公司json
    :return:
    """
    with open(all_company_json_path, 'r', encoding='utf8') as fp:
        data = json.loads(fp.read())
    return data


def merge_spider_3_data(all_data_json):
    """
    合并spider_3 json数据
    :param all_data_json:
    :return:
    """
    global company_id
    data_3 = company_spider_3.read_company_desc_list_has_web_json()

    for item_dict in data_3:
        item_dict = conn_sql_server.check_dict(item_dict)

        company_id += 1
        temp_dict = dict()

        temp_dict['company_id'] = company_id
        temp_dict['_from_spider'] = 3
        temp_dict['Name'] = item_dict.get('company_name', '')
        temp_dict['Description'] = item_dict.get('company_description', '')
        temp_dict['Country'] = item_dict.get('company_country', '')
        temp_dict['City'] = item_dict.get('company_city', '')
        temp_dict['Address'] = item_dict.get('company_adrress', '')
        temp_dict['Website'] = item_dict.get('company_web_origin', '')
        temp_dict['MainProduct'] = item_dict.get('company_product', '')
        temp_dict['Telephone'] = check_phone(item_dict.get('company_telephone', ''))
        temp_dict['Type'] = 'buyer or seller'
        print(temp_dict['_from_spider'], company_id)

        all_data_json.append(temp_dict)
        # if company_id == 20:
        #     break


def merge_spider_4_data(all_data_json):
    """
    合并spider_4 json数据
    :param all_data_json:
    :return:
    """
    global company_id
    data_4 = company_spider_4.read_company_desc_has_phone_str_json()

    for item_dict in data_4:

        item_dict = conn_sql_server.check_dict(item_dict)

        company_id += 1
        temp_dict = dict()

        temp_dict['company_id'] = company_id
        temp_dict['_from_spider'] = 4
        temp_dict['Name'] = item_dict.get('company_name', '')
        temp_dict['Description'] = item_dict.get('company_desc', '')
        temp_dict['Country'] = item_dict.get('Country/Region', '')
        temp_dict['City'] = item_dict.get('Location', '')
        temp_dict['Address'] = item_dict.get('Address', '')
        temp_dict['Website'] = item_dict.get('Website', '')
        temp_dict['MainProduct'] = item_dict.get('Main Products', '')
        temp_dict['Fax'] = check_phone(item_dict.get('Fax Number', ''))
        temp_dict['Telephone'] = check_phone(item_dict.get('Telephone', ''))
        temp_dict['CustomerPhone'] = check_phone(item_dict.get('Mobilephone', ''))
        temp_dict['SalesVolume'] = item_dict.get('Total Annual Sales Volume', '')
        temp_dict['MainMarkets'] = item_dict.get('Main Markets', '')
        temp_dict['PostCode'] = item_dict.get('Zip/Post Code', '')
        temp_dict['BusinessType'] = item_dict.get('Business Type', '')
        temp_dict['YearStartExporting'] = item_dict.get('Year Start Exporting', '')
        temp_dict['ContactPerson'] = item_dict.get('Contact Person', '')
        temp_dict['JobTitle'] = item_dict.get('Job Title', '')
        temp_dict['OfficeAddress_Detail'] = item_dict.get('Operational Address', '')
        temp_dict['Department'] = item_dict.get('Department', '')
        temp_dict['TradeCapacity'] = item_dict.get('Trade Capacity', '')
        temp_dict['ProductionCapacity'] = item_dict.get('Production Capacity', '')
        temp_dict['AverageLeadTime'] = item_dict.get('Average Lead Time', '')
        temp_dict['ContractManufacturing'] = item_dict.get('Contract Manufacturing', '')
        temp_dict['RegisteredCapital'] = item_dict.get('Registered Capital', '')
        temp_dict['RDCapacity'] = item_dict.get('R&D; Capacity', '')
        temp_dict['LegalRepresentative'] = item_dict.get('Legal Representative / CEO', '')
        temp_dict['QCStaff'] = item_dict.get('No. of QC Staff', '')
        temp_dict['QualityControl'] = item_dict.get('Quality Control', '')
        temp_dict['YearEstablished'] = item_dict.get('Year Established', '')
        temp_dict['Certificates'] = item_dict.get('Certificates', '')
        temp_dict['Revenue'] = item_dict.get('Total Revenue', '')
        temp_dict['NumberOfEmployess'] = item_dict.get('Number Of Employess', '')
        temp_dict['Type'] = 'buyer or seller'
        print(temp_dict['_from_spider'], company_id)

        all_data_json.append(temp_dict)
        # if company_id == 40:
        #     break


def merge_spider_5_data(all_data_json):
    """
    合并spider_5 json数据
    :param all_data_json:
    :return:
    """
    global company_id
    data_5 = company_spider_5.read_company_desc_list_json()

    for item_dict in data_5:

        item_dict = conn_sql_server.check_dict(item_dict)

        company_id += 1
        temp_dict = dict()

        temp_dict['company_id'] = company_id
        temp_dict['_from_spider'] = 5
        temp_dict['Industry'] = item_dict.get('category', '')
        temp_dict['Name'] = item_dict.get('company_name', '')
        temp_dict['Introduction'] = item_dict.get('company_introduction', '')
        temp_dict['Description'] = item_dict.get('company_desc_en', '')
        temp_dict['Description_cn'] = item_dict.get('company_desc_cn', '')
        temp_dict['Country'] = item_dict.get('country_or_area_en', '')
        temp_dict['Country_cn'] = item_dict.get('country_or_area_cn', '')
        temp_dict['Address'] = item_dict.get('company_address', '')
        temp_dict['Website'] = item_dict.get('company_web', '')
        temp_dict['Email'] = item_dict.get('company_email', '')
        temp_dict['Fax'] = check_phone(item_dict.get('company_fax', ''))
        temp_dict['Telephone'] = check_phone(item_dict.get('company_telephone', ''))
        temp_dict['PostCode'] = item_dict.get('post_code', '')
        temp_dict['ContactPerson'] = item_dict.get('contact_person', '')
        temp_dict['NumberOfEmployess'] = item_dict.get('employee_count', '')
        temp_dict['Type'] = 'buyer'
        print(temp_dict['_from_spider'], company_id)

        all_data_json.append(temp_dict)
        # if company_id == 60:
        #     break


def merge_spider_6_data(all_data_json):
    """
    合并spider_6 json数据
    :param all_data_json:
    :return:
    """
    global company_id
    data_6 = company_spider_6.read_company_desc_list_json()

    for item_dict in data_6:

        item_dict = conn_sql_server.check_dict(item_dict)

        company_id += 1
        temp_dict = dict()

        temp_dict['company_id'] = company_id
        temp_dict['_from_spider'] = 6
        temp_dict['Industry'] = item_dict.get('industry_category', '')
        Name_1 = item_dict.get('Company', '')
        Name_2 = item_dict.get('Company Name (公司名称)', '')
        temp_dict['Name'] = Name_1 if Name_1 != '' else Name_2
        temp_dict['Description'] = item_dict.get('content_desc_en', '')
        temp_dict['Description_cn'] = item_dict.get('content_desc_cn', '')
        temp_dict['Country'] = item_dict.get('Country(国家地区)', '')
        Address_1 = item_dict.get('Address', '')
        Address_2 = item_dict.get('Address (地址)', '')
        temp_dict['Address'] = Address_1 if Address_1 != '' else Address_2
        Website_1 = item_dict.get('Website', '')
        Website_2 = item_dict.get('Website (网站)', '')
        temp_dict['Website'] = Website_1 if Website_1 != '' else Website_2
        temp_dict['Email'] = item_dict.get('Email Address (电子邮件)', '')
        Fax_1 = item_dict.get('Fax', '')
        Fax_2 = item_dict.get('Fax (传真)', '')
        temp_dict['Fax'] = check_phone(Fax_1 if Fax_1 != '' else Fax_2)
        Telephone_1 = item_dict.get('Tel', '')
        Telephone_2 = item_dict.get('Tel (电话)', '')
        temp_dict['Telephone'] = check_phone(Telephone_1 if Telephone_1 != '' else Telephone_2)
        ContactPerson_1 = item_dict.get('Contact Person', '')
        ContactPerson_2 = item_dict.get('Contact Person (联系人)', '')
        temp_dict['ContactPerson'] = ContactPerson_1 if ContactPerson_1 != '' else ContactPerson_2
        temp_dict['PurchaseProduct'] = item_dict.get('Purchase Product (采购产品)', '')
        temp_dict['Type'] = 'buyer'
        print(temp_dict['_from_spider'], company_id)

        all_data_json.append(temp_dict)
        # if company_id == 80:
        #     break


def merge_spider_all_data_1(all_data_json):
    """
    合并all_data_1
    :return:
    """
    with open(all_company_json_path_1, 'r', encoding='utf8') as fp:
        data_1 = json.loads(fp.read())
    all_data_json.extend(data_1)


def merge_spider_all_data_2(all_data_json):
    """
    合并all_data_2
    :return:
    """
    with open(all_company_json_path_2, 'r', encoding='utf8') as fp:
        data_2 = json.loads(fp.read())
    all_data_json.extend(data_2)


def check_phone(phone_str):
    """
    检查手机号码
    :return:
    """
    allow_char_list = [' ', '-', '(', ')', '+', '/', ',', ';']

    new_phone_str = ''
    for item_char in phone_str:
        if (not is_int(item_char)) and (item_char not in allow_char_list):
                item_char = ' '
        new_phone_str += item_char
    return new_phone_str


def is_int(char):
    """
    字符是否为数字
    :param char: 单字符
    :return:
    """
    try:
        int(char)
    except ValueError:
        return False
    else:
        return True


if __name__ == '__main__':
    # merge_all_json_data()
    # merge_all_json_data_2()
    # print(read_all_company_json()[:5])
    merge_all_json_data()
