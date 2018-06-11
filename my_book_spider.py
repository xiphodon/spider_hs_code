#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/11 15:23
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : my_book_spider.py
# @Software: PyCharm

import requests
from lxml import etree
import os
import json
import time

home_url = r'https://www.wuxiaworld.com'

home_path = r'E:\work_all\topease\my_book_spider'
chapter_json_path = os.path.join(home_path, 'chapter_list.json')

book_text_path = os.path.join(home_path, 'coiling-dragon.txt')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
    'Connection': 'keep-alive'
}


def while_request_get(page_url, times=1000):
    """
    循环请求
    :return:
    """
    while_times = 0
    while True:
        try:
            result = requests.get(page_url, headers=headers, timeout=20)
            # result = requests.get(page_url, headers=headers, proxies=proxies, timeout=5)
        except Exception as e:
            if while_times < times:
                while_times += 1
                print('**********', '尝试重新链接', while_times, '次:', page_url.replace(home_url, ''))
                continue
            else:
                raise e
        else:
            return result


def get_book_chapter_json():
    """
    存取该书籍章节为json
    :return:
    """

    if os.path.exists(chapter_json_path):
        with open(chapter_json_path, 'r', encoding='utf8') as fp:
            data_json = json.loads(fp.read())
        print('get chapter json from file')
        return data_json

    result = while_request_get(home_url + '/novel/coiling-dragon')

    seletor = etree.HTML(result.text)

    chapter_a_list = seletor.xpath('.//div[@id="collapse-0"]//ul/li[@class="chapter-item"]/a')

    chapter_list_json = list()
    for a_node in chapter_a_list:
        chapter_href = ''.join(a_node.xpath('./@href'))
        chapter_name = ''.join(a_node.xpath('./span/text()'))
        chapter_list_json.append({
            'chapter_name': chapter_name,
            'chapter_href': home_url + chapter_href
        })

    with open(chapter_json_path, 'w', encoding='utf8') as fp:
        fp.write(json.dumps(chapter_list_json))

    print('get chapter json from web')
    return chapter_list_json


def download_chapter_content_to_file(data_json):
    """
    下载章节内容至文件
    :param data_json:
    :return:
    """

    book_content_text = ''

    for item_chapter in data_json:
        chapter_name = item_chapter['chapter_name']
        chapter_href = item_chapter['chapter_href']

        print(chapter_name)

        result = while_request_get(chapter_href)

        seletor = etree.HTML(result.text)

        # chapter_title = ''.join(seletor.xpath('.//div[@class="p-15"]//div/h4/text()'))

        chapter_content_p_list = seletor.xpath('.//div[@class="p-15"]/div[@class="fr-view"]/p')

        chapter_content_text = ''
        for p_node in chapter_content_p_list:
            p_node_text = p_node.xpath('string()')

            if p_node_text.strip() == 'Previous Chapter':
                continue

            chapter_content_text += p_node_text + '\n'

        book_content_text += chapter_content_text

        print(len(chapter_content_text))
        time.sleep(0.5)

    with open(book_text_path, 'w', encoding='utf8') as fp:
        fp.write(book_content_text)


def start():
    """
    启动入口
    :return:
    """
    data_json = get_book_chapter_json()
    download_chapter_content_to_file(data_json)


if __name__ == '__main__':
    start()
