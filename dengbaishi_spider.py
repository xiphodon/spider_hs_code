#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/7/13 23:43
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : dengbaishi_spider.py
# @Software: PyCharm
import json
import os
import random
import time

import requests


class DBSSpider:
    """
    dbs爬虫
    """
    home_path = r'E:\dbs_spider'

    def __init__(self):
        """
        初始化
        """
        if not os.path.exists(self.home_path):
            os.mkdir(self.home_path)
        self.sess_request = requests.session()

    def download_company_list(self, page_count: int):
        """
        下载公司列表
        :param page_count:
        :return:
        """
        args = {
            'cookie': '_ga=GA1.2.614541371.1626176202; _gid=GA1.2.1437380845.1626176202; LOGONID=AV_WR5RzJOg41cIBiwl; OSPWD="ayn/IhIQ/DVC4a95Lx+bvGi7jpQ="; SITE=Topeasedemo; USERID=AV_WR5RzJOg41cIBiwl; ak_bmsc=6A9195C8EEC63C055F5D2DA637AFF782~000000000000000000000000000000~YAAQ46zbF0HMPPB5AQAAviyEoAwr1QpGmc8eBmdfD02Dsp3jmIakwto64mc0BUgiA3zRWW5wSedJiaCxxdq3KJRCrUrRvvBvsSuJupqo+9dvPT4Jdu0m8r4TAhLQva4U9FpGoL9IbcHwPgxOvNWuSeRodFc7C31L3zrXQfTvgpu89pmtfMeAeaNA7DOpJl7rgYmGjReH627Lup+N6bIXI2jpzR7YInoxQ9EL4xAqzvdqMlCZoeFgh5mq+onWu/jb4GB2u6kl/YdYt1riefC9cdp7Kxcp45aZZ7F/JlXlCfRza2/cbCcmJR9l3WhRqhLbfETypFKcPJewyxSQogCwrfZzpvd6VZm55zxSY9K056dNf5y02V89y6Ym2mY2n1TuWbglAPKbELVfvMoyFw==; JSESSIONID=82817A486AA46A98E7B62FFC7819C8EC.prd2-av-app02; ext_id=MWUUTCNG3NU2N1ZHUMJGZCHFBMAI1GYK; OSMACH=11; RT="z=1&dm=app.dnbhoovers.com&si=d6c44847-e1f3-4183-a1c4-4000d3449c48&ss=kr1zblzy&sl=n&tt=1oo1&obo=e&rl=1"; bm_sv=BA937953BECC38649A4FD6A79695A4A4~I2POXt4L7U7qUB0CIiRkOLd0xoZGdQpJW69KR3UNmaArdv1AOHpSKcEPrUTiKt4+R5bfdY9vA15h/bmcICgExMEZZHjA0TQ3VbRHV/tjTx4G+uvSqZ5JOmqamnETjxCM1WuMpa8zwSBA9Wq+sc95kUO0Uf4vlxSQVbwRwPfYsBI=',
        }

        headers = {
            'authority': 'app.dnbhoovers.com',
            'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'x-requested-with': 'XMLHttpRequest',
            'sec-ch-ua-mobile': '?0',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36',
            'content-type': 'application/json',
            'origin': 'https://app.dnbhoovers.com',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://app.dnbhoovers.com/search/saved/8537c586-a0d7-4915-8566-f22f1919de08',
            'accept-language': 'zh-CN,zh;q=0.9',
        }

        for i in range(page_count):
            payload = '{"query":"","searchWeight":0,"filters":[{"geoFilter":{"or":[{"exclude":false,"type":"country","label":"Thailand","value":"b6f6463d-714e-3cce-be02-68f29de908d8"}]},"industryFilter":{"values":["6c52c30a-0d1a-3cf3-9aaa-62290fc8edb3"],"meta":{"values":[{"value":"6c52c30a-0d1a-3cf3-9aaa-62290fc8edb3","label":"5122 Drugs, proprietaries, and sundries","id":"6c52c30a-0d1a-3cf3-9aaa-62290fc8edb3","checked":true}],"source":"GDSSIC","primary":true},"primary":true}}],"aggs":{},"from":' + str(i * 25) + ',"size":25,"sortBy":[{"company":[{"numEmployees_l":"desc"}]}],"types":["company"]}'
            if i > 0:
                args['hpk'] = f'{int(time.time() * 1000)}-{i * 25}'
            new_headers = dict()
            new_headers.update(headers)
            new_headers.update(args)
            self._download_company_list_json(new_headers, payload, i)
            print(f'{i + 1}/{page_count}', 'OK')
            time.sleep(random.random() * 2 + 1)

    def _download_company_list_json(self, headers: dict, payload: str, page_no: int):
        """
        下载公司列表json数据
        :param headers:
        :param payload:
        :param page_no:
        :return:
        """
        url = 'https://app.dnbhoovers.com/api/search'
        response = self.sess_request.post(url=url, data=payload, headers=headers)

        json_path = os.path.join(self.home_path, f'{page_no}.json')
        if os.path.exists(json_path):
            return

        with open(json_path, 'w', encoding='utf8') as fp:
            fp.write(response.text)


if __name__ == '__main__':
    dbss = DBSSpider()
    dbss.download_company_list(250)
