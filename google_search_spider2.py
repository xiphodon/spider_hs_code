#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/2/22 19:58
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : google_search_spider2.py
# @Software: PyCharm


print('To enable your free eval account and get CUSTOMER, YOURZONE and ' + \
    'YOURPASS, please contact sales@luminati.io')
import sys
if sys.version_info[0] == 2:
    import six
    from six.moves.urllib import request
    import random
    username = 'lum-customer-52wmb-zone-static-route_err-pass_dyn'
    password = '4n06gf39d6un'
    port = 22225
    session_id = random.random()
    super_proxy_url = ('http://%s-session-%s:%s@zproxy.lum-superproxy.io:%d' %
        (username, session_id, password, port))
    proxy_handler = request.ProxyHandler({
        'http': super_proxy_url,
        'https': super_proxy_url,
    })
    opener = request.build_opener(proxy_handler)
    opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36')]
    print('Performing request')
    print(opener.open('http://lumtest.com/myip.json').read())
if sys.version_info[0] == 3:
    import urllib.request
    import random
    username = 'lum-customer-52wmb-zone-static-route_err-pass_dyn'
    password = '4n06gf39d6un'
    port = 22225
    session_id = random.random()
    super_proxy_url = f'http://{username}-session-{session_id}:{password}@zproxy.lum-superproxy.io:{port}'
    print(super_proxy_url)
    proxy_handler = urllib.request.ProxyHandler({
        'http': super_proxy_url,
        'https': super_proxy_url,
    })
    opener = urllib.request.build_opener(proxy_handler)
    opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36')]
    print('Performing request')
    # print(opener.open('http://lumtest.com/myip.json').read())
    print(opener.open('https://www.baidu.com/').read())
