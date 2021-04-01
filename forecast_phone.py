import datetime
from lxml import etree
import requests

url = r'https://m.zhouyi.cc/zhouyi/shouji/'


def forecast(target_word):
    """
    手机号码预测
    :return:
    """
    dt = datetime.datetime.now()
    dt_str = dt.strftime('%Y-%m-%d-%H-%M')
    dt_str_split = dt_str.split('-')

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://m.zhouyi.cc',
        'Referer': 'https://m.zhouyi.cc/zhouyi/shouji/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
    }

    data = {
        'csm': '手机',
        'isbz': '0',
        'word': target_word,
        'data_type': '0',
        'cboYear': dt_str_split[0],
        'cboMonth': str(int(dt_str_split[1])),
        'cboDay': str(int(dt_str_split[2])),
        'cboHour': str(int(dt_str_split[3])),
        'cboMinute': str(int(dt_str_split[4])),
        'pid': '',
        'cid': '选择城市',
        'txtName': '某人',
        'zty': '0',
        'rdoSex': '1',
    }
    res = requests.post(f'{url}/shouji.php', data=data, headers=headers)
    res.encoding = 'utf8'

    content = etree.HTML(res.text)
    jixlist = content.xpath('//div[@class="jixlist"]')[0]
    phone = jixlist.xpath('//span[@class="cphao cff fb"]/text()')[0]
    jixlist_text_list = jixlist.xpath('./text()')
    phone_analyse = jixlist_text_list[1].strip().lstrip('号码分析：')
    phone_effect = jixlist_text_list[2].strip()
    print(f'预测号码：{phone}')
    print(f'号码分析：{phone_analyse}')
    print(f'号码影响：{phone_effect}')


if __name__ == '__main__':
    forecast(target_word='13800008888')
