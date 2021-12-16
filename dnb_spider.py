#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/10/14 9:48
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : dnb_spider.py
# @Software: PyCharm
import json
from pprint import pprint

import requests
import pathlib

from base_spider import BaseSpider


class DNBSpider(BaseSpider):
    """
    dnb spider
    """
    dnb_home_path = pathlib.Path('E://dnb')
    dnb_home_path.mkdir(exist_ok=True)

    def __init__(self):
        """
        init
        """
        super(BaseSpider, self).__init__()
        self.sess = requests.session()

        headers = {
            # ':authority': 'www.dnb.com',
            # ':method': 'GET',
            # ':path': '/',
            # ':scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'max-age=0',
            'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': 'Windows',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            # 'sec-fetch-site': 'same-origin',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            # 'cookie': 'SSLB=1; chosen_visitorFrom_cookie_new=DIR; HID=1634175860203; _mkto_trk=id:080-UQJ-704&token:_mch-dnb.com-1634175861027-73474; _gcl_au=1.1.791645649.1634175861; _biz_uid=1a68c8eebdd14e0eeb6a6d5c95b62ba1; AMCVS_8E4767C25245B0B80A490D4C%40AdobeOrg=1; hpVer=coldstate; _ga=GA1.2.51689566.1634175863; __ncuid=50777953-cf78-4a46-b7cb-7a1762a26791; _hjid=6b144395-d94e-4202-9be9-92d6143e0a1f; tbw_bw_uid=bito.AALlz07Bm5wAADdqQJilMA; _biz_su=1a68c8eebdd14e0eeb6a6d5c95b62ba1; _biz_flagsA=%7B%22Version%22%3A1%2C%22Mkto%22%3A%221%22%2C%22Ecid%22%3A%22-237384295%22%2C%22ViewThrough%22%3A%221%22%2C%22XDomain%22%3A%221%22%7D; s_cc=true; drift_aid=808f1d78-c2f3-4671-8ec4-6f11c59b4033; driftt_aid=808f1d78-c2f3-4671-8ec4-6f11c59b4033; _st_bid=5bb67640-2c90-11ec-b035-072e1c18298e; w_AID=10794677889066133242926550009945468409; SSOD=AINyAAAAIAAjzDMAAQAAAHSLZ2F0i2dhY-5QAAIAAAB0i2dh0ItnYQAA; __gads=ID=295d3da66c2b7392-221469706ecc0065:T=1634176011:RT=1634176011:S=ALNI_MbVXNOsAN1vJ_R9HE2eC58js71rQQ; s_sq=%5B%5BB%5D%5D; DMCP=0; QSI_HistorySession=https%3A%2F%2Fwww.dnb.com%2Fbusiness-directory%2Fcompany-profiles.beijing_jingwa_kid_clothing_business_department.143170354ace1694e8677b3095326df0.html~1634176011326%7Chttps%3A%2F%2Fwww.dnb.com%2Fbusiness-directory%2Fcompany-search.html%3Fterm%3Dkid%2520clothing%26page%3D1~1634176019230%7Chttps%3A%2F%2Fwww.dnb.com%2F~1634176747287; SSID=CACQox0AAAAAAAB0i2dh_i6DJHSLZ2ECAAAAAAAAAAAAj51rYQB-5Q; SSSC=644.G7018731875225251582.2|0.0; ak_bmsc=15BB5354838FAC500011EA54306F0D12~000000000000000000000000000000~YAAQd8czuHysHVx8AQAAInpfjA18Lao+HHy+0QiJvpndn11YfUyUMmpvBDeQzuWqnsxgHjNu4RalIWv1/EUFLjrVPGz6ie3PvGQ3wI0KOMNTJSDTflz/su+dDBHxdos9rBhhxq4ixFWj8c5vIWVJEx3Fgj1tHkIhPV47gmLiWXgvqEwRKY0sPMwSNTgcRJZ7ylVUVpxqT0ExZhslLaHGctl+n/2yNLiuQ3ZS2SH3V52NOMbu74GNP2BG+GhfnLSh9Xn3jYNk5tjnMl4OVtSbKmG9idKado2mUtzoUq4Y1u6UJFD4XS8AHbmMLPc+T12oNKFvmuaqGU26T1CYon0Fdg+auodeR4i64I7IpwMoLXxnkBv9Aogv4L+XqLZwsbNPNIqnMU6B; s_nr30=1634442641946-Repeat; _biz_sid=326787; _biz_nA=10; AMCV_8E4767C25245B0B80A490D4C%40AdobeOrg=-1124106680%7CMCIDTS%7C18918%7CMCMID%7C10794677889066133242926550009945468409%7CMCAAMLH-1635047442%7C11%7CMCAAMB-1635047442%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1634449842s%7CNONE%7CvVersion%7C5.2.0; _biz_pendingA=%5B%5D; _gid=GA1.2.881465291.1634442642; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=0; _hjIncludedInSessionSample=0; nc-previous-guid=c4371d667fb5b75c2f303e02e3dd6bb4; tbw_bw_sd=1634442643; _sp_ses.2291=*; drift_campaign_refresh=2f2164b3-d33a-40d9-862b-e7ed213fd46c; _sp_id.2291=80cf17f2-86cc-4403-8c94-c153892f77d3.1634175870.2.1634442763.1634176888.a516629e-082d-463d-adc8-ceab87419792; _st_l=38.600|8005269018,8442384766,,+18442384766,0,1634443366.8662583217,8558935845,,+18558935845,0,1634177488|1903608662; _st=5bb67640-2c90-11ec-b035-072e1c18298e.5bb9d1a0-2c90-11ec-b035-072e1c18298e....0....1634443366.1634453446.600.10800.30.0....1....1.10,11..dnb^com.UA-18184345-1.51689566^1634175863.38.; SSRT=Dp5rYQQBAA; JSESSIONID=node01cuulqjegt6z6fty35s0po6i24576295.node0; bm_sv=CA1F651C0BAC438F32676C27BA26F166~wPHXFRTdbnQn9rUqGKvZUo9pMSir87SBSATa2XNa6ESJHOypHJIAeYxMhnn1X2u8wnvGcn0DjbX7E61ubVS0MaKAoL1d7uXf/2im9zd5K8LJumCeOTzrlIglpGHOVqNFaXbJesgdGe3mnPUiSrEyvQ==; AWSALB=pN5pQA3L/ERNVCzaIMT8JfZylDF/yZPuLlvse8p7RUihdmjQfYRPt35QpdYOEprorNQ3cou/z2NGfZFGOtDjpVajlTD/ykbW98LdoE9zk8k1R77JWdcbZngcLMtj; AWSALBCORS=pN5pQA3L/ERNVCzaIMT8JfZylDF/yZPuLlvse8p7RUihdmjQfYRPt35QpdYOEprorNQ3cou/z2NGfZFGOtDjpVajlTD/ykbW98LdoE9zk8k1R77JWdcbZngcLMtj; _gat_ncAudienceInsightsGa=1; integrated_search_query=kid%20clothing; RT="z=1&dm=www.dnb.com&si=fd22cc50-d87a-45ff-af43-7467f76bc8b1&ss=kuuoy2fc&sl=1&tt=52l&rl=1&ld=52n&ul=35a8"',
            # 'referer': 'https://www.dnb.com/',
            'user-agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)'
        }

        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36', 'accept-encoding': 'gzip, deflate, br', 'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.9', 'Connection': 'keep-alive', 'accept-language': 'en-US,en;q=0.9,de;q=0.8'}
        # result = self.sess.get('https://www.dnb.com/business-directory/industry-analysis.arts-entertainment-recreation-sector.html', headers=headers)
        # result = self.sess.get('https://www.dnb.com/business-directory.html', headers=headers)
        # result = self.sess.get('https://httpbin.org/headers', headers=headers)

        # result = self.sess.get('https://www.dnb.com/business-directory.html', headers=headers)
        result = self.sess.get('https://www.dnb.com/', headers=headers)
        # result = self.sess.get('https://google.com', headers=headers)



        print(result.text)
        pprint(result.request.headers)
        pprint(dict(result.request.headers))
        print(result.status_code)
        ##

    def download_company_list_pages(self):
        """
        下载公司列表页
        :return:
        """


        kw = 'kid clothing'
        p = 1
        url = f'https://www.dnb.com/business-directory/company-search.html?term={kw.replace(" ", "%20")}&page={p}'

        cookie_dict = {
            'cookie': 'SSLB=1; chosen_visitorFrom_cookie_new=DIR; HID=1634175860203; _mkto_trk=id:080-UQJ-704&token:_mch-dnb.com-1634175861027-73474; _gcl_au=1.1.791645649.1634175861; _biz_uid=1a68c8eebdd14e0eeb6a6d5c95b62ba1; AMCVS_8E4767C25245B0B80A490D4C%40AdobeOrg=1; hpVer=coldstate; _ga=GA1.2.51689566.1634175863; __ncuid=50777953-cf78-4a46-b7cb-7a1762a26791; _hjid=6b144395-d94e-4202-9be9-92d6143e0a1f; tbw_bw_uid=bito.AALlz07Bm5wAADdqQJilMA; _biz_su=1a68c8eebdd14e0eeb6a6d5c95b62ba1; _biz_flagsA=%7B%22Version%22%3A1%2C%22Mkto%22%3A%221%22%2C%22Ecid%22%3A%22-237384295%22%2C%22ViewThrough%22%3A%221%22%2C%22XDomain%22%3A%221%22%7D; s_cc=true; drift_aid=808f1d78-c2f3-4671-8ec4-6f11c59b4033; driftt_aid=808f1d78-c2f3-4671-8ec4-6f11c59b4033; _st_bid=5bb67640-2c90-11ec-b035-072e1c18298e; w_AID=10794677889066133242926550009945468409; SSOD=AINyAAAAIAAjzDMAAQAAAHSLZ2F0i2dhY-5QAAIAAAB0i2dh0ItnYQAA; __gads=ID=295d3da66c2b7392-221469706ecc0065:T=1634176011:RT=1634176011:S=ALNI_MbVXNOsAN1vJ_R9HE2eC58js71rQQ; s_sq=%5B%5BB%5D%5D; DMCP=0; SSID=CACQox0AAAAAAAB0i2dh_i6DJHSLZ2ECAAAAAAAAAAAAj51rYQB-5Q; SSSC=644.G7018731875225251582.2|0.0; _biz_sid=326787; AMCV_8E4767C25245B0B80A490D4C%40AdobeOrg=-1124106680%7CMCIDTS%7C18918%7CMCMID%7C10794677889066133242926550009945468409%7CMCAAMLH-1635047442%7C11%7CMCAAMB-1635047442%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1634449842s%7CNONE%7CvVersion%7C5.2.0; _gid=GA1.2.881465291.1634442642; tbw_bw_sd=1634442643; _sp_ses.2291=*; nc-previous-guid=3ec20415d42476553c25b6c8090e361a; bm_mi=F28D1AF483DE7C7B9646A2E0D035A4F0~VIMtPAN5Cytkni1rGpvNFSbk13CVEvnzHfs1sfTmGisRg4nXrN9R7YOGiYENoZad/1rC2tTjy9TKCn1XuUwHTrtToK6zOyrSvE+MsrwwXg2rvGYFLHU8RKZTUFy4qKMM/b/sZeolr7BNBBmxxuhmFheXGz4kuZunqcWQp9sv+nXOMMb97y2YZkLmksagI75eo2+Erb1xdxx79D1iwJ0cGKdCrKxuXhbqT8G1QXkChSFN491HItoiPQV80LlrflUq/awjqoxkng2UyQ4wzu8OXsKeQw8/s30t1idOhdJ0PGel/YuJg5xJRA3BNRPcjk9A; QSI_HistorySession=https%3A%2F%2Fwww.dnb.com%2Fbusiness-directory%2Fcompany-search.html%3Fterm%3Dkid%2520clothing%26page%3D1~1634176019230%7Chttps%3A%2F%2Fwww.dnb.com%2F~1634176747287%7Chttps%3A%2F%2Fwww.dnb.com%2Fbusiness-directory%2Fcompany-search.html%3Fterm%3Dkid%2520clothing%26page%3D1~1634442844631; ak_bmsc=15BB5354838FAC500011EA54306F0D12~000000000000000000000000000000~YAAQd8czuLKwHVx8AQAAUaFijA1ItZ4/zdCm0DprKqIdRJdbuwBbZEJQY650gyyxyOWfPs3lD+JeQwWf85iemqgZKjr+KESForRERmJmxc3svd1ENkWrLsxoa+cHFEV/31RIfEKBRb9yWvG4ezoybGNLSrwhnUmSqgO55hgGvP/fgNptlVD9CowMULzV5NMNFb1WpMg/kdRY5Jt2+CoTatwuhhC6GpnDTslggBx+QoeFW4a8hF7YX1FwBGcSo4UzvSR+OctRznRDBjpZjVoqOidXWQsFjestsw8rh+wYHzvEMmlf2PyehA5j4Byd2m7n8IwC4EMKX1C+zqWew3pBVK90Oao2ujuAvnK5BDiw6YQac0M2MiIRHUql+tw+I859E+05Qa/Ab3yNR0RKeCnfQUco2GnwzlQEon2ThjJrEB134cXYgWkB3fOY5rdJg8ihtG8XOMx18sL9ohwxqHo=; _st_l=38.600|8005269018,8442384766,,+18442384766,0,1634443366.8662583217,8558935845,,+18558935845,0,1634444344|1903608662; _st=5bb67640-2c90-11ec-b035-072e1c18298e.5bb9d1a0-2c90-11ec-b035-072e1c18298e....0....1634444344.1634453644.600.10800.30.0....1....1.10,11..dnb^com.UA-18184345-1.51689566^1634175863.38.; _biz_nA=13; s_nr30=1634443767875-Repeat; SSRT=-KFrYQQBAA; AWSALBCORS=XTaBN62e/oWzeDQc4o8LUw0mjmn+3EoFo2HbieUIa7cB4NInxO561+GJ3wtzxjDCjfBoqAoCBL2QDqrUMPhgZu2p65hHCCN4YisT0/OVFSpONMLOM/jaf+1kHsU9; AWSALB=XTaBN62e/oWzeDQc4o8LUw0mjmn+3EoFo2HbieUIa7cB4NInxO561+GJ3wtzxjDCjfBoqAoCBL2QDqrUMPhgZu2p65hHCCN4YisT0/OVFSpONMLOM/jaf+1kHsU9; JSESSIONID=node09a2k4nb4y2ny10oni12ib9yha4577072.node0; _sp_id.2291=80cf17f2-86cc-4403-8c94-c153892f77d3.1634175870.2.1634443770.1634176888.a516629e-082d-463d-adc8-ceab87419792; _biz_pendingA=%5B%5D; _gat_ncAudienceInsightsGa=1; bm_sv=CA1F651C0BAC438F32676C27BA26F166~wPHXFRTdbnQn9rUqGKvZUo9pMSir87SBSATa2XNa6ESJHOypHJIAeYxMhnn1X2u8wnvGcn0DjbX7E61ubVS0MaKAoL1d7uXf/2im9zd5K8Li4GjadvFO+YeaVaOvSLxVdCZde8RbUkoH2lWijSV3TA==; RT="z=1&dm=www.dnb.com&si=fd22cc50-d87a-45ff-af43-7467f76bc8b1&ss=kuuoy2fc&sl=3&tt=52l&obo=2&rl=1&ld=13bkp&r=njqavo9d&ul=13bkq&hd=13c6a"'
        }
        # self.sess.cookies = requests.utils.cookiejar_from_dict(cookie_dict, cookiejar=None, overwrite=None)
        result = self.sess.get(url)
        print(result.cookies)
        with open(self.dnb_home_path / f'{p}.html', 'w', encoding='utf8') as fp:
            fp.write(result.text)


if __name__ == '__main__':
    dnb = DNBSpider()
    # dnb.download_company_list_pages()