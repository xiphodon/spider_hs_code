#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/3 13:53
# @Author  : GuoChang
# @Site    : https://github.com/xiphodon
# @File    : my_test_demo.py
# @Software: PyCharm

import requests
import company_spider_4
import os

try:
    import Image
except ImportError:
    from PIL import Image

import pytesseract

# 设置Tesseract-OCR的启动入口和训练集目录
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe'
tessdata_dir_config = r'--tessdata-dir "C:\Program Files (x86)\Tesseract-OCR\tessdata"'


test_img_png_dir_path = r'E:\work_all\topease\company_spider_2\phone_img_list_test'
test_img_png_path = os.path.join(test_img_png_dir_path, 'test.png')


def download_img_png(img_url):
    """
    下载png类型图片
    :param img_url:
    :return:
    """
    result = requests.get(img_url, timeout=5, stream=True)
    if result.status_code == 200:
        file_path = test_img_png_path
        with open(file_path, 'wb') as fp:
            for chunk in result.iter_content(1024):
                fp.write(chunk)


def binarizing(_img, threshold):
    """
    转化为二值图像
    :param _img:
    :param threshold:
    :return:
    """
    pixdata = _img.load()
    w, h = _img.size
    for y in range(h):
        for x in range(w):
            if pixdata[x, y] < threshold:
                pixdata[x, y] = 0
            else:
                pixdata[x, y] = 255
    return _img


def depoint(_img):
    """
    去除电话号码中的横杠
    :param _img: 二值图
    :return:
    """
    pixdata = _img.load()
    w,h = _img.size

    cover_y = 8
    # cover_recode_0_list = []
    cover_recode_255_list = []

    for y in range(1, h-1):
        for x in range(1, w-1):
            if y == cover_y:
                # if pixdata[x, y + 1] < 10 or pixdata[x, y - 1] < 10 \
                #         or pixdata[x - 1, y + 1] < 10 or pixdata[x - 1, y - 1] < 10 \
                #         or pixdata[x + 1, y + 1] < 10 or pixdata[x + 1, y - 1] < 10:
                #     cover_recode_0_list.append((x, y))
                # else:
                #     cover_recode_255_list.append((x, y))

                if pixdata[x, y + 1] > 245 and pixdata[x, y - 1] > 245 \
                        and pixdata[x - 1, y + 1] > 245 and pixdata[x - 1, y - 1] > 245 \
                        and pixdata[x + 1, y + 1] > 245 and pixdata[x + 1, y - 1] > 245:
                    cover_recode_255_list.append((x, y))

    for x, y in cover_recode_255_list:
        pixdata[x, y] = 255

    return _img


def shrink_space(_img):
    """
    缩小多余的空格
    :return:
    """
    pixdata = _img.load()
    w, h = _img.size

    cover_len = 9
    flag = 0
    x_iter_start_index = 0
    while True:

        if flag == 1:
            return _img

        x_start_index = x_iter_start_index
        x_end_index = -1

        for x in range(x_iter_start_index, w):
            has_black = False
            for y in range(h):
                if pixdata[x, y] < 10:
                    has_black = True
                    break
            if has_black and x_end_index != -1:
                x_start_end_len = x_end_index - x_start_index
                if x_start_end_len > cover_len:
                    # 缩为一个空格
                    move_len = (x_start_end_len // cover_len - 1) * cover_len

                    cover_start_index = x_start_end_len - move_len + x_start_index
                    x_iter_start_index = cover_start_index

                    for xx in range(cover_start_index, w):
                        for yy in range(h):
                            if xx + move_len < w:
                                pixdata[xx, yy] = pixdata[xx + move_len, yy]
                            else:
                                pixdata[xx, yy] = 255

                    _img.show()
                    # 跳出循环，执行while True
                    break

                x_start_index = x + 1
            else:
                x_end_index = x

                if x_end_index == w - 1:
                    flag = 1
                    break


def img_to_str(img_png_path):
    """
    手机号图片转文字
    :param img_png_path:
    :return:
    """
    # download_img_png(r'http://www.listcompany.org/0086_Art_Gallery_Info.html/phone-3-1291926.png')
    # download_img_png(r'http://www.listcompany.org/011expo_Inc_Info.html/phone-1-1726623.png')
    image = Image.open(img_png_path)

    # 转化为灰度图
    img = image.convert('L')
    # 把图片变成二值图像。
    img1 = binarizing(img, 128)
    # 去除干扰线
    img1 = depoint(img1)
    # 缩小多余的空格
    img1 = shrink_space(img1)
    # img1.show()
    img_str = pytesseract.image_to_string(img1, config=tessdata_dir_config)
    print(img_str)
    print(img_str.replace(' ', '-'))


def bat_test_img_to_str():
    """
    批处理检测图像转文字
    :return:
    """
    for company_id_dir in os.listdir(test_img_png_dir_path):
        company_id_dir_path = os.path.join(test_img_png_dir_path, company_id_dir)
        for img_file in os.listdir(company_id_dir_path):
            img_path = os.path.join(company_id_dir_path, img_file)
            print(company_id_dir)
            img_to_str(img_path)
            print()


if __name__ == '__main__':
    img_to_str(test_img_png_path)
    # bat_test_img_to_str()
