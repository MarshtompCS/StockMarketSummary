import json
from typing import Dict
import bs4
from bs4 import BeautifulSoup
import requests
import re
import pickle
import os
import datetime
import time
from tqdm import tqdm
from typing import List
import tushare

POSTS_DIR = "./posts_data"

"""
一、预训练语料
    
    投资论坛过滤清洗方法
    
    1. 过滤太短的帖子
        先看长度分布，筛选文本长度前90%的数据
        例如：
        {
            "id": "157580811",
            "title": "记录思路，辨析进步",
            "time": "2020-06-23",
            "url": "https://www.55188.com/thread-9308816-1-1.html",
            "text_post": "只为记录思路，,守不败之地，攻可胜之敌。"
        }
    2. 数字/符号占比过多
        只保留中文字符大于70%的帖子，数字太多一般是罗列表格数据
        例如：
        {
            "id": "161479794",
            "title": "4.5复盘",
            "time": "2022-04-06",
            "url": "https://www.55188.com/thread-10001825-1-1.html",
            "text_post": "4.5\n引力传媒 603598  元宇宙虚拟人 9:46 13.7亿 封单6209 46.6% 双响炮，缩量回调3日，微放量上穿60日线，预亏，主选\n美盛文化 002699  元宇宙 11:02 45亿 封单1.03 27% 双响炮，缩量回调3日，缩量上穿60日线，减亏，备选\n华媒控股 000607  元宇宙＋nft 9:49 30亿 封单5611 42.8% 预盈，微放量突破前高，主选\n亚联发展 002316  数字经济数字货币 10:44 16亿 封单4632 29% 阶梯放量突破前高，上穿30日线，2次探底，减亏，主选\n海德股份 000567  资管＋海南 13:35 29亿 封单4945 22% 放巨量上穿30.60日线，突破前高，备选\n东方银星 600753  芯片滤波器＋煤化工 10:59 28亿 封单274.5 23% 炸板，双响炮，缩量回调5日，放量阳盖阴，预亏，备选\n"
        }

二、预训练模型
    (PTtype: Pre-train type)
    
    PTtype1. GPT-base (initialized from CPT or Chinese-BART):
        train to generate full post
        
    PTtype2. CPT-base/Chinese-BART-base:
        encoder's input: post中提到的股票/板块/大盘的相关信息 (constrained generation)
        decoder's output: post

三、可选的预训练任务
    
    Encoder的预训练，输入为post
        1. MLM (Masked Language Modeling)
            对post的非数值部分随机mask
            WWM (Whole-Word-Mask)；n-gram mask连续mask多个token
        2. NSP (Next Sentence Prediction)
            帖子的连续两句和随机的两句
        3. SOP (Sentence Order Prediction)
        4. SBO (Sentence Boundary Objective)
            金融实体span的开始和结束，SpanBERT
        
    生成预训练任务
        PTtype1: UniLM (Unidirectional Language Modeling)
        PTtype2: Seq2SeqLM (Sequence-to-Sequence Language Modeling)
        PTtype2: DAE (Denoising Auto Encoder)


四、微调


2. 数值过多:
不希望复盘帖子内容只是罗列当天的股票涨跌数值，我们更想要的是投资或市场的描述
找到每个浮点数span \d+\.\d+
找到每个数值span \d+
如果数值span 和 任意浮点数span有重合，舍弃这个数值
"""


def chinese_percentage(text) -> float:
    # 汉字unicode:
    # \u4e00 ~ \u9fa5
    chinese_num = len([i for i in text if u'\u4e00' <= i <= u'\u9fa5'])
    percent = chinese_num / len(text)
    return percent


def get_digital(text) -> List[re.Match]:
    floats = re.finditer(r"\d+\.\d+", text)  # <re.Match object; span=(104, 108), match='1.03'>
    numbers = re.finditer(r"\d+", text)

    floats = list(floats)
    numbers = list(numbers)
    filtered_numbers = []
    floats_span = [i.span() for i in floats]
    for i in numbers:
        cur_span = i.span()
        overlapped = False
        for fs in floats_span:
            if cur_span[0] >= fs[0] and cur_span[1] <= fs[1]:
                overlapped = True
                break
        if not overlapped:
            filtered_numbers.append(i)
    all_nums = floats + filtered_numbers
    return all_nums


def get_post_by_name(name):
    post = json.load(open(os.path.join(POSTS_DIR, name), 'r', encoding='utf-8'))
    get_digital(post['text_post'])
    return post


def main():
    previous_files = os.listdir(POSTS_DIR)
    previous_ids = []

    for file in previous_files:
        id = file.split('_')[-1].split('.')[0]
        previous_ids.append(id)
        year_month_day = file.split('_')[0]
        post=get_post_by_name(file)


if __name__ == '__main__':
    # get_post_by_name("2022-04-06_4.5复盘_161479794.json")
    main()
