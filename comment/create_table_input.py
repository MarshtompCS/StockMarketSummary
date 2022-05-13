import pickle
import os
import logging
import datetime
import tushare as ts
import pandas as pd
from tqdm import tqdm
import time
# snssns.set()
from comment.tushare_utils import get_stock_basic, get_industry_basic, get_stock_day, get_industry_day, get_market_day, \
    get_trade_day

if not os.path.exists("./data"):
    os.chdir("../")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)
POSTS_DIR = "./data/posts_data"
RAW_POST_DIR = "./data/posts_raw_data"


def get_all_required_industry_data():
    data_annotated_with_entities_path = "./0512_filtered_posts.pkl"
    data = pickle.load(open(data_annotated_with_entities_path, 'rb'))
    stock_set = set()
    industry_set = set()
    date_set = set()
    for item in data:
        cur_date = item["time"]
        cur_stocks = item["stock_entity"]
        cur_industry = item["industry_entity"]

        stock_set.update(cur_stocks)
        industry_set.update(cur_industry)
        date_set.add(cur_date)
    #  '2020-03-03',
    date_list = [date.replace("-", "") for date in date_set]
    all_trade_day = get_trade_day(rewrite=True)
    all_trade_day = set(all_trade_day)

    filtered_date_list = [date for date in date_list if date in all_trade_day]
    datetime_formatted_date_list = [datetime.datetime.strptime(date, "%Y%m%d") for date in filtered_date_list]
    datetime_formatted_date_list = sorted(datetime_formatted_date_list)
    min_date = datetime_formatted_date_list[0]
    max_date = datetime_formatted_date_list[-1]
    stock_list = list(stock_set)
    industry_list = list(industry_set)
    # filtered_date_list, stock_set, industry_set

    stock_basic = get_stock_basic()
    industry_basic = get_industry_basic()

    stock_ts_code_list = []
    for stock_name in stock_list:
        stock_ts_code = stock_basic.loc[
            (stock_basic["name"] == stock_name), "ts_code"
        ]
        assert len(stock_ts_code) == 1
        stock_ts_code = stock_ts_code.item()
        stock_ts_code_list.append(stock_ts_code)

    industry_ts_code_list = []
    for industry_name in industry_list:
        readout_industry = industry_basic.loc[
            (industry_basic["name"] == industry_name) & (industry_basic["exchange"] == "A"),
        ]

        res_industry = []
        for idx, cur_industry in readout_industry.iterrows():
            cur_industry_open_date = datetime.datetime.strptime(cur_industry.list_date, "%Y%m%d")
            if cur_industry_open_date > min_date:
                continue
            res_industry.append((cur_industry.ts_code, cur_industry_open_date))

        if len(res_industry) > 1:
            sorted_by_list_date = sorted(res_industry, key=lambda x: x[1])
            industry_ts_code_list.append(sorted_by_list_date[0][0])
        elif len(res_industry) == 1:
            industry_ts_code_list.append(res_industry[0][0])

        # if len(industry_ts_code) == 0:
        #     continue
        # else:
        #     assert len(industry_ts_code) == 1
        # industry_ts_code = industry_ts_code.item()
        # industry_ts_code_list.append(industry_ts_code)

    industry_ts_code_list = list(set(industry_ts_code_list))

    ts.set_token("8e87184d68ee4afdc1f68d1ab4b3e21db0679ea49ac8def3baf336fe")
    ts_api = ts.pro_api()

    # 股票
    # 120积分每分钟内最多调取500次，每次5000条数据，相当于单次提取23年历史
    # ts_code	str	N	股票代码（支持多个股票同时提取，逗号分隔）
    # trade_date	str	N	交易日期（YYYYMMDD）
    # start_date	str	N	开始日期(YYYYMMDD)
    # end_date	str	N	结束日期(YYYYMMDD)
    # ts_api.daily(start_date='20190101', end_date='20220511')

    # 行业板块
    # 单次最大3000行数据（5000积分），可根据指数代码、日期参数循环提取。
    # ts_code	str	N	指数代码
    # trade_date	str	N	交易日期（YYYYMMDD格式，下同）
    # start_date	str	N	开始日期
    # end_date	str	N	结束日期

    min_date_str = min_date.strftime("%Y%m%d")
    max_date_str = max_date.strftime("%Y%m%d")

    all_industry_dataframe = None
    # 共 273 industry ts_code， 279交易日
    for itertimes, idx in tqdm(enumerate(range(0, len(industry_ts_code_list), 5)),
                               total=len(industry_ts_code_list) // 5):
        cur_ts_code_list = industry_ts_code_list[idx:idx + 5]
        cur_ts_code_request_st = ",".join(cur_ts_code_list)
        cur_data = ts_api.ths_daily(ts_code=cur_ts_code_request_st, start_date=min_date_str, end_date=max_date_str)
        assert len(cur_data) < 3000
        if all_industry_dataframe is None:
            all_industry_dataframe = cur_data
        else:
            all_industry_dataframe = pd.concat([all_industry_dataframe, cur_data])

        time.sleep(12)

    pickle.dump(all_industry_dataframe, open("./all_industry_data.pkl", 'wb'))


def get_all_required_stock_data():
    data_annotated_with_entities_path = "./0512_filtered_posts.pkl"
    data = pickle.load(open(data_annotated_with_entities_path, 'rb'))
    stock_set = set()
    industry_set = set()
    date_set = set()
    for item in data:
        cur_date = item["time"]
        cur_stocks = item["stock_entity"]
        cur_industry = item["industry_entity"]

        stock_set.update(cur_stocks)
        industry_set.update(cur_industry)
        date_set.add(cur_date)
    #  '2020-03-03',
    date_list = [date.replace("-", "") for date in date_set]
    all_trade_day = get_trade_day(rewrite=True)
    all_trade_day = set(all_trade_day)

    filtered_date_list = [date for date in date_list if date in all_trade_day]
    datetime_formatted_date_list = [datetime.datetime.strptime(date, "%Y%m%d") for date in filtered_date_list]
    datetime_formatted_date_list = sorted(datetime_formatted_date_list)
    min_date = datetime_formatted_date_list[0]
    max_date = datetime_formatted_date_list[-1]
    stock_list = list(stock_set)
    industry_list = list(industry_set)
    # filtered_date_list, stock_set, industry_set

    stock_basic = get_stock_basic()
    industry_basic = get_industry_basic()

    stock_ts_code_list = []
    for stock_name in stock_list:
        stock_ts_code = stock_basic.loc[
            (stock_basic["name"] == stock_name), "ts_code"
        ]
        assert len(stock_ts_code) == 1
        stock_ts_code = stock_ts_code.item()
        stock_ts_code_list.append(stock_ts_code)

    stock_ts_code_list = list(set(stock_ts_code_list))

    ts.set_token("8e87184d68ee4afdc1f68d1ab4b3e21db0679ea49ac8def3baf336fe")
    ts_api = ts.pro_api()

    # 股票
    # 120积分每分钟内最多调取500次，每次5000条数据，相当于单次提取23年历史
    # ts_code	str	N	股票代码（支持多个股票同时提取，逗号分隔）
    # trade_date	str	N	交易日期（YYYYMMDD）
    # start_date	str	N	开始日期(YYYYMMDD)
    # end_date	str	N	结束日期(YYYYMMDD)
    # ts_api.daily(start_date='20190101', end_date='20220511')

    min_date_str = min_date.strftime("%Y%m%d")
    max_date_str = max_date.strftime("%Y%m%d")

    all_stock_dataframe = None
    # 共 273 industry ts_code， 279交易日
    for itertimes, idx in tqdm(enumerate(range(0, len(stock_ts_code_list), 8)),
                               total=len(stock_ts_code_list) // 8):
        cur_ts_code_list = stock_ts_code_list[idx:idx + 8]
        cur_ts_code_request_st = ",".join(cur_ts_code_list)
        cur_data = ts_api.daily(ts_code=cur_ts_code_request_st, start_date=min_date_str, end_date=max_date_str)
        assert len(cur_data) < 5000
        if all_stock_dataframe is None:
            all_stock_dataframe = cur_data
        else:
            all_stock_dataframe = pd.concat([all_stock_dataframe, cur_data])

        time.sleep(3)

    pickle.dump(all_stock_dataframe, open("./all_stock_data.pkl", 'wb'))


def get_industry_ts_code_by_name(industry_name, industry_basic, min_date):
    readout_industry = industry_basic.loc[
        (industry_basic["name"] == industry_name) & (industry_basic["exchange"] == "A"),
    ]

    res_industry = []
    for idx, cur_industry in readout_industry.iterrows():
        cur_industry_open_date = datetime.datetime.strptime(cur_industry.list_date, "%Y%m%d")
        if cur_industry_open_date > min_date:
            continue
        res_industry.append((cur_industry.ts_code, cur_industry_open_date))

    if len(res_industry) > 1:
        sorted_by_list_date = sorted(res_industry, key=lambda x: x[1])
        ts_code = sorted_by_list_date[0][0]
    elif len(res_industry) == 1:
        ts_code = res_industry[0][0]
    else:
        ts_code = None

    return ts_code


def get_industry_data_by_date_and_ts_code(industry_ts_code, date, all_industry_data):
    data = all_industry_data.loc[
        (all_industry_data["ts_code"] == industry_ts_code) & (all_industry_data["trade_date"] == date)
        ]
    return data


def get_stock_ts_code_by_name(stock_name, stock_basic):
    stock_ts_code = stock_basic.loc[
        (stock_basic["name"] == stock_name), "ts_code"
    ]
    assert len(stock_ts_code) == 1
    stock_ts_code = stock_ts_code.item()
    return stock_ts_code


def get_stock_data_by_date_and_ts_code(stock_ts_code, date, all_stock_data):
    data = all_stock_data.loc[
        (all_stock_data["ts_code"] == stock_ts_code) & (all_stock_data["trade_date"] == date)
        ]
    return data


def annotate_day_data():
    data_annotated_with_entities_path = "./0512_filtered_posts.pkl"
    data = pickle.load(open(data_annotated_with_entities_path, 'rb'))
    #  dict_keys(['id', 'title', 'time', 'url', 'text_post',
    #  'stock_entity', ''industry_entity',
    #  'industry_entity_num', stock_entity_num', 'chinese_char_percentage'])

    date_set = set()
    for item in data:
        cur_date = item["time"]
        date_set.add(cur_date)
    date_list = [date.replace("-", "") for date in date_set]
    all_trade_day = get_trade_day(rewrite=True)
    all_trade_day = set(all_trade_day)

    filtered_date_list = [cur_date for cur_date in date_list if cur_date in all_trade_day]
    datetime_formatted_date_list = [datetime.datetime.strptime(cur_date, "%Y%m%d") for cur_date in filtered_date_list]
    datetime_formatted_date_list = sorted(datetime_formatted_date_list)
    min_date = datetime_formatted_date_list[0]
    max_date = datetime_formatted_date_list[-1]

    stock_basic = get_stock_basic()
    industry_basic = get_industry_basic()

    all_industry_data = pickle.load(open("./all_industry_data.pkl", "rb"))
    all_stock_data = pickle.load(open("./all_stock_data.pkl", 'rb'))
    market_data = pickle.load(open("./stock_data/market_data.dict", 'rb'))

    res_annotate_day_data = []

    stock_name_to_code = dict()
    industry_name_to_code = dict()
    stock_name_and_date_to_data = dict()
    industry_name_and_date_to_data = dict()

    tqdm_bar = tqdm(data, desc=f"cur num: {len(res_annotate_day_data)}")
    for item in tqdm_bar:
        cur_date = item["time"]
        cur_stocks = item["stock_entity"]
        cur_industry = item["industry_entity"]
        cur_date = cur_date.replace("-", "")

        if cur_date not in all_trade_day:
            continue

        sh_data, sz_data = get_market_day(date=cur_date, market_data=market_data)

        item["stock_data"] = dict()
        for stock_name in cur_stocks[:20]:
            if stock_name in stock_name_to_code:
                stock_ts_code = stock_name_to_code[stock_name]
            else:
                stock_ts_code = get_stock_ts_code_by_name(stock_name, stock_basic)
                stock_name_to_code[stock_name] = stock_ts_code
            if f"{stock_ts_code}-{cur_date}" in stock_name_and_date_to_data:
                stock_data = stock_name_and_date_to_data[f"{stock_ts_code}-{cur_date}"]
            else:
                stock_data = get_stock_data_by_date_and_ts_code(stock_ts_code, cur_date, all_stock_data)
                stock_name_and_date_to_data[f"{stock_ts_code}-{cur_date}"] = stock_data
            item["stock_data"][stock_name] = stock_data

        item["industry_data"] = dict()
        for industry_name in cur_industry[:10]:
            if industry_name in industry_name_to_code:
                industry_ts_code = industry_name_to_code[industry_name]
            else:
                industry_ts_code = get_industry_ts_code_by_name(industry_name, industry_basic, min_date)
                industry_name_to_code[industry_name] = industry_ts_code
            if industry_ts_code is None:
                continue
            if f"{industry_name}-{cur_date}" in industry_name_and_date_to_data:
                industry_data = industry_name_and_date_to_data[f"{industry_name}-{cur_date}"]
            else:
                industry_data = get_industry_data_by_date_and_ts_code(industry_ts_code, cur_date, all_industry_data)
                industry_name_and_date_to_data[f"{industry_name}-{cur_date}"] = industry_data

            item["industry_data"][industry_name] = industry_data

        item["market_data"] = {"sh": sh_data, "sz": sz_data}

        res_annotate_day_data.append(item)
        tqdm_bar.set_description(f"cur num: {len(res_annotate_day_data)}")

    logging.info(f"annotate day data num: {len(res_annotate_day_data)}")
    pickle.dump(res_annotate_day_data, open("./annotate_day_data_0512.pkl", 'wb'))

    # {'id': '161686515',
    # 'title': '记录四年一遇世纪大底',
    # 'time': '2022-04-29',
    # 'url': 'https://www.55188.com/thread-10045107-1-1.html',
    # 'text_post': '今天是2022年4月27日.很激动市场迎来了四年一遇的大底。历次世纪大底都有一个明确的信号，这个位置进入会躺赚，并且这个点位未来看不到。也许大盘基本面可能继续走坏，但是部分板块这个位置不会继续走低，会是世纪大底。部分板块会带着指数继续走低，但是不重要，重要的是这个位置是我心目中的大底。底部特征:市场牛股几乎为零，熊股是马股的一倍。这个是我2011年上班到现在亲身经历两次的可靠信号。998，1664，1894，2638，2440如今的2863。附属表格为本人做的市场盈亏，数据做的周期比较短。 代表全市场亏损20%，底部信号的一个辅助验证或者前提。
    # ', 'stock_entity': [], 'stock_entity_num': 0,
    # 'industry_entity': [], 'industry_entity_num': 0, 'chinese_char_percentage': 0.7773722627737226}


if __name__ == '__main__':
    # get_all_required_industry_data()
    # get_all_required_stock_data()
    annotate_day_data()
