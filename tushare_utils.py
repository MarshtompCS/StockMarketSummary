import logging
from typing import Optional, Dict, List
import tushare as ts
import pandas as pd
import json
import pickle
import os
import time
import numpy as np

# ts.set_token("f90439faa983eb0cf67b6d97f89b77aa773")
# ts.set_token("8e87184d68ee4afdc1f68d1ab4b3e21db0679ea49ac8def3baf336fe")
ts_api = ts.pro_api()


def get_trade_day(rewrite=False):
    trade_day_path = "./stock_data/trade_day.list"
    if rewrite or not os.path.exists(trade_day_path):
        data = ts_api.trade_cal(exchange='SSE', is_open='1', start_date='20180101', end_date=time.strftime("%Y%m%d"))
        data = data.cal_date.tolist()
        pickle.dump(data, open(trade_day_path, 'wb'))
    else:
        data = pickle.load(open(trade_day_path, 'rb'))
    return data


def get_market_day(rewrite=False, date=time.strftime("%Y%m%d")):
    # ts_code	str	Y	指数代码
    # trade_date	str	N	交易日期 （日期格式：YYYYMMDD，下同）
    # start_date	str	N	开始日期
    # end_date	str	N	结束日期

    # ts_code	str	TS指数代码
    # trade_date	str	交易日
    # close	float	收盘点位
    # open	float	开盘点位
    # high	float	最高点位
    # low	float	最低点位
    # pre_close	float	昨日收盘点
    # change	float	涨跌点
    # pct_chg	float	涨跌幅（%）
    # vol	float	成交量（手）
    # amount	float	成交额（千元）
    # data = ts_api.index_dailybasic(trade_date='20220505')
    # 000001.SH 上证指数
    # 399001.SZ 深证成指

    market_data_path = "./stock_data/market_data.dict"
    if rewrite or not os.path.exists(market_data_path):
        sh = ts_api.index_daily(ts_code='000001.SH', start_date='20190101', end_date=date)
        sz = ts_api.index_daily(ts_code='399001.SZ', start_date='20190101', end_date=date)
        data = {"sh": sh, "sz": sz}
    else:
        data = pickle.load(open(market_data_path, 'rb'))

    return data


def get_stock_day(stock_basic: pd.DataFrame, date, stock_name,
                  stock_ts_code=None, rewrite=False):
    all_stock_dir = "./stock_data/stocks/"
    if not os.path.exists(all_stock_dir):
        os.mkdir(all_stock_dir)

    stock_dir = os.path.join(all_stock_dir, stock_name)
    if not os.path.exists(stock_dir):
        os.mkdir(stock_dir)

    # daily input
    # ts_code	str	Y	指数代码
    # trade_date	str	N	交易日期 （日期格式：YYYYMMDD，下同）
    # start_date	str	N	开始日期
    # end_date	str	N	结束日期

    # daily output
    # ts_code	str	股票代码
    # trade_date	str	交易日期
    # open	float	开盘价
    # high	float	最高价
    # low	float	最低价
    # close	float	收盘价
    # pre_close	float	昨收价
    # change	float	涨跌额
    # pct_chg	float	涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
    # vol	float	成交量 （手）
    # amount	float	成交额 （千元）

    # weekly input
    # ts_code	str	N	TS代码 （ts_code,trade_date两个参数任选一）
    # trade_date	str	N	交易日期 （每周最后一个交易日期，YYYYMMDD格式）
    # start_date	str	N	开始日期
    # end_date	str	N	结束日期

    # weekly output
    # ts_code	str	Y	股票代码
    # trade_date	str	Y	交易日期
    # close	float	Y	周收盘价
    # open	float	Y	周开盘价
    # high	float	Y	周最高价
    # low	float	Y	周最低价
    # pre_close	float	Y	上一周收盘价
    # change	float	Y	周涨跌额
    # pct_chg	float	Y	周涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
    # vol	float	Y	周成交量
    # amount	float	Y	周成交额

    stock_day_data_path = os.path.join(stock_dir, f"{date}.dict")
    if rewrite or not os.path.exists(stock_day_data_path):
        if stock_ts_code is None:
            stock_ts_code = stock_basic.loc[stock_basic["name"] == stock_name, "ts_code"]
            assert len(stock_ts_code) == 1
            stock_ts_code = stock_ts_code.item()
        daily_data = ts_api.daily(ts_code=stock_ts_code, start_date=date, end_date=date)
        if len(daily_data.values) != 1:
            return len(daily_data.values)
        # weekly_data = ts_api.weekly(ts_code=stock_ts_code, start_date=date, end_date=date)
        # data = {"daily_data": daily_data, "weekly_data": weekly_data}
        pickle.dump(daily_data, open(stock_day_data_path, 'wb'))
    else:
        daily_data = pickle.load(open(stock_day_data_path, 'rb'))

    return daily_data


def get_industry_day(industry_basic: pd.DataFrame, date, industry_name, industry_ts_code=None, rewrite=False):
    all_industry_dir = "./stock_data/industries/"
    if not os.path.exists(all_industry_dir):
        os.mkdir(all_industry_dir)

    industry_dir = os.path.join(all_industry_dir, industry_name)
    if not os.path.exists(industry_dir):
        os.mkdir(industry_dir)

    # ts_code	str	N	指数代码
    # trade_date	str	N	交易日期（YYYYMMDD格式，下同）
    # start_date	str	N	开始日期
    # end_date	str	N	结束日期

    # ts_code	str	Y	TS指数代码
    # trade_date	str	Y	交易日
    # close	float	Y	收盘点位
    # open	float	Y	开盘点位
    # high	float	Y	最高点位
    # low	float	Y	最低点位
    # pre_close	float	Y	昨日收盘点
    # avg_price	float	Y	平均价
    # change	float	Y	涨跌点位
    # pct_change	float	Y	涨跌幅
    # vol	float	Y	成交量
    # turnover_rate	float	Y	换手率
    # total_mv	float	N	总市值
    # float_mv	float	N	流通市值

    industry_day_data_path = os.path.join(industry_dir, f"{date}.dict")
    if rewrite or not os.path.exists(industry_day_data_path):
        if industry_ts_code is None:
            industry_ts_code = industry_basic.loc[industry_basic["name"] == industry_name, "ts_code"]
            assert len(industry_ts_code) == 1
            industry_ts_code = industry_ts_code.item()
        daily_data = ts_api.ths_daily(ts_code=industry_ts_code, start_date=date, end_date=date)
        if len(daily_data.values) != 1:
            return len(daily_data.values)
        pickle.dump(daily_data, open(industry_day_data_path, 'wb'))
    else:
        daily_data = pickle.load(open(industry_day_data_path, 'rb'))

    return daily_data


def get_stock_weekly_data(stock_name, today, trade_day):
    # today_data = get_stock_day(stock_basic, today, stock_name)

    today_idx = trade_day.index(today)

    six_days_data = []
    for date in trade_day[today_idx - 5: today_idx + 1]:
        cur_date_data = get_stock_day(stock_basic, date, stock_name)
        six_days_data.append(cur_date_data)

    # today-close - six-days-ago-close (or five-days-ago-open)
    weekly_change = six_days_data[-1].close.item() - six_days_data[0].close.item()
    weekly_pct_chg = weekly_change / six_days_data[0].close.item()

    weekly_close_data = [cur_date_data.close.item() for cur_date_data in six_days_data]

    return {
        "weekly_change": weekly_change,
        "weekly_pct_chg": weekly_pct_chg,
        "weekly_close_data": weekly_close_data,
        "six_days_data": six_days_data
    }


def get_industry_weekly_data(industry_name, today, trade_day):
    # today_data = get_stock_day(stock_basic, today, stock_name)

    today_idx = trade_day.index(today)

    six_days_data = []
    for date in trade_day[today_idx - 5: today_idx + 1]:
        cur_date_data = get_industry_day(stock_basic, date, industry_name)
        six_days_data.append(cur_date_data)

    # today-close - six-days-ago-close (or five-days-ago-open)
    weekly_change = six_days_data[-1].close.item() - six_days_data[0].close.item()
    weekly_pct_chg = weekly_change / six_days_data[0].close.item()

    weekly_close_data = [cur_date_data.close.item() for cur_date_data in six_days_data]

    return {
        "weekly_change": weekly_change,
        "weekly_pct_chg": weekly_pct_chg,
        "weekly_close_data": weekly_close_data,
        "six_days_data": six_days_data
    }


def get_stock_basic(rewrite=False):
    stock_basic_path = "./stock_data/stock_basic.pd"
    if rewrite or not os.path.exists(stock_basic_path):
        # 查询当前所有正常上市交易的股票列表
        fields = 'ts_code,symbol,name,area,industry,fullname,market'
        # TS代码, 股票代码，股票名称，地域，所属行业，股票全称，市场类型
        data = ts_api.stock_basic(exchange='', list_status='L', fields=fields)
        # 根据名称获取ts_code：data.loc[data["name"]=="平安银行", "ts_code"]
        # 获取所有股票名称： data.loc[:, "name"]
        # data.loc[:, "name"].values (numpy.ndarray)
        pickle.dump(data, open(stock_basic_path, 'wb'))
    else:
        data = pickle.load(open(stock_basic_path, 'rb'))
    return data


def get_industry_basic(rewrite=False):
    # Input:
    # ts_code	str	N	指数代码
    # exchange	str	N	市场类型A-a股 HK-港股 US-美股
    # type	str	N	指数类型 N-板块指数 I-行业指数 S-同花顺特色指数
    # Output:
    # ts_code	str	Y	代码
    # name	str	Y	名称
    # count	int	Y	成分个数
    # exchange	str	Y	交易所
    # list_date	str	Y	上市日期
    # type	str	Y	N概念指数S特色指数

    industry_basic_path = "./stock_data/industry_basic.pd"
    if rewrite or not os.path.exists(industry_basic_path):
        data = ts_api.ths_index()
        # 获取所有板块/行业名称
        # industry_names = data.loc[(data["type"] == "N") | (data["type"] == "I"), "name"]
        pickle.dump(data, open(industry_basic_path, 'wb'))
    else:
        data = pickle.load(open(industry_basic_path, 'rb'))
    return data


def get_news():
    # fields='ts_code,trade_date,open,close,high,low,pct_change')
    # df = ts_api.news(src='sina', start_date='2018-11-21 09:00:00', end_date='2018-11-22 10:10:00')
    raise NotImplementedError("TuShare-get-news-api is not implemented.")


if __name__ == '__main__':
    get_market_day()
    trade_day = get_trade_day()
    # get_stock_basic(rewrite=True)
    # get_industry_basic(rewrite=True)
    stock_basic = get_stock_basic()
    industry_basic = get_industry_basic()

    # get_stock_day(stock_basic, "20220418", "平安银行", rewrite=True)
