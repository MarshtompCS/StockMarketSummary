import tushare as ts
import pandas as p

# ts.set_token("f90439faa983eb0cf67b6d97f89b77aa773")
# ts.set_token("8e87184d68ee4afdc1f68d1ab4b3e21db0679ea49ac8def3baf336fe")
pro = ts.pro_api()
df = pro.news(src='sina', start_date='2018-11-21 09:00:00', end_date='2018-11-22 10:10:00')
print()
# df = pro.daily()
#
#
# df = pro.query('daily', ts_code='000001.SZ', start_date='20180701', end_date='20180718')
# df = pro.daily(trade_date='20180810')


# pro = ts.pro_api()
# df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
#
# #多个股票
# df = pro.daily(ts_code='000001.SZ,600000.SH', start_date='20180701', end_date='20180718')