# coding='utf-8'

import time
import datetime


def list2str(list):
    return ' '.join([str(i) for i in list])


# 根据生日时间戳(ms)，计算出年龄
def user_age(given_timestamp):

    current_timestamp = int(time.time() * 1000)  # 当前时间戳（以毫秒为单位）

    given_datetime = datetime.datetime.fromtimestamp(given_timestamp / 1000)
    current_datetime = datetime.datetime.fromtimestamp(current_timestamp / 1000)

    age_timedelta = current_datetime - given_datetime

    return age_timedelta.days // 365


# 将时间戳(ms)转换为日期与时间
def timestamp2date(timestamp):
    s = str(datetime.datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S"))
    date, time = s.split(" ")

    return date, time

if __name__=="__main__":
    date, time = timestamp2date(1599015359162)
    print(date, time)



