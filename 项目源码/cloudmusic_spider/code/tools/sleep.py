import random
import time

def sleep(mu = 0.5):
    # mu = 0.5  # 平均值
    sigma = 0.5  # 标准差
    # 正态分布
    sleep_time = random.gauss(mu, sigma)
    sleep_time = max(0.5*mu, min(1.5*mu, sleep_time))  # 将休眠时间限制在0.5x到1.5x之间

    time.sleep(sleep_time)