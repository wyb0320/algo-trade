#  *_ coding : UTF-8 _*_
#  开发团队  : Bill 
#  开发时间  ：2022/1/6 
#  文件名称  : Astocktrade.PY
#  开发工具  : PyCharm
import requests
from time import sleep
from datetime import datetime, time, timedelta
# from dateutil import parser
import pandas as pd
from pandas import DataFrame
import os
import numpy as np
# from jqdatasdk import *
from WindPy import w
from matplotlib import *
# from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import time
import csv
warnings.filterwarnings('ignore')
csv.field_size_limit(500 * 1024 * 1024)
# 写入csv
def writetocsv(res,des_file):
    with open(des_file, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for line in res:
            writer.writerow(line)
    # print('保存成功')

    
def make_bar(c,h,l,o): 
    bar = abs(o-c)
    if c<o:
        up_len = h-o
        down_len = c-l
    if c>o:
        up_len = h-c
        down_len = o-l
    else:
        up_len = h-c
        down_len = c-l
    return {'up_len':up_len,'bar':bar,'down_len':down_len}

def get_history_data_from_Windpy(code):
    
    print('从wind获取数据......')
    begintime='2018-1-1'
    endtime=datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    # 获取tick
#     fields="last,bid1,ask1" 
#     res = w.wst(codes,fields,begintime,endtime)
    # 获取分钟序列
#     fields='CLOSE,HIGH,LOW,OPEN'
#     res = w.wsi(codes,fields,begintime,endtime)
    # 获取日时间序列数据
    fields='CLOSE,HIGH,LOW,OPEN'
    res = w.wsd(code,fields,begintime,endtime)
    df= pd.DataFrame(data = res.Data,index = res.Fields,columns=res.Times).T
    
    bar_path = 'D:\\Desktop\\python量化\\wind日频\\'+code + '_bar_1d.csv'
    df.to_csv(bar_path,index_label='datetime')
#     display(df)
    return df
def get_newest_data_from_WindPy(code):
    print('从wind获取数据......')
    begintime=datetime.strftime(datetime.now(),'%Y-%m-%d 9:30:00')
    endtime=datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    # 获取日时间序列数据
    fields='CLOSE,HIGH,LOW,OPEN'
    res = w.wsd(code,fields,begintime,endtime)
    df = pd.DataFrame(data = res.Data,index = res.Fields,columns=res.Times).T
    return df

# if k['up_len'] >= 2*k['bar'] and 2*k['bar']>= 2*k['down_len']: # 倒锤子（看涨）:
def choice_A(code):
    print('正在处理',code)
    try:
        bar_path = 'D:\\Desktop\\python量化\\wind日频\\'+code + '_bar_1d.csv'
        df = pd.read_csv(bar_path)
    except:
        print('本地没有历史数据')
        get_history_data_from_Windpy(code)
        bar_path = 'D:\\Desktop\\python量化\\wind日频\\'+code + '_bar_1d.csv'
        df = pd.read_csv(bar_path)
    dfc = df.copy()
    dfc['keep_decline_days']=[0]*len(df)
    dfc['daochuizi'] = ['']*len(df)
    dfc['up_days'] = ['']*len(df)
    dfc['code'] = [code]*len(df)
    dfc['daily_fd'] = ['']*len(df)
    dfc['total_fd'] = ['']*len(df)
    dfc['max_fd'] = ['']*len(df)
    def check_future(i):
        ts = []
        daily_fd = []
        total_fd = []
        fd = 0
        try:
            for t in range(1,7):  #不包括第二根阳线
                if dfc['CLOSE'][i+t] > dfc['CLOSE'][i+t-1]:
                    ts.append(t)
                    daily_fd.append(float('%.2f' % ((dfc['CLOSE'][i+t]-dfc['CLOSE'][i+t-1])/dfc['CLOSE'][i+t-1]*100)))
                    total_fd.append(float('%.2f' % ((dfc['CLOSE'][i+t]-dfc['CLOSE'][i+1])/dfc['CLOSE'][i+1]*100)))
            # print(daily_fd,total_fd)
        except:
            print(code,'最近发生',dfc['datetime'][i])
        if ts != []:
            fd = max(total_fd)
        return [ts,daily_fd,total_fd,fd]
    for i in range(7,len(dfc)-1):
        k = make_bar(dfc['CLOSE'][i],dfc['HIGH'][i],dfc['LOW'][i],dfc['OPEN'][i])
        # k_list = []
        # for num in range(1,8): # 制作过去七天蜡烛图
        #     # print(make_bar(dfc['CLOSE'][i-num],dfc['HIGH'][i-num],dfc['LOW'][i-num],dfc['OPEN'][i-num]))
        #     k_list.append((make_bar(dfc['CLOSE'][i-num],dfc['HIGH'][i-num],dfc['LOW'][i-num],dfc['OPEN'][i-num]))['bar'])
        # average_bar = np.mean(k_list)
        if dfc['CLOSE'][i] < dfc['CLOSE'][i-1] : # 下降趋势
            dfc['keep_decline_days'][i] = dfc['keep_decline_days'][i-1]+1
            if dfc['keep_decline_days'][i] >= 3 and k['up_len'] >= 2*k['bar'] and 2*k['bar']>= 2*k['down_len']: # 倒锤子（看涨）# daochuizi（反转看涨）:
                dfc['daochuizi'][i] = 1
                check_future_res = check_future(i)
                dfc['up_days'][i] = check_future_res[0]
                dfc['daily_fd'][i] = check_future_res[1]
                dfc['total_fd'][i] = check_future_res[2]
                dfc['max_fd'][i] = check_future_res[3]
    df_res = dfc[dfc['daochuizi']==1]
    # 结果汇总
    a = len(df_res) # daochuizi天数
    #     b = len(df_res[df_res['up_days']!=[]])# 未来上涨次数
    c = df_res['max_fd'].mean() #max_fd平均
    c2 = df_res['max_fd'].max() #最大上涨幅度
    date = df_res['datetime'][df_res[df_res['max_fd']==df_res['max_fd'].max()].index[0]] #最大涨幅日期
    e = df_res['daily_fd'][df_res[df_res['max_fd']==df_res['max_fd'].max()].index[0]]
    f = df_res['total_fd'][df_res[df_res['max_fd']==df_res['max_fd'].max()].index[0]]
    d = len(df_res[df_res['max_fd']>0])/a #胜率
    last = df_res['datetime'].tolist()[-1]

    res = [code,a,c,c2,d,date,e,f,last]
    # print(res)
    writetocsv([res],'倒锤子result.csv')
    print(df_res)
    return df_res

def main(codes):
    df_list = []
    header = ['code','eat_line天数','平均上涨幅度','最大上涨幅度','胜率','最大涨幅日期','每日涨幅','每日总幅度','最近出现日期']
    writetocsv([header],'倒锤子result.csv')
    for code in codes[455:]:
        save_path ='D:\\Desktop\\python量化\\倒锤子\\' + code + 'daochuizi.csv'
        try:
            run_res = choice_A(code)
            run_res.to_csv(save_path)
            df_list.append(run_res)
        except:
            # print(code,'没现象！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！')
            # writetocsv([[code]],'wrong_codes2.csv')
            pass
    res = pd.concat(df_list,axis=0)
    res.to_excel('倒锤子原始数据.xlsx')

def daily_check(codes):
    for code in codes:
        df = get_newest_data_from_WindPy(code)
        dfc = df.copy()
        # print(dfc.index)
        k = make_bar(dfc['CLOSE'][-1],dfc['HIGH'][-1],dfc['LOW'][-1],dfc['OPEN'][-1])
        if k['up_len'] >= 2*k['bar'] and 2*k['bar']>= 2*k['down_len']:
            print(code,'今日有倒锤子')
            writetocsv([[code,dfc.index[0]]],'20220106.csv')

if __name__ == '__main__':
    starttime = time.time()
    w.start() # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒
    w.isconnected() # 判断WindPy是否已经登录成功
    codes = pd.read_csv('stock_list.csv')['ts_code'].tolist()
    main(codes)
    # daily_check(codes)

    endtime = time.time()
    dtime = endtime - starttime
    print("程序运行时间：%.8s s" % dtime)

    
    
    
    
    
    
    
    
    
    