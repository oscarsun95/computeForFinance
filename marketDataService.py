# -*- coding: utf-8 -*-
"""
Created on Sun July 21 23:12:21 2019

@author: Eddie
"""

import pandas as pd
import numpy as np
import os
import time


class OrderBookSnapshot_FiveLevels:
    outputCols = ['ticker', 'date', 'time', 'askPrice5', 'askPrice4', 'askPrice3', 'askPrice2', 'askPrice1', 'bidPrice1',
     'bidPrice2', 'bidPrice3', 'bidPrice4', 'bidPrice5', 'askSize5', 'askSize4', 'askSize3', 'askSize2', 'askSize1',
     'bidSize1', 'bidSize2', 'bidSize3', 'bidSize4', 'bidSize5']

    def __init__(self, data):
        self.data=data
        self.askPrice5, self.askPrice4, self.askPrice3, self.askPrice2, self.askPrice1 = data[3] or None, data[
            4] or None, data[5] or None, data[6] or None, data[7] or None
        self.bidPrice1, self.bidPrice2, self.bidPrice3, self.bidPrice4, self.bidPrice5 = data[8] or None, data[
            9] or None, data[10] or None, data[11] or None, data[12] or None
        self.askSize5, self.askSize4, self.askSize3, self.askSize2, self.askSize1=data[13] or None, data[
            14] or None, data[15] or None, data[16] or None, data[17] or None
        self.bidSize1, self.bidSize2, self.bidSize3, self.bidSize4, self.bidSize5=data[18] or None, data[
            19] or None, data[20] or None, data[21] or None, data[22] or None

    def outputAsDataFrame(self):
        oneLine = pd.DataFrame(data=[self.data], columns=self.outputCols)
        return oneLine


class MarketDataService:

    def __init__(self, marketData_2_exchSim_q, marketData_2_platform_q):
        print("[%d]<<<<< call MarketDataService.init" % (os.getpid(),))
        ltmx = []
        for file in os.walk('processedData/TMX/2019/201904/2019-04-01'):
            for table in file[2]:
                path = file[0] + "/" + table
                data = pd.read_csv(path, encoding='utf-8').iloc[:, 1:]
                data['ticker'] = table[:-4]
                ltmx.append(data)
        tmx = pd.concat(ltmx)
        ltse = []
        for file in os.walk('processedData/TSE/2019/201904/2019-04-01'):
            for table in file[2]:
                path = file[0] + "/" + table
                data = pd.read_csv(path, compression='gzip', encoding='utf-8').iloc[:, 1:]
                data['ticker'] = table[:-7]
                ltse.append(data)
        tse = pd.concat(ltse)
        df = pd.concat([tse, tmx], axis=0, sort=False)
        df['timeStamp'] = pd.to_datetime(df.date, format='%Y-%m-%d') - pd.to_datetime('1900',
                                                                                      format='%Y') + pd.to_datetime(
            df.time, format='%H%M%S%f')
        df.sort_values(by=['timeStamp'], inplace=True)
        df = df[~((df['askPrice5'] == 0) & (df['askPrice4'] == 0) & (df['askPrice3'] == 0) & (df['askPrice2'] == 0) & (
                df['askPrice1'] == 0))]
        df = df[~((df['askSize5'] == 0) & (df['askSize4'] == 0) & (df['askSize3'] == 0) & (df['askSize2'] == 0) & (
                df['askSize1'] == 0))]
        df = df[~((df['bidPrice5'] == 0) & (df['bidPrice4'] == 0) & (df['bidPrice3'] == 0) & (df['bidPrice2'] == 0) & (
                df['bidPrice1'] == 0))]
        df = df[~((df['bidSize5'] == 0) & (df['bidSize4'] == 0) & (df['bidSize3'] == 0) & (df['bidSize2'] == 0) & (
                df['bidSize1'] == 0))]
        df = df.reset_index(drop=True)
        df = df[['timeStamp', 'ticker', 'date', 'time', 'askPrice5', 'askPrice4', 'askPrice3', 'askPrice2', 'askPrice1',
                'bidPrice1', 'bidPrice2', 'bidPrice3', 'bidPrice4', 'bidPrice5', 'askSize5', 'askSize4', 'askSize3',
                'askSize2', 'askSize1', 'bidSize1', 'bidSize2', 'bidSize3', 'bidSize4', 'bidSize5']]
        self.timeStamps = np.unique(np.array(df.timeStamp))
        self.timeintervals = np.diff(self.timeStamps)
        self.datalist = [item[:, 1:] for item in df.groupby('timeStamp').apply(lambda x: np.array(x)).values]
        self.produce_market_data(marketData_2_exchSim_q, marketData_2_platform_q)

    def produce_market_data(self, marketData_2_exchSim_q, marketData_2_platform_q):
        order_id=0
        while True:
            self.produce_quote(marketData_2_exchSim_q, marketData_2_platform_q)
            if order_id ==len(self.timeStamps)-1:
                break
            time.sleep(self.timeintervals[order_id]/1000000000)
            order_id+=1

    def produce_quote(self,marketData_2_exchSim_q, marketData_2_platform_q):
        print('[%d]MarketDataService>>>produce_quote' % (os.getpid()))
        for onetickerdata in self.datalist[0]:
            quoteSnapshot = OrderBookSnapshot_FiveLevels(onetickerdata)
            print(quoteSnapshot.outputAsDataFrame())
            marketData_2_exchSim_q.put(quoteSnapshot)
            marketData_2_platform_q.put(quoteSnapshot)
        self.datalist=self.datalist[1:]
