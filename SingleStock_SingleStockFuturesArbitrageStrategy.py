#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""

@author: Team 4
"""

import os
import time
import pandas as pd
import numpy as np
from typing import Dict
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from common.Strategy import Strategy
from common.SingleStockOrder import SingleStockOrder
from common.SingleStockExecution import SingleStockExecution


class SingleStock_SingleStockFuturesArbitrageStrategy(Strategy):

    def __init__(self, stratID, stratName, stratAuthor, ticker, day):
        super(SingleStock_SingleStockFuturesArbitrageStrategy, 
                          self).__init__(stratID, stratName, stratAuthor)  # call constructor of parent
        self.ticker = ticker  # public field # list
        self.day = day  # public field
        self.initialStatusTime = time.asctime(time.localtime(time.time()))
        self.initialCash = 2000.0
        self.bookRecordsCol = ['day', 'currStatusTime', 'cash',
                  'currPosition_{}'.format(self.ticker[0]), 'currPosition_{}'.format(self.ticker[1]),
                  'snapPrice_{}'.format(self.ticker[0]), 'snapPrice_{}'.format(self.ticker[1]),
                  'marketValue']
        self.bookRecords = pd.DataFrame(columns = self.bookRecordsCol,
                                        data = [[self.day, self.initialStatusTime,
                                                self.initialCash, 0, 0, 
                                                0.0, 0.0, self.initialCash]])
        self.bookRecords.to_csv('bookRecords.csv')

        self.spreadPosition = 0
        self.executionFinished = 1
        self.thresholdOpen = 0.003
        self.thresholdClose = 0.0015
        
    def setSingleOrder(self, ticker, day, submissionTime, 
                       orderDirection, orderPrice, orderSize, orderType):
        singleStockOrder = SingleStockOrder(ticker, day, submissionTime)
        singleStockOrder.direction = orderDirection
        singleStockOrder.price = orderPrice
        singleStockOrder.size = orderSize
        singleStockOrder.type = orderType
        return singleStockOrder
    
    def updateBookRecoreds(self, inputData):
        
        day_old, currStatusTime_old, cash_old, currPosition0_old, currPosition1_old, \
            snapPrice0_old, snapPrice1_old, marketValue_old = self.bookRecords.iloc[-1]
        
        if isinstance(inputData[self.ticker[0]], SingleStockExecution):
            
            execution = pd.DataFrame(data = [inputData[self.ticker[0]].outputAsArray(),
                                             inputData[self.ticker[1]].outputAsArray()], 
                                     columns = ['date', 'ticker', 'timeStamp',
                                                'execID', 'orderID', 'direction', 
                                                'price', 'size', 'comm'], 
                                                index = [[0, 1]])
            
            print('updateBookRecords: ', inputData[self.ticker[0]].outputAsArray())
            print('updateBookRecords: ', inputData[self.ticker[1]].outputAsArray())

            
            execution['currPosition_old'] = [currPosition0_old, currPosition1_old]
            execution['price_old'] = [snapPrice0_old, snapPrice1_old]
    
            day_new = execution['date'].max()
            
            currStatusTime_new = execution['timeStamp'].max()
            
            currPosition0_new, currPosition1_new = execution['currPosition_old'] \
                                                    + execution['direction'] * execution['size']
                                                    
            cash_new = cash_old - (execution['direction'] * execution['size'] \
                                   * execution['price'] + execution['comm']).sum()
            
            snapPrice0_new, snapPrice1_new = execution['price']
            
            marketValue_new = marketValue_old + (execution['currPosition_old'] \
                                * (execution['price'] \
                                   - execution['price_old']) \
                                - execution['comm']).sum()
                                
            self.bookRecords = self.bookRecords.append(pd.DataFrame(data = [[day_new, currStatusTime_new, 
                                                                  cash_new, currPosition0_new, 
                                                                  currPosition1_new, 
                                                                  snapPrice0_new, snapPrice1_new, 
                                                                  marketValue_new]], 
                                                                    columns = self.bookRecordsCol, 
                                                                    index = [len(self.bookRecords)]))      
                  
            self.bookRecords.to_csv('bookRecords.csv')
        else:
            print('Input is not SingleStockExecution')
        
            
    def getStratDay(self):
        return self.day

    def on_marketData(self, marketData: Dict[str, OrderBookSnapshot_FiveLevels]):
        
        print('&$&$&$&$ Strategy received market data:\n', 
              marketData[self.ticker[0]].outputAsDataFrame(), '\n',
              marketData[self.ticker[1]].outputAsDataFrame())

        ticker0Bid1 = marketData[self.ticker[0]].outputAsDataFrame()['bidPrice1'].iat[0]
        ticker0Ask1 = marketData[self.ticker[0]].outputAsDataFrame()['askPrice1'].iat[0]
        ticker0MidQ = 1/2 * (ticker0Ask1 + ticker0Bid1)
        
        ticker1Bid1 = marketData[self.ticker[1]].outputAsDataFrame()['bidPrice1'].iat[0]
        ticker1Ask1 = marketData[self.ticker[1]].outputAsDataFrame()['askPrice1'].iat[0]
        ticker1MidQ = 1/2 * (ticker1Ask1 + ticker1Bid1)
        
        relativeReturn = np.log(ticker1MidQ / ticker0MidQ)
        thresholdOpen = self.thresholdOpen
        thresholdClose = self.thresholdClose
        sizePair = 1
        
        print('>+>+>+>+ log(Pf/Ps):\n', relativeReturn)
        
        if self.executionFinished == 1:
        
            if self.spreadPosition == 0 and relativeReturn > thresholdOpen:
                print('>>>>>>>>>>>>Open position: +1')
                singleStockOrder0 = self.setSingleOrder(self.ticker[0], '2019-07-06', 
                                                  time.asctime(time.localtime(time.time())),
                                                  1, None, sizePair, 'MO')
                singleStockOrder1 = self.setSingleOrder(self.ticker[1], '2019-07-06', 
                                                  time.asctime(time.localtime(time.time())),
                                                  -1, None, sizePair, 'MO')
                self.spreadPosition = 1
                self.executionFinished = 0
                return [singleStockOrder0, singleStockOrder1]
    
            elif self.spreadPosition == 0 and relativeReturn < -thresholdOpen:
                print('>>>>>>>>>>>>Open position: -1')
                singleStockOrder0 = self.setSingleOrder(self.ticker[0], '2019-07-06', 
                                                  time.asctime(time.localtime(time.time())),
                                                  -1, None, sizePair, 'MO')
                singleStockOrder1 = self.setSingleOrder(self.ticker[1], '2019-07-06', 
                                                  time.asctime(time.localtime(time.time())),
                                                  1, None, sizePair, 'MO')
                self.spreadPosition = -1
                self.executionFinished = 0
                return [singleStockOrder0, singleStockOrder1]
            
            elif self.spreadPosition == 1 and relativeReturn < thresholdClose:    
                print('>>>>>>>>>>>>Close position: 0')    
                singleStockOrder0 = self.setSingleOrder(self.ticker[0], '2019-07-06', 
                                                  time.asctime(time.localtime(time.time())),
                                                  -1, None, sizePair, 'MO')
                singleStockOrder1 = self.setSingleOrder(self.ticker[1], '2019-07-06', 
                                                  time.asctime(time.localtime(time.time())),
                                                  1, None, sizePair, 'MO')    
                self.spreadPosition = 0  
                self.executionFinished = 0
                return [singleStockOrder0, singleStockOrder1]
            
            elif self.spreadPosition == -1 and relativeReturn > -thresholdClose:    
                print('>>>>>>>>>>>>Close position: 0')
                singleStockOrder0 = self.setSingleOrder(self.ticker[0], '2019-07-06', 
                                                  time.asctime(time.localtime(time.time())),
                                                  1, None, sizePair, 'MO')
                singleStockOrder1 = self.setSingleOrder(self.ticker[1], '2019-07-06', 
                                                  time.asctime(time.localtime(time.time())),
                                                  -1, None, sizePair, 'MO')
                self.spreadPosition = 0
                self.executionFinished = 0
                return [singleStockOrder0, singleStockOrder1]
            
            if  self.executionFinished == 0:

                print('&!&!&!&! Strategy sent orders:\n', 
                singleStockOrder0.outputAsArray(), '\n',
                singleStockOrder1.outputAsArray())
        
#        # test order submission
#        singleStockOrder0 = self.setSingleOrder(self.ticker[0], '2019-04-01', 
#                                              time.asctime(time.localtime(time.time())),
#                                              1, None, sizePair, 'MO')
#        singleStockOrder1 = self.setSingleOrder(self.ticker[1], '2019-04-01', 
#                                              time.asctime(time.localtime(time.time())),
#                                              -1, None, sizePair, 'MO')
##            self.spreadPosition = 1
        
#        return [singleStockOrder0, singleStockOrder1]

            
    def on_execution(self, executions: Dict[str, SingleStockExecution]):
        #handle executions
#        print('[%d] Strategy.handle_execution' % (os.getpid()))
        print('!~!~!~!~ Strategy received execution results:\n',
              executions[self.ticker[0]].outputAsArray(), '\n',
              executions[self.ticker[1]].outputAsArray())

        if (executions[self.ticker[0]].size not in [None, 0]) or (executions[self.ticker[1]].size not in [None, 0]):
            self.updateBookRecoreds(executions)
            self.executionFinished = 1
############# End