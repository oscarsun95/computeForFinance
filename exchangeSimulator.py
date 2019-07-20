# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:12:21 2019

@author: 
"""
import pandas as pd
import threading
import os
import time
from common.SingleStockExecution import SingleStockExecution

class ExchangeSimulator:
    
    def __init__(self, marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        print("[%d]<<<<< call ExchSim.init" % (os.getpid(),))
        
        t_md = threading.Thread(name='exchsim.on_md', target=self.consume_md, args=(marketData_2_exchSim_q,))
        t_md.start()
        
        t_order = threading.Thread(name='exchsim.on_order', target=self.consume_order, args=(platform_2_exchSim_order_q, exchSim_2_platform_execution_q, ))
        t_order.start()
        
        ######################################################
        self.OrderBook = None
        self.OrderBook_20 = None
        self.Flag_KeepAllQuotes = False
        self.Flag_Keep20Quotes = True
        self.OrderBookIsEmpty = True
        self.OrderBook20IsEmpty = True
        self.OrderBookCurrent = None
        ######################################################

    def consume_md(self, marketData_2_exchSim_q):
        while True:
            res = marketData_2_exchSim_q.get()
            print('[%d]ExchSim.consume_md' % (os.getpid()))
            print(res.outputAsDataFrame())
            
            #update orderbook
            if self.Flag_KeepAllQuotes:
                if self.OrderBookIsEmpty:
                    self.OrderBook = res.outputAsDataFrame().copy(deep=True)
                    self.OrderBookIsEmpty = False
                else:
                    self.OrderBook = pd.concat([self.OrderBook,res.outputAsDataFrame()])
                    self.OrderBook.reset_index(drop=True,inplace=True)
            
            #update orderbook_20
            if self.OrderBook20IsEmpty:
                self.OrderBook_20 = res.outputAsDataFrame().copy(deep=True)
                self.OrderBook20IsEmpty = False
            else:
                if len(self.OrderBook_20) == 20:
                    self.OrderBook_20.drop(index=[0], inplace=True)
                self.OrderBook_20 = pd.concat([self.OrderBook_20,res.outputAsDataFrame()])
                self.OrderBook_20.reset_index(drop=True,inplace=True)
                
            #update orderbookcurrent
            self.OrderBookCurrent = res.outputAsDataFrame().copy(deep=True)
            
            
    def consume_order(self, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        while True:  # 说不定可以做一个market_open的boolean开关 ———— GU
            res = platform_2_exchSim_order_q.get()
            print('[%d]ExchSim.on_order' % (os.getpid()))
            print(res.outputAsArray())
            self.produce_execution(res, exchSim_2_platform_execution_q)
    
    def produce_execution(self, order, exchSim_2_platform_execution_q):
        # In this step, we need to create an instance of SingleStockExecution regarding to the current orderbook and the order
        execution = SingleStockExecution(order.ticker, order.date, time.asctime(time.localtime(time.time())))
        execution.orderID = order.orderID
#        execution.execID =  What is execID
        execution.direction = order.direction
        # Depending on the order type, decide the next step
        if order.type == 'MO':
            if order.direction > 0:
                ### This is a buy MO
                remainingSize = order.size
                execPrice = []
                execSize = []
                for ii in range(5):
                    if remainingSize > 0:
                        AvailbleSizeWithLowestPrice = self.OrderBookCurrent['askSize{}'.format(str(ii+1))].iloc[0]
                        if AvailbleSizeWithLowestPrice > remainingSize:
                            execPrice.append(self.OrderBookCurrent['askPrice{}'.format(str(ii+1))].iloc[0])
                            execSize.append(remainingSize)
                            remainingSize = 0
                        else:
                            execPrice.append(self.OrderBookCurrent['askPrice{}'.format(str(ii+1))].iloc[0])
                            execSize.append(AvailbleSizeWithLowestPrice)
                            remainingSize -= AvailbleSizeWithLowestPrice
                totalSize = sum(execSize)
                totalPrice = sum([execPrice[jj]*execSize[jj] for jj in range(len(execPrice))])
                averagePrice = totalPrice / totalSize
                execution.size = totalSize
                execution.price = averagePrice
            if order.direction < 0:
                ### This is a sell MO
                remainingSize = order.size
                execPrice = []
                execSize = []
                for ii in range(5):
                    if remainingSize > 0:
                        AvailbleSizeWithLowestPrice = self.OrderBookCurrent['bidSize{}'.format(str(ii+1))].iloc[0]
                        if AvailbleSizeWithLowestPrice > remainingSize:
                            execPrice.append(self.OrderBookCurrent['bidPrice{}'.format(str(ii+1))].iloc[0])
                            execSize.append(remainingSize)
                            remainingSize = 0
                        else:
                            execPrice.append(self.OrderBookCurrent['bidPrice{}'.format(str(ii+1))].iloc[0])
                            execSize.append(AvailbleSizeWithLowestPrice)
                            remainingSize -= AvailbleSizeWithLowestPrice
                totalSize = sum(execSize)
                totalPrice = sum([execPrice[jj]*execSize[jj] for jj in range(len(execPrice))])
                averagePrice = totalPrice / totalSize
                execution.size = totalSize
                execution.price = averagePrice    
# ------------------------------------------------------ added by GU
        else:
            print('order type neither limit order nor market order!')
# ------------------------------------------------------ added by GU
        exchSim_2_platform_execution_q.put(execution)
        print('[%d]ExchSim.produce_execution' % (os.getpid()))
        print(execution.outputAsArray())
