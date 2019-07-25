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

# ---------------------
# pkg to support execID
#from typing import List
# ---------------------

class ExchangeSimulator:
    
    def __init__(self, marketData_2_exchSim_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        print("[%d]<<<<< call ExchSim.init" % (os.getpid(),))
        
        t_md = threading.Thread(name='exchsim.on_md', target=self.consume_md, args=(marketData_2_exchSim_q,exchSim_2_platform_execution_q,))
        t_md.start()
        
        t_order = threading.Thread(name='exchsim.on_order', target=self.consume_order, args=(platform_2_exchSim_order_q, exchSim_2_platform_execution_q, ))
        t_order.start()
        
        ######################################################
        self.OrderBook = {}  # A dictionary to record all quotes
        self.OrderBook_20 = {}  # A dictionary to record 20 records
        self.OrderBookCurrent = {}  # A dictionary to record the most recent record
        self.OrderBook_own = {}

        self.Flag_KeepAllQuotes = False  # Flag to control the behavior of OrderBook
        self.Flag_Keep20Quotes = True  # Flag to control the behavior of OrderBook_20
        self.TickerRecord = []  # A list to record the ticker
        self.Flag_EasyExecution = True  # When True, all the order will be regard as a MO with size 1

        self.ExecRecordColumn = ['Date', 'Ticker', 'timeStamp', 'execID', 'OrderID', 'direction', 'price', 'size', 'common']
        self.ExecRecord = pd.DataFrame(columns=self.ExecRecordColumn)
        ######################################################
        ######################################################
        #Codes for limit order (part 1/3)
        self.LimitOrderFlag = True
        self.LimitOrderBook = {}
        self.LimitOrderBookTickerRecord = []
        ######################################################

    def consume_md(self, marketData_2_exchSim_q,exchSim_2_platform_execution_q):
        while True:
            res = marketData_2_exchSim_q.get()
            print('[%d]ExchSim.consume_md' % (os.getpid()))
            print(res.outputAsDataFrame())
            
            ticker = res.ticker
            if ticker not in self.TickerRecord:
                # if the ticker is new
                # record the ticker
                self.TickerRecord.append(ticker)
                # create the orderbook for the ticker
                if self.Flag_KeepAllQuotes:
                    self.OrderBook[ticker] = res.outputAsDataFrame().copy(deep=True)
                # Create the orderbook_20 for the ticker
                if self.Flag_Keep20Quotes:
                    self.OrderBook_20[ticker] = res.outputAsDataFrame().copy(deep=True)
                #Create the orderbookcurrent for the ticker
                self.OrderBookCurrent[ticker] = res.outputAsDataFrame().copy(deep=True)
            else:
                # if the ticker is old
                # update the Orderbook
                if self.Flag_KeepAllQuotes:
                    self.OrderBook[ticker] = pd.concat([self.OrderBook[ticker],res.outputAsDataFrame()])
                    self.OrderBook[ticker].reset_index(drop=True,inplace=True)
                # update the OrderBook_20
                if self.Flag_Keep20Quotes:
                    if len(self.OrderBook_20[ticker]) == 20:
                        self.OrderBook_20[ticker].drop(index=[0], inplace=True)
                    self.OrderBook_20[ticker] = pd.concat([self.OrderBook_20[ticker],res.outputAsDataFrame()])
                    self.OrderBook_20[ticker].reset_index(drop=True,inplace=True)
                # update the OrderBookCurrent
                self.OrderBookCurrent[ticker] = res.outputAsDataFrame().copy(deep=True)
                
                
            ######################################################
            #Codes for limit order (part 2/3)
            if self.LimitOrderFlag:
                Ticker_ = "B" + ticker
                if Ticker_ in self.LimitOrderBookTickerRecord:
                    self.crossTwoOrderBook(Ticker_,exchSim_2_platform_execution_q,initiator="Market")
                    
                Ticker_ = "S" + ticker
                if Ticker_ in self.LimitOrderBookTickerRecord:
                    self.crossTwoOrderBook(Ticker_,exchSim_2_platform_execution_q,initiator="Market")

            
            
            
            
#-------------------------------------------------------------------------------
#Codes for supporting LO (part 3/3)
    def comsume_limit_order(self, order, exchSim_2_platform_execution_q):
        # deal with limit order
        # get the info of the order
        ticker = order.ticker
        direction = order.direction
        if direction > 0:
            Ticker_ = "B"+ticker
        else:
            Ticker_ = "S"+ticker
        price = order.price
        submissionTime = order.submissionTime
        
        
        
        # determine whether the limit orderbook orginally exist
        if Ticker_ in self.LimitOrderBookTickerRecord:
            if price in self.LimitOrderBook[Ticker_].keys():
                self.LimitOrderBook[Ticker_][price].append(order)
            else:
                self.LimitOrderBook[Ticker_][price] = [order]
        else:
            self.LimitOrderBookTickerRecord.append(Ticker_)
            self.LimitOrderBook[Ticker_] = {price:[order]}
        
        # cross the two order book
        self.crossTwoOrderBook(Ticker_, exchSim_2_platform_execution_q, initiator = "NotMarket")



    def crossTwoOrderBook(self, Ticker_, exchSim_2_platform_execution_q, initiator = "Market"):
        # cross the particular orderbook and our own orderbook
        ticker = Ticker_[1:]
        lowestMarketSellPrice = self.OrderBookCurrent[ticker]['askPrice1'].iloc[0]
        highestMarketBuyPrice = self.OrderBookCurrent[ticker]['bidPrice1'].iloc[0]
        if Ticker_[0] == "B":
            prices = list(self.LimitOrderBook[Ticker_].keys())
            for price in prices:
                if price >= lowestMarketSellPrice:
                    for order_ in self.LimitOrderBook[Ticker_][price]:
                        if initiator == "Market":
                            execPrice = price
                        else:
                            execPrice = lowestMarketSellPrice
                        self.produce_execution_limit_order(order_, execPrice, exchSim_2_platform_execution_q)
                    self.LimitOrderBook[Ticker_].pop(price)
                    
        if Ticker_[0] == "S":
            prices = list(self.LimitOrderBook[Ticker_].keys())
            for price in prices:
                if price <= highestMarketBuyPrice:
                    for order_ in self.LimitOrderBook[Ticker_][price]:
                        if initiator == "Market":
                            execPrice = price
                        else:
                            execPrice = highestMarketBuyPrice
                        self.produce_execution_limit_order(order_, execPrice, exchSim_2_platform_execution_q)
                    self.LimitOrderBook[Ticker_].pop(price)
                    
        if len(self.LimitOrderBook[Ticker_])== 0:
            self.LimitOrderBook.pop(Ticker_)
            self.LimitOrderBookTickerRecord.remove(Ticker_)
            
            
            
    def produce_execution_limit_order(self, order, execPrice, exchSim_2_platform_execution_q):
        execution = SingleStockExecution(order.ticker,order.date, time.asctime(time.localtime(time.time())))
        execution.orderID = order.orderID
        
        if len(self.ExecRecord) == 0:
            execution.execID = str(order.date + order.ticker + '000000')
        else:
            execution.execID = str(order.date + order.ticker + '%06d' % len(self.ExecRecord))

        execution.direction = order.direction
        execution.size = order.size
        execution.price = execPrice
        
        
        tempList=execution.outputAsArray()
        tempList[3]=execution.execID
        tempDf=pd.DataFrame([tempList], columns=self.ExecRecordColumn)
        self.ExecRecord=self.ExecRecord.append(tempDf,ignore_index=True)
        
        exchSim_2_platform_execution_q.put(execution)
        print('[%d]ExchSim.produce_execution' % (os.getpid()))
        print(execution.outputAsArray())


#-------------------------------------------------------------------------------

    def consume_order(self, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        while True:
            res = platform_2_exchSim_order_q.get()
            print('[%d]ExchSim.on_order' % (os.getpid()))
            print(res.outputAsArray())
            
            if self.LimitOrderFlag:
                if res.type == "LO":
                    self.comsume_limit_order(res, exchSim_2_platform_execution_q)
                else:
                    self.produce_execution(res, exchSim_2_platform_execution_q)
            else:
                self.produce_execution(res, exchSim_2_platform_execution_q)
    
    def produce_execution(self, order, exchSim_2_platform_execution_q):
        # In this step, we need to create an instance of SingleStockExecution regarding to the current orderbook and the order
        execution = SingleStockExecution(order.ticker, order.date, time.asctime(time.localtime(time.time())))
        execution.orderID = order.orderID
#        execution.execID =  What is execID
        # calculate the execID
        if len(self.ExecRecord) == 0:
            execution.execID = str(order.date + order.ticker + '000000')
        else:
            execution.execID = str(order.date + order.ticker + '%06d' % len(self.ExecRecord))

        # Get the direction       
        execution.direction = order.direction
        
        # Get the ticker from the order
        ticker = order.ticker
        
        #In case there is no market data
        while(True):
#            if ticker in self.OrderBookCurrent.keys(): 
            if ticker in self.TickerRecord:
                break
            time.sleep(0.0001)
        
        # EasyExecution will always fully filled at Ask/Bid Price 1.
        if self.Flag_EasyExecution:
            execution.size = order.size
            if order.direction > 0:
                execution.price = self.OrderBookCurrent[ticker]['askPrice1'].iloc[0]
            elif order.direction < 0:
                execution.price = self.OrderBookCurrent[ticker]['bidPrice1'].iloc[0]


        else:
            if order.type == 'MO':
                if order.direction > 0:
                    ### This is a buy MO
                    remainingSize = order.size
                    execPrice = []
                    execSize = []
                    for ii in range(5):
                        if remainingSize > 0:
                            AvailbleSize = self.OrderBookCurrent[ticker]['askSize{}'.format(str(ii+1))].iloc[0]
                            if AvailbleSize > remainingSize:
                                execPrice.append(self.OrderBookCurrent[ticker]['askPrice{}'.format(str(ii+1))].iloc[0])
                                execSize.append(remainingSize)
#                                self.OrderBookCurrent[ticker]['askSize{}'.format(str(ii+1))].iloc[0] -= remainingSize
                                self.OrderBookCurrent[ticker].loc[0,'askSize{}'.format(str(ii+1))] -= remainingSize
                                remainingSize = 0
                            else:
                                execPrice.append(self.OrderBookCurrent[ticker]['askPrice{}'.format(str(ii+1))].iloc[0])
                                execSize.append(AvailbleSize)
    #                            self.OrderBookCurrent[ticker]['askSize{}'.format(str(ii+1))].iloc[0] -= AvailbleSize
                                self.OrderBookCurrent[ticker].loc[0,'askSize{}'.format(str(ii+1))] -= AvailbleSize
                                remainingSize -= AvailbleSize
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
                            AvailbleSize = self.OrderBookCurrent[ticker]['bidSize{}'.format(str(ii+1))].iloc[0]
                            if AvailbleSize > remainingSize:
                                execPrice.append(self.OrderBookCurrent[ticker]['bidPrice{}'.format(str(ii+1))].iloc[0])
                                execSize.append(remainingSize)
    #                            self.OrderBookCurrent[ticker]['bidSize{}'.format(str(ii+1))].iloc[0] -= remainingSize
                                self.OrderBookCurrent[ticker].loc[0,'bidSize{}'.format(str(ii+1))] -= remainingSize
                                remainingSize = 0
                            else:
                                execPrice.append(self.OrderBookCurrent[ticker]['bidPrice{}'.format(str(ii+1))].iloc[0])
                                execSize.append(AvailbleSize)
    #                            self.OrderBookCurrent[ticker]['bidSize{}'.format(str(ii+1))].iloc[0] -= AvailbleSize
                                self.OrderBookCurrent[ticker].loc[0, 'bidSize{}'.format(str(ii+1))] -= AvailbleSize
                                remainingSize -= AvailbleSize
                    totalSize = sum(execSize)
                    totalPrice = sum([execPrice[jj]*execSize[jj] for jj in range(len(execPrice))])
                    averagePrice = totalPrice / totalSize
                    execution.size = totalSize
                    execution.price = averagePrice    
                    
                    
        tempList=execution.outputAsArray()
        tempList[3]=execution.execID
        tempDf=pd.DataFrame([tempList], columns=self.ExecRecordColumn)
        self.ExecRecord=self.ExecRecord.append(tempDf,ignore_index=True)
        
        exchSim_2_platform_execution_q.put(execution)
        print('[%d]ExchSim.produce_execution' % (os.getpid()))
        print(execution.outputAsArray())
