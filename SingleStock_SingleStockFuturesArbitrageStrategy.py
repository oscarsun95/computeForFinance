#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
Created on July 17 2019

@author: Team 4
"""

import os
import time
from typing import Dict
from common.OrderBookSnapshot_FiveLevels import OrderBookSnapshot_FiveLevels
from common.Strategy import Strategy
from common.SingleStockOrder import SingleStockOrder
from common.SingleStockExecution import SingleStockExecution


class SingleStock_SingleStockFuturesArbitrageStrategy(Strategy):
    
    def __init__(self, stratID, stratName, stratAuthor, ticker, day):
        super(SingleStock_SingleStockFuturesArbitrageStrategy, self).__init__(stratID, stratName, stratAuthor)
        # call constructor of parent
        self.ticker = ticker  # public field
        self.day = day  # public field
        self.currStatusTime = None
        self.position = 0
        self.cash = 100000
        self.snapPrice = 0
        self.marketValue = self.cash + self.position * self.snapPrice
            
    def getStratDay(self):
        return self.day

    def on_marketData(self, marketData: Dict[str, OrderBookSnapshot_FiveLevels]):
        # handle new market data
        singleStockOrder = SingleStockOrder('stock', '2019-07-05', time.asctime(time.localtime(time.time())))
        singleStockOrder.submissionTime = time.asctime(time.localtime(time.time()))
        singleStockOrder.currStatus = "New"  # "New", "Filled", "PartiallyFilled", "Cancelled"
        singleStockOrder.direction = 1
        singleStockOrder.price = marketData['stock'].askPrice1
        singleStockOrder.size = self.cash * 0.2 // singleStockOrder.price
        print('*******strat size: ', singleStockOrder.size)
        singleStockOrder.type = "MO"  # "MLO", "LO", "MO", "TWAP"
        # TODO:: just a sample, need to add future order details
        singleFutureOrder = singleStockOrder.copyOrder()
        singleFutureOrder.ticker = 'future'
        singleFutureOrder.price = marketData['future'].bidPrice1
        singleFutureOrder.direction = singleStockOrder.direction * -1
        return [singleStockOrder, singleFutureOrder]

    def on_execution(self, executions: Dict[str, SingleStockExecution]):
        # handle executions
        print('[%d] Strategy.handle_execution' % (os.getpid()))
        execution = executions['stock']
        # TODO::need to handle future execution
        print('execution:', execution.outputAsArray())

        self.currStatusTime = execution.timeStamp
        self.position += execution.direction * execution.size
        self.cash -= (execution.direction * execution.size * execution.price + execution.comm)
        self.snapPrice = execution.price
        self.marketValue = self.cash + self.position * self.snapPrice

        print('*******strat position: ', self.position)
        print('*******strat cash: ', self.cash)
        print('*******strat marketValue: ', self.marketValue)

        return None

    def run(self, marketData, execution):
        if (marketData is None) and (execution is None):
            return None
        elif (marketData is None) and ((execution is not None) and (isinstance(execution, SingleStockExecution))):
            # handle executions
            print('[%d] Strategy.handle_execution' % (os.getpid()))
            print('execution:', execution.outputAsArray())
            
            self.currStatusTime = execution.timeStamp
            self.position += execution.direction * execution.size
            self.cash -= (execution.direction * execution.size * execution.price + execution.comm)
            self.snapPrice = execution.price
            self.marketValue = self.cash + self.position * self.snapPrice
            
            print('*******strat position: ', self.position)
            print('*******strat cash: ', self.cash)
            print('*******strat marketValue: ', self.marketValue)

            return None
        elif ((marketData is not None) and (isinstance(marketData, OrderBookSnapshot_FiveLevels))) and (execution is None):
            # handle new market data
            singleStockOrder = SingleStockOrder('testTicker', '2019-07-05', time.asctime(time.localtime(time.time())))
            singleStockOrder.submissionTime = time.asctime(time.localtime(time.time()))
            singleStockOrder.currStatus = "New"  # "New", "Filled", "PartiallyFilled", "Cancelled"
            singleStockOrder.direction = 1
            singleStockOrder.price = marketData.outputAsDataFrame()['askPrice1']
            singleStockOrder.size = (self.cash > 0) * self.cash * 0.2 // singleStockOrder.price
            print('*******strat size: ', singleStockOrder.size)
            singleStockOrder.type = None  # "MLO", "LO", "MO", "TWAP"

            return singleStockOrder
        else:
            return None
