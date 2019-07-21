# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:15:48 2019

@author: hongsong chou
"""

import threading
import os
from SingleStock_SingleStockFuturesArbitrageStrategy import SingleStock_SingleStockFuturesArbitrageStrategy


class TradingPlatform:

    ssfArbStrat = None
    
    def __init__(self, marketData_2_platform_q, platform_2_exchSim_order_q, exchSim_2_platform_execution_q):
        print("[%d]<<<<< call Platform.init" % (os.getpid(),))
        
        # Instantiate individual strategies
        self.ssfArbStrat = SingleStock_SingleStockFuturesArbitrageStrategy(
            "tf_1", "singleStock_singleStockFuturesArbStrategy", "hongsongchou", "2330", "20190706")

        t_md = threading.Thread(name='platform.on_marketData', target=self.consume_marketData,
                                args=(platform_2_exchSim_order_q, marketData_2_platform_q,))
        t_md.start()
        
        t_exec = threading.Thread(name='platform.on_exec', target=self.handle_execution,
                                  args=(exchSim_2_platform_execution_q, ))
        t_exec.start()

    def consume_marketData(self, platform_2_exchSim_order_q, marketData_2_platform_q):
        print('[%d]Platform.consume_marketData' % (os.getpid(),))
        while True:
            res = marketData_2_platform_q.get()
            print('[%d] Platform.on_md' % (os.getpid()))
            print(res.outputAsDataFrame())
            # result = self.ssfArbStrat.run(res, None)
            result = self.ssfArbStrat.on_marketData(res)
            if result is not None:
                # do something with the new order
                platform_2_exchSim_order_q.put(result)
    
    def handle_execution(self, exchSim_2_platform_execution_q):
        print('[%d]Platform.handle_execution' % (os.getpid(),))
        while True:
            execution = exchSim_2_platform_execution_q.get()
            if execution is not None:
                print('[%d] Platform.handle_execution' % (os.getpid()))
                print(execution.outputAsArray())
                self.ssfArbStrat.on_execution(execution)
                # self.ssfArbStrat.run(None, execution)
