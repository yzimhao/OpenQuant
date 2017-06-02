#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 futu, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from .futu_utils import *
from rqalpha.utils.logger import system_log

#富途的市场状态
class Futu_Market_State(Enum):
    MARKET_NONE = "none"
    MARKET_PRE_OPEN = 'pre_open'
    MARKET_OPEN = 'open'
    MARKET_REST = 'rest'
    MARKET_CLOSE = 'close'


class FUTUMarketStateSource(object):
    def __init__(self, env, quote_context):
        self._env = env
        self._quote_context = quote_context
        self._market_state = None
        self._mkt_key = ""
        self._mkt_dic = {0: Futu_Market_State.MARKET_NONE,  # 未开盘
                   1: Futu_Market_State.MARKET_PRE_OPEN,  # 竞价交易(港股)
                   2: Futu_Market_State.MARKET_PRE_OPEN,  # 早盘前等待开盘(港股)
                   3: Futu_Market_State.MARKET_OPEN,  # 早盘(港股)/ 盘中(美股)
                   4: Futu_Market_State.MARKET_REST,  # 午休(A|港股)
                   5: Futu_Market_State.MARKET_OPEN,  # 午盘(A|港股)
                   6: Futu_Market_State.MARKET_CLOSE,  # 交易日结束(A|港股) / 已收盘(美股)
                   8: Futu_Market_State.MARKET_PRE_OPEN,  # 盘前开始(美股)
                   9: Futu_Market_State.MARKET_PRE_OPEN,  # 盘前结束(美股)
                   10: Futu_Market_State.MARKET_CLOSE,  # 盘后开始(美股)
                   11: Futu_Market_State.MARKET_CLOSE,  # 盘后结束(美股)
                   12: Futu_Market_State.MARKET_NONE,  # 盘后结束(美股)

                   13: Futu_Market_State.MARKET_OPEN,  # 夜市交易中(港期货)
                   14: Futu_Market_State.MARKET_CLOSE,  # 夜市收盘(港期货)
                   15: Futu_Market_State.MARKET_OPEN,  # 日市交易中(港期货)
                   16: Futu_Market_State.MARKET_REST,  # 日市午休(港期货)
                   17: Futu_Market_State.MARKET_CLOSE,  # 日市收盘(港期货)
                   18: Futu_Market_State.MARKET_PRE_OPEN,  # 日市等待开盘(港期货)

                   19: Futu_Market_State.MARKET_CLOSE,  # 港股盘后竞价
                   }

        if IsFutuMarket_CNStock():
            self._mkt_key = 'Market_SZ'
        elif IsFutuMarket_HKStock():
            self._mkt_key = 'Market_HK'
        elif IsFutuMarket_USStock():
            self._mkt_key = 'Market_US'
        else:
            raise RuntimeError("Market Error")

        # 定时请求市场状态
        market_state_thread = Thread(target=self._mark_state_timer_query)
        market_state_thread.setDaemon(True)
        market_state_thread.start()


    def get_futu_market_state(self):
        return self._market_state

    def _mark_state_timer_query(self):
        while True:
            print("定时请求当前市场状态")
            ret, state_dict = self._quote_context.get_global_state()

            if ret == 0:
                mkt_val = int(state_dict[self._mkt_key])
                if mkt_val in self._mkt_dic.keys():
                    self._market_state = self._mkt_dic[mkt_val]
                else:
                    err_log = "Unkown market state: {}".format(mkt_val)
                    system_log.error(err_log)
            sleep(3)
        pass












