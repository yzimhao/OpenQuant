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

#富途的市场状态
class Futu_Market_State(Enum):
    MARKET_PRE_OPEN = 'pre_open'
    MARKET_OPEN = 'open'
    MARKET_REST = 'rest'
    MARKET_CLOSE = 'close'

class FUTUMarketStateSource(object):
    def __init__(self, env, quote_context):
        self._env = env
        self._quote_context = quote_context
        self._market_state = None

        # 定时请求市场状态
        market_state_thread = Thread(target=self._mark_state_timer_query)
        market_state_thread.setDaemon(True)
        market_state_thread.start()

    def get_futu_market_state(self):
        return self._market_state

    def _mark_state_timer_query(self):
        while True:
            print("定时请求当前市场状态")
            #_quote_context.query_market_state
            self._market_state = Futu_Market_State.MARKET_OPEN
            sleep(5)
        pass












