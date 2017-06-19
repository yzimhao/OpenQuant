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

from rqalpha.environment import Environment
from rqalpha.mod.rqalpha_mod_sys_simulation.simulation_event_source import SimulationEventSource
from rqalpha.interface import AbstractEventSource
from rqalpha.utils.logger import system_log
from rqalpha.interface import AbstractEventSource
from rqalpha.events import Event, EVENT
from rqalpha.utils import RqAttrDict

from datetime import datetime, timedelta, date
from .futu_market_state import *

#回测的event 直接用rqalpha 的simyulation就可以
class FUTUEventForBacktest(SimulationEventSource):
    def __init__(self, env):
        super(FUTUEventForBacktest, self).__init__(env)

class TimePeriod(Enum):
    BEFORE_TRADING = 'before_trading'
    AFTER_TRADING = 'after_trading'
    REST = "rest"
    TRADING = 'trading'
    CLOSING = 'closing'

#实时策略的event
class FUTUEventForRealtime(AbstractEventSource):
    def __init__(self, env, mod_config, market_state_source):
        self._env = env
        self._mod_config = mod_config
        fps = int(float(self._mod_config.futu_bar_fps) * 1000) #转成毫秒
        self._fps_delta_dt = timedelta(days= 0, seconds= fps//1000, microseconds= fps%1000)

        self._before_trading_processed = False
        self._after_trading_processed = False
        self._time_period = None
        self._market_state_source = market_state_source
        self._last_onbar_dt = None

    def mark_time_period(self, start_date, end_date):
        trading_days = self._env.data_proxy.get_trading_dates(start_date, end_date)

        def in_before_trading_time(time):
            return self._market_state_source.get_futu_market_state() == Futu_Market_State.MARKET_PRE_OPEN

        def in_after_trading(time):
            return self._market_state_source.get_futu_market_state() == Futu_Market_State.MARKET_CLOSE

        def in_rest_trading(time):
            return self._market_state_source.get_futu_market_state() == Futu_Market_State.MARKET_REST

        def in_trading_time(time):
            return self._market_state_source.get_futu_market_state() == Futu_Market_State.MARKET_OPEN

        def in_trading_day(time):
            if time.date() in trading_days:
                return True
            return False

        while True:
            now = datetime.now()
            if in_trading_time(now):
                self._time_period = TimePeriod.TRADING
                continue
            if in_rest_trading(now):
                self._time_period = TimePeriod.REST
                continue
            if not in_trading_day(now):
                self._time_period = TimePeriod.CLOSING
                continue
            if in_before_trading_time(now):
                self._time_period = TimePeriod.BEFORE_TRADING
                continue
            if in_after_trading(now):
                self._time_period = TimePeriod.AFTER_TRADING
                continue
            else:
                self._time_period = TimePeriod.CLOSING
                continue

    def events(self, start_date, end_date, frequency):

        while datetime.now().date() < start_date - timedelta(days=1):
            continue

        mark_time_thread = Thread(target=self.mark_time_period, args=(start_date, date.fromtimestamp(2147483647)))
        mark_time_thread.setDaemon(True)
        mark_time_thread.start()
        while True:
            if self._time_period == TimePeriod.BEFORE_TRADING:
                if self._after_trading_processed:
                    self._after_trading_processed = False
                if not self._before_trading_processed:
                    system_log.debug("FUTUEventForRealtime: before trading event")
                    yield Event(EVENT.BEFORE_TRADING, calendar_dt=datetime.now(), trading_dt=datetime.now())
                    self._before_trading_processed = True
                    continue
                else:
                    sleep(0.01)
                    continue
            elif self._time_period == TimePeriod.TRADING:
                now_dt = datetime.now()
                if not self._before_trading_processed:
                    yield Event(EVENT.BEFORE_TRADING, calendar_dt=now_dt, trading_dt=now_dt)
                    self._before_trading_processed = True
                    continue
                else:
                    fire_bar = False
                    if self._last_onbar_dt == None or (now_dt > (self._last_onbar_dt + self._fps_delta_dt)):
                        self._last_onbar_dt = now_dt
                        fire_bar = True
                    if fire_bar:
                        system_log.debug("FUTUEventForRealtime: BAR event")
                        yield Event(EVENT.BAR, calendar_dt=now_dt, trading_dt=now_dt)
                    else:
                        sleep(0.01)
                    continue
            elif self._time_period == TimePeriod.AFTER_TRADING:
                if self._before_trading_processed:
                    self._before_trading_processed = False
                if not self._after_trading_processed:
                    system_log.debug("FUTUEventForRealtime: after trading event")
                    yield Event(EVENT.AFTER_TRADING, calendar_dt=datetime.now(), trading_dt=datetime.now())
                    self._after_trading_processed = True
                else:
                    sleep(0.01)
                    continue
