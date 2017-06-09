#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
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

from rqalpha.interface import AbstractDataSource
from rqalpha.environment import Environment
from rqalpha.model.instrument import Instrument
from .futu_utils import *
import pandas as pd
from datetime import date
import datetime
import time
import six
from rqalpha.events import EVENT


class FUTUDataSource(AbstractDataSource):
    def __init__(self, env, quote_context, data_cache):
        self._env = env
        self._quote_context = quote_context
        self._quote_context.subscribe(stock_code=self._env.config.base.benchmark, data_type='K_DAY', push=False)
        self._cache = data_cache._cache

    def get_all_instruments(self):
        """
        获取所有Instrument。

        :return: list[:class:`~Instrument`]
        """
        if IsFutuMarket_HKStock() is True:
            if self._cache['basicinfo_hk'] is None:
                ret_code, ret_data_cs = self._quote_context.get_stock_basicinfo(market="HK", stock_type="STOCK")
                if ret_code == -1 or ret_data_cs is None:
                    for i in range(3):
                        ret_code, ret_data_cs = self._quote_context.get_stock_basicinfo(market="HK", stock_type="STOCK")
                        if ret_code != -1 and ret_code is not None:
                            return ret_code, ret_data_cs
                        else:
                            time.sleep(0.1)
                ret_data_cs.at[ret_data_cs.index, 'stock_type'] = 'CS'

                ret_code, ret_data_idx = self._quote_context.get_stock_basicinfo("HK", "IDX")
                if ret_code == -1 or ret_data_idx is None:
                    for i in range(3):
                        ret_code, ret_data_idx = self._quote_context.get_stock_basicinfo("HK", "IDX")
                        if ret_code != -1 and ret_code is not None:
                            return ret_code, ret_data_idx
                        else:
                            time.sleep(0.1)
                ret_data_idx.at[ret_data_idx.index, 'stock_type'] = 'INDX'

                ret_code, ret_data_etf = self._quote_context.get_stock_basicinfo("HK", "ETF")
                if ret_code == -1 or ret_data_etf is None:
                    for i in range(3):
                        ret_code, ret_data_etf = self._quote_context.get_stock_basicinfo("HK", "ETF")
                        if ret_code != -1 and ret_code is not None:
                            return ret_code, ret_data_etf
                        else:
                            time.sleep(0.1)

                ret_code, ret_data_war = self._quote_context.get_stock_basicinfo("HK", "WARRANT")
                if ret_code == -1 or ret_data_war is None:
                    for i in range(3):
                        ret_code, ret_data_war = self._quote_context.get_stock_basicinfo("HK", "WARRANT")
                        if ret_code != -1 and ret_code is not None:
                            return ret_code, ret_data_war
                        else:
                            time.sleep(0.1)

                ret_code, ret_data_bond = self._quote_context.get_stock_basicinfo("HK", "BOND")
                if ret_code == -1 or ret_data_bond is None:
                    for i in range(3):
                        ret_code, ret_data_bond = self._quote_context.get_stock_basicinfo("HK", "BOND")
                        if ret_code != -1 and ret_code is not None:
                            return ret_code, ret_data_bond
                        else:
                            time.sleep(0.1)

                frames = [ret_data_cs, ret_data_idx, ret_data_etf, ret_data_war, ret_data_bond]
                ret_data = pd.concat(frames).reset_index(drop=True)
                self._cache['basicinfo_hk'] = ret_data
            else:
                ret_code, ret_data = 0, self._cache['basicinfo_hk']

        elif IsFutuMarket_USStock() is True:
            if self._cache['basicinfo_us'] is None:
                ret_code, ret_data_cs = self._quote_context.get_stock_basicinfo(market="US", stock_type="STOCK")
                if ret_code == -1 or ret_data_cs is None:
                    for i in range(3):
                        ret_code, ret_data_cs = self._quote_context.get_stock_basicinfo(market="US", stock_type="STOCK")
                        if ret_code != -1 and ret_code is not None:
                            return ret_code, ret_data_cs
                        else:
                            time.sleep(0.1)
                ret_data_cs.at[ret_data_cs.index, 'stock_type'] = 'CS'

                ret_code, ret_data_idx = self._quote_context.get_stock_basicinfo(market="US", stock_type="IDX")
                if ret_code == -1 or ret_data_idx is None:
                    for i in range(3):
                        ret_code, ret_data_idx = self._quote_context.get_stock_basicinfo("US", "IDX")
                        if ret_code != -1 and ret_code is not None:
                            return ret_code, ret_data_idx
                        else:
                            time.sleep(0.1)
                ret_data_idx.at[ret_data_idx.index, 'stock_type'] = 'INDX'

                ret_code, ret_data_etf = self._quote_context.get_stock_basicinfo(market="US", stock_type="ETF")
                if ret_code == -1 or ret_data_etf is None:
                    for i in range(3):
                        ret_code, ret_data_etf = self._quote_context.get_stock_basicinfo("US", "ETF")
                        if ret_code != -1 and ret_code is not None:
                            return ret_code, ret_data_etf
                        else:
                            time.sleep(0.1)

                frames = [ret_data_cs, ret_data_idx, ret_data_etf]
                ret_data = pd.concat(frames).reset_index(drop=True)
            else:
                ret_code, ret_data = 0, self._cache['basic_info_us']

        if ret_code == -1 or ret_data is None:
            raise NotImplementedError

        del ret_data['stock_child_type'], ret_data['owner_stock_code']  # 删除多余的列
        ret_data.reset_index(drop=True)

        ret_data['de_listed_date'] = str("2999-12-31")  # 增加一列退市日期

        ret_data.rename(
            columns={'code': 'order_book_id', 'name': 'symbol', 'stock_type': 'type', 'listing_date': 'listed_date', 'lot_size': 'round_lot'},
            inplace=True)  # 修改列名

        stock_basicinfo = ret_data.to_dict(orient='records')    # 转置并转为字典格式
        all_instruments = [Instrument(i) for i in stock_basicinfo]
        return all_instruments

    def get_bar(self, instrument, dt, frequency):
        """
        根据 dt 来获取对应的 Bar 数据 ---待实现 相当于获取K线 先从历史数据找（需要历史更新程序并且全量下载了），找到返回，
        没找到再从当前(获取指定时间的bar)

        :param instrument: 合约对象
        :type instrument: :class:`~Instrument`

        :param datetime.datetime dt: calendar_datetime

        :param str frequency: 周期频率，`1d` 表示日周期, `1m` 表示分钟周期

        :return: `numpy.ndarray` | `dict`
        """
        if frequency != '1d':
            raise NotImplementedError
        if dt is None:
            dt = datetime.now().date()

        self.is_today(dt)  # 如果是当前时间，就清缓存

        current = date.today()
        current_time = str(current).replace('-', '')
        dt_time = str(dt.date()).replace('-', '')
        base = Environment.get_instance().config.base

        if dt_time == current_time:  # 判断时间是否是当天，注意格式转换
            if self._cache['cur_kline'] is None:  #判断条件应该是当前日期不在缓存里
                ret_code, bar_data = self._quote_context.get_cur_kline(instrument.order_book_id, num=10, ktype='K_DAY')
                if ret_code == -1 or bar_data is None:
                    for i in range(3):
                        ret_code, bar_data = self._quote_context.get_cur_kline(instrument.order_book_id, num=10,
                                                                               ktype='K_DAY')
                        if ret_code != -1 and bar_data is not None:
                            return ret_code, bar_data
                        else:
                            time.sleep(0.1)
                    self._cache['cur_kline'] = bar_data
            else:
                ret_code, bar_data = 0, self._cache['cur_kline']

        elif dt_time < current_time:
            if self._cache['history_kline'] is None:   #判断条件应该是当前日期不在缓存里
                ret_code, bar_data = self._quote_context.get_history_kline(instrument.order_book_id,
                                                                           start=base.start_date.strftime('%Y-%m-%d'),
                                                                           end=dt.strftime('%Y-%m-%d'), ktype='K_DAY')
                if ret_code == -1 or bar_data is None:
                    for i in range(3):
                        ret_code, bar_data = self._quote_context.get_history_kline(instrument.order_book_id,
                                                                                   start=base.start_date.strftime(
                                                                                       '%Y-%m-%d'),
                                                                                   end=dt.strftime('%Y-%m-%d'),
                                                                                   ktype='K_DAY')
                        if ret_code != -1 and bar_data is not None:
                            return ret_code, bar_data
                        else:
                            time.sleep(0.1)
                    self._cache['history_kline'] = bar_data
            else:
                ret_code, bar_data = 0, self._cache['history_kline']

        elif dt_time > current_time:
            if self._cache['history'] is None:  #判断条件应该是当前日期不在缓存里
                ret_code, bar_data = self._quote_context.get_history_kline(instrument.order_book_id,
                                                                           start=base.start_date.strftime('%Y-%m-%d'),
                                                                           end=current.strftime('%Y-%m-%d'),
                                                                           ktype='K_DAY')
                if ret_code == -1 or bar_data is None:
                    for i in range(3):
                        ret_code, bar_data = self._quote_context.get_history_kline(instrument.order_book_id,
                                                                                   start=base.start_date.strftime(
                                                                                       '%Y-%m-%d'),
                                                                                   end=current.strftime('%Y-%m-%d'),
                                                                                   ktype='K_DAY')
                        if ret_code != -1 and bar_data is not None:
                            return ret_code, bar_data
                        else:
                            time.sleep(0.1)
                    self._cache['history_kline'] = bar_data
            else:
                ret_code, bar_data = 0, self._cache['history_kline']

        if ret_code == -1 or bar_data is None:
            raise NotImplementedError

        del bar_data['code']  # 去掉code

        for i in range(len(bar_data['time_key'])):  # 时间转换
            bar_data.loc[i, 'time_key'] = int(
                bar_data['time_key'][i].replace('-', '').replace(' ', '').replace(':', ''))

        bar_data.rename(columns={'time_key': 'datetime', 'turnover': 'total_turnover'}, inplace=True)  # 将字段名称改为一致的

        ret_dict = bar_data[bar_data.datetime <= int(dt_time + "000000")].iloc[-1].to_dict()

        return ret_dict

    def history_bars(self, instrument, bar_count, frequency, fields, dt, skip_suspended=True,
                     include_now=False, adjust_type='pre', adjust_orig=None):
        """
        获取历史数据

        :param instrument: 合约对象
        :type instrument: :class:`~Instrument`

        :param int bar_count: 获取的历史数据数量
        :param str frequency: 周期频率，`1d` 表示日周期, `1m` 表示分钟周期
        :param str fields: 返回数据字段

        =========================   ===================================================
        fields                      字段名
        =========================   ===================================================
        datetime                    时间戳
        open                        开盘价
        high                        最高价
        low                         最低价
        close                       收盘价
        volume                      成交量
        total_turnover              成交额
        datetime                    int类型时间戳
        open_interest               持仓量（期货专用）
        basis_spread                期现差（股指期货专用）
        settlement                  结算价（期货日线专用）
        prev_settlement             结算价（期货日线专用）
        =========================   ===================================================

        :param datetime.datetime dt: 时间
        :param bool skip_suspended: 是否跳过停牌日
        :param bool include_now: 是否包含当天最新数据
        :param str adjust_type: 复权类型，'pre', 'none', 'post'
        :param datetime.datetime adjust_orig: 复权起点；

        :return: `numpy.ndarray`

        """
        if frequency != '1d' or not skip_suspended:
            raise NotImplementedError

        self.is_today(dt)

        start_dt_loc = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = start_dt_loc.strftime("%Y-%m-%d").replace('-', '').replace(' ', '').replace(':', '')   # 开始时间 字符串2017-06-01
        start_dt = int(start_dt) - bar_count + 1

        if self._cache['history'] is None:  # 判断条件应该是当前日期不在缓存里
            ret_code, bar_data = self._quote_context.get_history_kline(instrument.order_book_id, start=start_dt,
                                                                       end=dt.strftime('%Y-%m-%d'), ktype='K_DAY')
            if ret_code == -1 or bar_data is None:
                for i in range(3):
                    ret_code, bar_data = self._quote_context.get_history_kline(instrument.order_book_id, start=start_dt,
                                                                               end=dt.strftime('%Y-%m-%d'),
                                                                               ktype='K_DAY')
                    if ret_code != -1 and bar_data is not None:
                        return ret_code, bar_data
                    else:
                        time.sleep(0.1)
        else:
            ret_code, bar_data = 0, self._cache['history_kline']

        if ret_code == -1 or bar_data is None:
            raise NotImplementedError
        else:
            if isinstance(fields, six.string_types):
                fields = [fields]

            del bar_data['code']   # 去掉code

            for i in range(len(bar_data['time_key'])):  # 时间转换
                bar_data.loc[i, 'time_key'] = int(bar_data['time_key'][i].replace('-', '').replace(' ', '').replace(':', ''))

            bar_data.rename(columns={'time_key': 'datetime', 'turnover': 'total_turnover'}, inplace=True)  # 将字段名称改为一致的

            fields = [field for field in fields if field in bar_data.columns]

            return bar_data[fields].as_matrix()

    def get_trading_calendar(self):
        """
        获取交易日历 ---看支持的交易日级别 ---这里还需要思考下 如果end_date超过现在怎么办，
        rqalpha里这个数从本地数据里读取的就肯定不会超，感觉这里有点问题，API的接口是以后的结束日期也能读取的到

        :return:
        """
        base = self._env.config.base
        if self._cache["trading_days"] is None:   #base里的日期不在列表里
            ret_code, calendar_list = self._quote_context.get_trading_days(market="HK",
                                                                           start_date=base.start_date.strftime(
                                                                               "%Y-%m-%d"),
                                                                           end_date=base.end_date.strftime("%Y-%m-%d"))
            if ret_code == -1 or calendar_list is None:
                for i in range(3):
                    ret_code, calendar_list = self._quote_context.get_trading_days(market="HK",
                                                                                   start_date=base.start_date.strftime(
                                                                                       "%Y-%m-%d"),
                                                                                   end_date=base.end_date.strftime(
                                                                                       "%Y-%m-%d"))
                    if ret_code != -1 and calendar_list is not None:
                        return ret_code, calendar_list
                    else:
                        time.sleep(0.1)
        else:
            ret_code, calendar_list = 0, self._cache["trading_days"]

        if ret_code == -1 or calendar_list is None:
            raise NotImplementedError
        calendar = pd.Index(pd.Timestamp(str(d)) for d in calendar_list)
        return calendar[::-1]

    def current_snapshot(self, instrument, frequency, dt):
        """
        获得当前市场快照数据。只能在日内交易阶段调用，获取当日调用时点的市场快照数据。  ---控制频率和数量 5s 200支(日K不用实现)
        市场快照数据记录了每日从开盘到当前的数据信息，可以理解为一个动态的day bar数据。
        在目前分钟回测中，快照数据为当日所有分钟线累积而成，一般情况下，最后一个分钟线获取到的快照数据应当与当日的日线行情保持一致。
        需要注意，在实盘模拟中，该函数返回的是调用当时的市场快照情况，所以在同一个handle_bar中不同时点调用可能返回的数据不同。
        如果当日截止到调用时候对应股票没有任何成交，那么snapshot中的close, high, low, last几个价格水平都将以0表示。

        :param instrument: 合约对象
        :type instrument: :class:`~Instrument`

        :param str frequency: 周期频率，`1d` 表示日周期, `1m` 表示分钟周期
        :param datetime.datetime dt: 时间

        :return: :class:`~Snapshot`
        """
        raise NotImplementedError

    def available_data_range(self, frequency):
        """
        此数据源能提供数据的时间范围 ---2000-2952  rqalpha是基于历史数据得到的，这个函数还用来在run里调整配置里的原始时间
        调整时间是用这里返回的end 和配置中做比较取得较小的---问题在于run是回测运行一次，实盘也有这个函数，返回max

       :param str frequency: 周期频率，`1d` 表示日周期, `1m` 表示分钟周期

        :return: (earliest, latest)
        """
        s = date(2000, 1, 1)
        # e = date.fromtimestamp(30999999999)
        e = date.today()
        return s, e

    def is_suspended(self, order_book_id, dates):
        #  用市场快照 判断一只股票是否停牌
        if IsRuntype_Backtest() is True:   # 回测
            return [(False) for d in dates]
        elif IsRuntype_RealTrade() is True:  # 实盘
            if self._cache["market_snapshot"] is None:  #日期不在里面
                result = []
                for i in range(len(dates)):
                    date_time = dates[i].strftime("%Y-%m-%d")
                    ret_code, ret_data = self._quote_context.get_market_snapshot([order_book_id])
                    if len(dates) != 1:
                        time.sleep(5)
                    if ret_code == -1 or ret_data is None:
                        for j in range(3):
                            ret_code, ret_data = self._quote_context.get_market_snapshot([order_book_id])
                            if ret_code != -1 and ret_data is not None:
                                return ret_code, ret_data
                            else:
                                time.sleep(5)
            else:
                ret_code, ret_data = 0, self._cache["market_snapshot"]

            if ret_data is not None and date_time in str(ret_data['update_time'])[5:15]:
                if str(ret_data['suspension'])[5:10] == 'False':
                    result.append(False)
                elif str(ret_data['suspension'])[5:10] == 'True':
                    result.append(True)

            if ret_data is None or date_time not in ret_data['update_time'][5:15]:
                result.append(True)
        return result

    def is_today(self, dt):
        if dt is None:
            dt = datetime.now().date()
        dt_time = str(dt.date()).replace('-', '')
        current = date.today()
        current_time = str(current).replace('-', '')
        if current_time == dt_time:
            self._cache.remove_all()

    def register_event(self, event):
        event_bus = Environment.get_instance().event_bus
        event_bus.add_listener(EVENT.PRE_BEFORE_TRADING, self.get_all_instruments())

    def get_trading_minutes_for(self, order_book_id, trading_dt):
        """
        获取证券某天的交易时段，用于期货回测---不实现
        :param order_book_id:
        :param trading_dt:
        :return:
        """
        raise NotImplementedError

    def get_yield_curve(self, start_date, end_date, tenor=None):
        """
        获取国债利率---不实现

        :param pandas.Timestamp str start_date: 开始日期
        :param pandas.Timestamp end_date: 结束日期
        :param str tenor: 利率期限

        :return: pandas.DataFrame, [start_date, end_date]
        """
        return None

    def get_dividend(self, order_book_id):
        """
        获取股票/基金分红信息---不实现

        :param str order_book_id: 合约名
        :return:
        """
        return None

    def get_split(self, order_book_id):
        """
        获取拆股信息---不实现

        :param str order_book_id: 合约名

        :return: `pandas.DataFrame`
        """

        return None

    def get_settle_price(self, instrument, date):
        """
        获取期货品种在 date 的结算价---期货日线专用---不实现
        :param instrument: 合约对象
        :type instrument: :class:`~Instrument`

        :param datetime.date date: 结算日期

        :return: `str`
        """
        raise NotImplementedError

    def get_margin_info(self, instrument):
        """
        获取合约的保证金数据---margin_rate 是期货合约最低保证金率---不实现

        :param instrument: 合约对象
        :return: dict
        """
        raise NotImplementedError

    def get_commission_info(self, instrument):
        """
        获取合约的手续费信息---期货的类型---万分之五(针对期货的，故不实现)
        :param instrument:
        :return:
        """
        raise NotImplementedError

    def get_merge_ticks(self, order_book_id_list, trading_date, last_dt=None):
        """
        获取合并的 ticks---不支持逐笔---不实现

        :param list order_book_id_list: 合约名列表
        :param datetime.date trading_date: 交易日
        :param datetime.datetime last_dt: 仅返回 last_dt 之后的时间

        :return: Tick
        """
        raise NotImplementedError


class DataCache:
    def __init__(self):
        self._cache = {}
        self._cache["basicinfo_hk"] = None
        self._cache["basicinfo_us"] = None
        self._cache["cur_kline"] = None
        self._cache["history_kline"] = None
        self._cache["trading_days"] = None
        self._cache["market_snapshot"] = None

    def __contains__(self, key):
        """
        根据该键判断是否存在缓存中
        """
        return key in self._cache

    def is_empty(self, key):
        """
        判断某个键的对应的值是否为空
        """
        return self._cache[key] is None

    def get(self, key):
        return self._cache[key]

    def remove_all(self):
        """
        删除全部
        :return:
        """
        for key in self._cache:
            self._cache[key] = None

