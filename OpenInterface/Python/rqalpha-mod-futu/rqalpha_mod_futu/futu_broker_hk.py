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

from rqalpha.interface import AbstractBroker
from rqalpha.environment import Environment
from rqalpha.const import ACCOUNT_TYPE
from rqalpha.const import MATCHING_TYPE
from rqalpha.mod.rqalpha_mod_sys_simulation.simulation_broker import SimulationBroker
from rqalpha.mod.rqalpha_mod_sys_simulation.utils import init_portfolio
import six

class FUTUBrokerHK(AbstractBroker):
    def __init__(self, env, mod_config):
        self._env = env
        self._mod_config = mod_config

    def get_portfolio(self):
        """
        [Required]

        获取投资组合。系统初始化时，会调用此接口，获取包含账户信息、净值、份额等内容的投资组合

        :return: Portfolio
        """
        return init_portfolio(self._env)

    def submit_order(self, order):
        """
        [Required]

        提交订单。在当前版本，RQAlpha 会生成 :class:`~Order` 对象，再通过此接口提交到 Broker。
        TBD: 由 Broker 对象生成 Order 并返回？
        """
        print("FUTUBrokerHK : submit_order ")
        #raise NotImplementedError

    def cancel_order(self, order):
        """
        [Required]

        撤单。

        :param order: 订单
        :type order: :class:`~Order`
        """
        raise NotImplementedError

    def get_open_orders(self, order_book_id=None):
        """
        [Required]

        获得当前未完成的订单。

        :return: list[:class:`~Order`]
        """
        raise NotImplementedError




