# -*- coding: utf-8 -*-

from rqalpha import run_file


config = {
  "base": {
    "start_date": "2017-06-01",
    "end_date": "2017-06-13",
    "securities": ['stock'],
    "accounts": {
      "stock": 100000
    },
    "benchmark": "HK.00700",
    "frequency": "1d",
    # 运行类型，`b` 为历史数据回测，`p` 为实时数据模拟交易, `r` 为实时数据实盘交易。
    "run_type":  "b",
  },
  "extra": {
    "log_level": "verbose",
  },
  "mod": {
    "sys_analyser": {
      "enabled": True,
      "plot": True
    },
    "sys_simulation":
    {
      'enabled': False,
    },
    "sys_stock_realtime":
    {
      'enabled': False,
    },
  }
}

strategy_file_path = "./debug_strategy.py"

run_file(strategy_file_path, config)
