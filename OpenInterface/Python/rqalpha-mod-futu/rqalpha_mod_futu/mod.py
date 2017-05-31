from rqalpha.interface import AbstractMod
from .futu_utils import *
from .rqalpha_simulate_broker import RQSimulateBroker
from .futu_event_source import *
from .futu_broker_hk import FUTUBrokerHK
from .futu_market_state import FUTUMarketStateSource

class FUTUMod(AbstractMod):
    _futu_mod = None
    def __init__(self):
        FUTUMod._futu_mod = self
        self._env = None
        self._mod_config = None
        self._quote_context = None
        self._market_state_source = None

    @classmethod
    def get_instance(cls):
        return FUTUMod._futu_mod

    def start_up(self, env, mod_config):
        self._env = env
        self._mod_config = mod_config

        #需要在用户的策略脚本中配置不加载mod_sys_simulation
        if self._env.config.mod.sys_simulation.enabled != False or self._env.broker != None or self._env.event_source != None:
            raise RuntimeError("请在策略脚本中增加config, {'mod':'sys_simulation':{'enabled': False,} } ")

        #检查市场配置参数: 一个策略脚本只针对一个市场
        CheckFutuMarketConfig()

        #runtype有三种 ： 回测、实盘交易、仿真交易
        #futu api对接，只能支持港美股的实盘和港股的仿真
        CheckRunTypeConfig()

        #初始化api行情对象
        self._init_quote_context()
        self._market_state_source = FUTUMarketStateSource(self._env, self._quote_context)

        #替换关键组件
        self._set_broker()
        self._set_data_source()
        self._set_event_source()
        print(">>> FUTUMod.start_up")

    def tear_down(self, success, exception=None):
        print(">>> FUTUMod.tear_down")
        pass

    def _set_broker(self):
        if IsRuntype_Backtest():
            config_broker = mod_config.rqalpha_broker_config
            self._env.set_broker(RQSimulateBroker(self._env, config_broker))
        elif IsRuntype_RealtimeStrategy():
            if IsFutuMarket_HKStock():  # 港股实时策略
                broker = FUTUBrokerHK(self._env, self._mod_config)
                self._env.set_broker(broker)
            elif IsFutuMarket_USStock():  # 美股实时策略
                raise RuntimeError("_set_broker no impl")
        else:
            raise RuntimeError("_set_broker err param")

    def _set_event_source(self):
        if IsRuntype_Backtest():
            event_source = FUTUEventForBacktest(self._env, self._env.config.base.account_list)
            self._env.set_event_source(event_source)
        elif IsRuntype_RealtimeStrategy():
            event_source = FUTUEventForRealtime(self._env, self._mod_config, self._market_state_source)
            self._env.set_event_source(event_source)
        else:
            raise RuntimeError("_set_event_source err param")

    def _set_data_source(self):
        pass

    def _init_quote_context(self):
        self._mod_config.api_svr.ip = '127.0.0.1' #119.29.141.202'
        self._mod_config.api_svr.port = 11111
        self._quote_context = OpenQuoteContext(str(self._mod_config.api_svr.ip), int(self._mod_config.api_svr.port))

