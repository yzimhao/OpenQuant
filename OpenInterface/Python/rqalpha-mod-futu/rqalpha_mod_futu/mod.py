from rqalpha.interface import AbstractMod


class FUTUMod(AbstractMod):
    def __init__(self):
        self._env = None

    def start_up(self, env, mod_config):
        self._env = env

        #需要在用户的策略脚本中配置不加载mod_sys_simulation
        if self._env.config.mod.sys_simulation.enabled != False or self._env.broker != None or self._env.event_source != None:
            raise RuntimeError("请在脚本中配置config: 'mod':'sys_simulation':{'enabled': False,} ")

        from .rqalpha_simulate_broker import RQSimulateBroker
        from .futu_event_source import FUTUEventSource

        ip = mod_config.api_svr.ip
        port = mod_config.api_svr.port
        per = mod_config.rqalpha_broker_config.volume_percent

        #替换默认Broker
        config_RQBroker = mod_config.rqalpha_broker_config
        RQBroker = RQSimulateBroker(self._env, config_RQBroker)
        self._env.set_broker(RQBroker)

        #替换默认EventSource
        event_source = FUTUEventSource(env, env.config.base.account_list)
        env.set_event_source(event_source)

        print(">>> FUTUMod.start_up")
        pass

    def tear_down(self, success, exception=None):
        print(">>> FUTUMod.tear_down")
        pass
