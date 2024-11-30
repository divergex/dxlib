from dxlib.interfaces import MarketInterface
from dxlib.module_proxy import ModuleProxy

cboe = ModuleProxy("dxlib.interfaces.external.cboe.cboe")
Cboe = cboe[MarketInterface]("Cboe")
