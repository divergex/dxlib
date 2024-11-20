from dxlib.module_proxy import ModuleProxy
from dxlib.interfaces.interface import Interface

ibkr = ModuleProxy("dxlib.interfaces.external.ibkr.ibkr")
Ibkr = ibkr[Interface]("Ibkr")
