from dxlib.interfaces import MarketInterface
from dxlib.module_proxy import ModuleProxy

investing_com = ModuleProxy("dxlib.interfaces.external.investing_com.investing_com")
InvestingCom = investing_com[MarketInterface]("InvestingCom")
