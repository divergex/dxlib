from dxlib.module_proxy import ModuleProxy
from dxlib.interfaces.interface import Interface

investing_com = ModuleProxy("dxlib.interfaces.external.investing_com.investing_com")
InvestingCom = investing_com[Interface]("InvestingCom")
