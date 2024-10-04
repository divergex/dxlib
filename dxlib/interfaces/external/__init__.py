from ..interface import Interface

from dxlib.module_proxy import ModuleProxy

investing_com = ModuleProxy("dxlib.interfaces.external.investing_com")
InvestingCom = investing_com[Interface]("InvestingCom")
