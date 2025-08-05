from typing import TYPE_CHECKING

from dxlib.interfaces import MarketInterface
from dxlib.module_proxy import ModuleProxy

# twelvedata := [[twelvedata.py]]
twelvedata = ModuleProxy("dxlib.interfaces.external.twelvedata.twelvedata")

if TYPE_CHECKING:
    from dxlib.interfaces.external.twelvedata.twelvedata import TwelveData
else:
    TwelveData = twelvedata[MarketInterface]("TwelveData")
