from dxlib.module_proxy import ModuleProxy
from .yfinance import YFinance

# yfinance := [[yfinance.py]]
yfinance = ModuleProxy("dxlib.interfaces.external.yfinance.yfinance")
