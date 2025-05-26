import pandas as pd
from src.oil_price.cpc_fpcc import CpcPrice, FpccPrice

cpc_price = CpcPrice()
fpcc_price = FpccPrice()

cpc_board_price = pd.DataFrame(cpc_price.get_board_price())
print("cpc_board_price test success")

cpc_asia_price = pd.DataFrame(cpc_price.get_asia_price())
print("cpc_asia_price test success")

cpc_price_adj = pd.DataFrame(cpc_price.get_price_adj())
print("cpc_price_adj test success")

cpc_price_compare = pd.DataFrame(cpc_price.get_price_compare())
print("cpc_price_compare test success")

fpcc_board_price = pd.DataFrame(fpcc_price.get_board_price())
print("fpcc_board_price test success")
