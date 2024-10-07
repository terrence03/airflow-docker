# %%
import sys
from pathlib import Path
from datetime import timedelta
import pandas as pd

sys.path.append(R"D:\OneDrive\WORK\Projects\airflow-docker")
from plugins.oil_price.cpc_fpcc import CpcPrice, FpccPrice
from plugins.oil_price.moea import CrudeOilPrice, AvgPrice, TownPrice, RefPrice
from plugins.tools import sqlite_tools
from plugins.tools.period_config import Week


# CrudeOilPrice().get_weekly_data()
# AvgPrice().get_weekly_data()
TownPrice().get_weekly_data(1292)
# RefPrice().get_weekly_data()
# %%
Week().get_week_id()
# %%
