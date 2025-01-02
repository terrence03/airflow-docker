# %%
import logging
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd

URL = "https://www2.moeaea.gov.tw/oil111/Dealer/GasStations/load"
now = datetime.now()
# save_folder = Path("opt/ariflow/downloads/gas_station_namelist")
save_folder = Path(R"plugins/gas_station")
log_file = save_folder / "update.log"


class Log:
    def __init__(self, log_file):
        self.log_file = log_file
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """Set up the logger"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def write_update_info_to_logger(self, update_date: str, update_status: str):
        """Test the logger"""
        logger = self.logger
        update_info = f"{update_date} - {update_status}"
        logger.info(update_info)

    def get_last_update_date(self):
        """Get the last update date"""
        with self.log_file.open() as _:
            if not _.readlines():
                return ""
            _.seek(0)
            last_line = _.readlines()[-1]
            last_update_date = last_line.split(" - ")[1]
            return str(last_update_date)


def get_data() -> tuple[str, pd.DataFrame]:
    """
    Get the gas station data

    Returns
    -------
    dict
        The gas station data
    """
    response = requests.post(
        URL,
        headers={"User-Agent": "Mozilla/5.0"},
        data={"city": "all"},
    )
    data = response.json()
    data = data["data"]
    update_time = data["UpdateTime"]
    stations = data["stations"]
    stations = pd.DataFrame(stations)
    return update_time, stations


def main():
    log = Log(log_file)
    update_time, stations = get_data()
    last_update_date = log.get_last_update_date()
    if update_time != last_update_date:
        log.write_update_info_to_logger(update_time, "update")
        stations.to_csv(
            save_folder / f"{update_time.replace("/","-")}_全國加油站名單.csv",
            index=False,
        )
        return True
    else:
        log.write_update_info_to_logger(update_time, "no update")
    return False


# %%
