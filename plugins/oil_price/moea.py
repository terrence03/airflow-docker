import sys
import json
from datetime import datetime, timedelta
import requests
import pandas as pd

sys.path.append("/opt/airflow")
from plugins.tools.period_config import Week, Month


class CrudeOilPrice:
    def __init__(self):
        self.url = "https://www2.moeaea.gov.tw/oil111/CrudeOil/Price"
        self.url_post = "https://www2.moeaea.gov.tw/oil111/CrudeOil/GetRangePrice"  # uesd to get the data

    def __payload(self, unit: str, start: str | int, end: str | int):
        """payload

        Parameters
        ----------
        unit : str or int
            "day", "week", "month"
        start : str or int
            YYYY/MM/DD, week_id, YYYY/MM, depending on the unit
        end : str
            YYYY/MM/DD, week_id, YYYY/MM, depending on the unit

        Returns
        -------
        payload : dict
            payload

        Examples
        --------
        payload("day", "2015/01/01", "2023/12/31")
        >>> {"unit": "day", "start": "2015/01/01", "end": "2023/12/31"}

        payload("week", "2015/01", "2023/12")
        >>> {"unit": "week", "start": 1250, "end": 1252}

        payload("month", "2015/01", "2023/12")
        >>> {"unit": "month", "start": "2015/01", "end": "2023/12"}
        """
        payload = {"unit": unit, "start": start, "end": end}
        return payload

    def __get_data(self, unit: str, start: str, end: str) -> pd.DataFrame:
        res = requests.post(
            self.url_post,
            data=self.__payload(unit=unit, start=start, end=end),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        res = json.loads(res.text)
        if res["res"] != "00":
            _data = res["data"]["crudeoil"]
            _data.reverse()
            _data = pd.DataFrame(_data)
            _data.rename(
                columns={
                    "WestT": "wti",
                    "Burant": "brent",
                    "Dubit": "dubai",
                    "Trade": "trade",
                },
                inplace=True,
            )
            return _data
        else:
            return None

    def get_daily_data(self, start_date: str = None, end_date: str = None):
        if all([start_date, end_date]):
            _data = self.__get_data(unit="day", start=start_date, end=end_date)
        elif start_date:
            _data = self.__get_data(unit="day", start=start_date, end=start_date)
        else:
            today = datetime.today().date()
            yesterday = today - timedelta(days=1)
            _data = self.__get_data(
                unit="day",
                start=yesterday.strftime("%Y/%m/%d"),  # 資料有1天的延遲
                end=yesterday.strftime("%Y/%m/%d"),
            )

        if _data is not None:
            _data.insert(0, "date", _data["SurDate"].map(lambda x: x.replace("/", "-")))
            _data.drop(columns=["SurDate"], inplace=True)
            return _data
        else:
            print("No data")
            return None

    def get_weekly_data(
        self, start_week_id: int = None, end_week_id: int = None
    ) -> pd.DataFrame:
        if all([start_week_id, end_week_id]):
            _data = self.__get_data(unit="week", start=start_week_id, end=end_week_id)
            week_id = range(start_week_id, end_week_id + 1)
        elif start_week_id:
            _data = self.__get_data(unit="week", start=start_week_id, end=start_week_id)
            week_id = [start_week_id]
        else:
            week = Week()
            start_week_id = end_week_id = week.get_week_id() - 1
            _data = self.__get_data(unit="week", start=start_week_id, end=end_week_id)
            week_id = [start_week_id]
        _data = _data.drop_duplicates(subset=["SurDate"], keep="first")
        _data["week_start"] = _data["SurDate"].map(
            lambda x: x.split(" ~ ")[0].replace("/", "-")
        )
        _data["week_end"] = _data["SurDate"].map(
            lambda x: x.split(" ~ ")[1].replace("/", "-")
        )
        _data["week_id"] = week_id
        _data.drop(columns=["SurDate"], inplace=True)
        return _data

    def get_monthly_data(
        self, start_year_month: str = None, end_year_month: str = None
    ) -> pd.DataFrame:
        if all([start_year_month, end_year_month]):
            _data = self.__get_data(
                unit="month", start=start_year_month, end=end_year_month
            )
        else:
            year, month = Month().last_month()
            _data = self.__get_data(
                unit="month", start=f"{year}/{month:02d}", end=f"{year}/{month:02d}"
            )
        _data["year"] = _data["SurDate"].map(lambda x: int(x.split("/")[0]))
        _data["month"] = _data["SurDate"].map(lambda x: int(x.split("/")[1]))
        _data.drop(columns=["SurDate"], inplace=True)
        return _data


class AvgPrice:
    def __init__(self):
        self.url = "https://www2.moeaea.gov.tw/oil111/Gasoline/NationwideAvg"
        self.url_post = "https://www2.moeaea.gov.tw/oil111/Gasoline/GetRangeNationwideAvg"  # uesd to get the data

    def __payload(self, unit: str, start: int | str, end: int | str):
        """payload

        Parameters
        ----------
        unit : str or int
            "week", "month", Future: "season", "year"
        start : int | str
            week_id, "YYYY/MM", depending on the unit, Future: "YYYY/season", "YYYY"
        end : int | str
            week_id, "YYYY/MM", depending on the unit, Future: "YYYY/season", "YYYY"

        Returns
        -------
        payload : dict
            payload

        Examples
        --------
        payload("week", "2015/01", "2023/12")
        >>> {"unit": "week", "start": 1250, "end": 1252}
        payload("month", "2015/01", "2023/12")
        >>> {"unit": "month", "start": "2015/01", "end": "2023/12"}
        """
        return {"unit": unit, "start": start, "end": end}

    def __get_data(self, unit: str, start=None, end=None) -> dict:
        res = requests.post(
            self.url_post,
            data=self.__payload(unit=unit, start=start, end=end),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        res = json.loads(res.text)
        return res

    def __process_weekly_data(self, response) -> pd.DataFrame:
        if response["res"] != "00":
            price = response["data"]["gasoline"]
            price.reverse()
            price = pd.DataFrame(price)
            price.rename(
                columns={
                    "Oil92": "gasoline_92",
                    "Oil95": "gasoline_95",
                    "Oil98": "gasoline_98",
                    "Oilchai": "diesel",
                },
                inplace=True,
            )
            price["week_start"] = price["SurDate"].map(
                lambda x: x.split(" ~ ")[0].replace("/", "-")
            )
            price["week_end"] = price["SurDate"].map(
                lambda x: x.split(" ~ ")[1].replace("/", "-")
            )

            start_week = int(response["data"]["startweek"])
            end_week = int(response["data"]["endweek"])
            price["week_id"] = range(start_week, end_week + 1)

            price = price[
                [
                    "week_id",
                    "week_start",
                    "week_end",
                    "gasoline_92",
                    "gasoline_95",
                    "gasoline_98",
                    "diesel",
                ]
            ]
            return price
        else:
            return None

    def __process_monthly_data(self, response) -> pd.DataFrame:
        if response["res"] != "00":
            price = response["data"]["gasoline"]
            price.reverse()
            price = pd.DataFrame(price)
            price.rename(
                columns={
                    "Oil92": "gasoline_92",
                    "Oil95": "gasoline_95",
                    "Oil98": "gasoline_98",
                    "Oilchai": "diesel",
                },
                inplace=True,
            )
            price["year"] = price["SurDate"].map(lambda x: int(x.split("/")[0]))
            price["month"] = price["SurDate"].map(lambda x: int(x.split("/")[1]))

            price = price[
                [
                    "year",
                    "month",
                    "gasoline_92",
                    "gasoline_95",
                    "gasoline_98",
                    "diesel",
                ]
            ]
            return price
        else:
            return None

    def get_weekly_data(
        self, start_week_id: int = None, end_week_id: int = None
    ) -> pd.DataFrame:
        if all([start_week_id, end_week_id]):
            response = self.__get_data(
                unit="week", start=start_week_id, end=end_week_id
            )
        elif start_week_id:
            response = self.__get_data(
                unit="week", start=start_week_id, end=start_week_id
            )
        else:
            week = Week()
            start_week_id = end_week_id = week.get_week_id() - 1  # 資料有1週的延遲
            response = self.__get_data(
                unit="week", start=start_week_id, end=end_week_id
            )
        data = self.__process_weekly_data(response)

        return data

    def get_monthly_data(
        self, start_year_month: str = None, end_year_month: str = None
    ) -> pd.DataFrame:
        """Get the monthly data

        Parameters
        ----------
        start : str
            "YYYY/MM"
        end : str
            "YYYY/MM"
        """
        if all([start_year_month, end_year_month]):
            response = self.__get_data(
                unit="month", start=start_year_month, end=end_year_month
            )
        elif start_year_month:
            response = self.__get_data(
                unit="month", start=start_year_month, end=start_year_month
            )
        else:
            year, month = Month().last_month()
            response = self.__get_data(
                unit="month", start=f"{year}/{month:02d}", end=f"{year}/{month:02d}"
            )
        data = self.__process_monthly_data(response)
        return data


class CountyPrice:
    def __init__(self):
        self.url = "https://www2.moeaea.gov.tw/oil111/Gasoline/CountyCityAvg"
        self.url_post = "https://www2.moeaea.gov.tw/oil111/Gasoline/CountyCityAvg/load"  # uesd to get the data
        self.oil_dict = {
            "gasoline_92": "01",
            "gasoline_95": "02",
            "gasoline_98": "03",
            "diesel": "04",
        }
        self.category_dict = {"A": "本島", "B": "離島", "C": "山地鄉"}

    def __payload(self, unit: str, oil: str, start: str):
        """payload

        Parameters
        ----------
        unit : str
            "month"
        oil : str
            ["gasoline_92", "gasoline_95", "gasoline_98", "diesel"]
        start : str
            YYYY/MM/DD

        Returns
        -------
        payload : dict
            payload

        Examples
        --------
        payload("month", "01", "2015/01/01")
        >>> {"unit": "month", "oil": "01", "start": "2015/01/01"}
        """
        assert oil in self.oil_dict.keys(), "Invalid oil type"
        payload = {"unit": unit, "oil": self.oil_dict[oil], "start": start}
        return payload

    def __get_data(self, unit: str, oil: str, start: str) -> pd.DataFrame:
        res = requests.post(
            self.url_post,
            data=self.__payload(unit, oil, start),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        res = json.loads(res.text)
        if res["res"] != "00":
            _data = res["data"]
            return _data
        else:
            return None

    def __process_data(self, data: dict, oil: str) -> pd.DataFrame:
        _data = data["gasoline"]
        _data = pd.DataFrame(_data)
        _data["year"] = _data["SurDate"].map(lambda x: int(x.split("/")[0]))
        _data["month"] = _data["SurDate"].map(lambda x: int(x.split("/")[1]))
        _data["oil"] = oil
        _data.rename(columns={"City": "county"}, inplace=True)

        result = pd.DataFrame()
        for i in ["A", "B", "C"]:  # 本島/離島/山地鄉
            _data_to_concat = _data[
                ["year", "month", "county", "oil", f"{i}avg", f"{i}high", f"{i}low"]
            ].copy()
            _data_to_concat.rename(
                columns={f"{i}avg": "avg", f"{i}high": "high", f"{i}low": "low"},
                inplace=True,
            )
            _data_to_concat["category"] = self.category_dict[i]
            result = pd.concat([result, _data_to_concat])
        result = result[
            [
                "year",
                "month",
                "county",
                "category",
                "oil",
                "avg",
                "high",
                "low",
            ]
        ]
        result.dropna(inplace=True)
        result.reset_index(drop=True, inplace=True)
        return result

    def get_monthly_data(self, year_month: str = None) -> pd.DataFrame:
        if year_month:
            start = year_month
        else:
            year, month = Month().last_month()
            start = f"{year}/{month:02d}"
        result = pd.DataFrame()
        for oil in self.oil_dict.keys():
            _data = self.__get_data(unit="month", oil=oil, start=start)
            if _data:
                _data = self.__process_data(_data, oil)
                result = pd.concat([result, _data])
        return result


class TownPrice:
    def __init__(self):
        self.url = "https://www2.moeaea.gov.tw/oil111/Gasoline/TownshipAvg/"
        self.url_post = "https://www2.moeaea.gov.tw/oil111/Gasoline/TownshipAvg/load"  # uesd to get the data

    def __payload(self, week_id: int):
        return {"start": week_id}

    def __get_data(self, week_id: int) -> dict:
        res = requests.post(
            self.url_post,
            data=self.__payload(week_id=week_id),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        res = json.loads(res.text)
        return res

    def __process_data(self, response) -> pd.DataFrame:
        if response["res"] != "00":
            price = response["data"]["gasoline"]
            data_range = response["data"]["start"].split(" ~ ")

            price = pd.DataFrame(price)
            price["week_start"] = data_range[0].replace("/", "-")
            price["week_end"] = data_range[1].replace("/", "-")
            price["Nums"] = price["Nums"].map(lambda x: int(x))
            price["Oil92"] = price["Oil92"].map(lambda x: float(x) if x else None)
            price["Oil95"] = price["Oil95"].map(lambda x: float(x) if x else None)
            price["Oil98"] = price["Oil98"].map(lambda x: float(x) if x else None)
            price["Oilchai"] = price["Oilchai"].map(lambda x: float(x) if x else None)
            price.rename(
                columns={
                    "City": "county",
                    "Town": "town",
                    "Type": "type",
                    "Nums": "num",
                    "Oil92": "gasoline_92",
                    "Oil95": "gasoline_95",
                    "Oil98": "gasoline_98",
                    "Oilchai": "diesel",
                },
                inplace=True,
            )
            price = price[
                [
                    "week_start",
                    "week_end",
                    "county",
                    "town",
                    "type",
                    "num",
                    "gasoline_92",
                    "gasoline_95",
                    "gasoline_98",
                    "diesel",
                ]
            ]
            return price

    def get_weekly_data(self, week_id: int = None) -> pd.DataFrame:
        if not week_id:
            week = Week()
            week_id = week.get_week_id() - 1  # 資料有1週的延遲
        response = self.__get_data(week_id=week_id)
        data = self.__process_data(response)
        data.insert(0, "week_id", week_id)
        return data


class RefPrice:
    def __init__(self):
        self.url = "https://www2.moeaea.gov.tw/oil111/Gasoline/RetailPrice"
        self.url_post = "https://www2.moeaea.gov.tw/oil111/Gasoline/RetailPrice/load"  # uesd to get the data

    def __payload(self, start_date: str, end_date: str):
        """payload

        Parameters
        ----------

        start : str
            YYYY/MM/DD
        end : str
            YYYY/MM/DD

        Returns
        -------
        payload : dict
            payload

        Examples
        --------
        payload("2015/01/01", "2023/12/31")
        >>> {"start": "2015/01/01", "end": "2023/12/31"}
        """
        payload = {"start": start_date, "end": end_date}
        return payload

    def __get_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        res = requests.post(
            self.url_post,
            data=self.__payload(start_date=start_date, end_date=end_date),
            headers={"User-Agent": "Mozilla/5.0"},
        )
        res = json.loads(res.text)
        return res

    def __process_data(self, response) -> pd.DataFrame:
        if response["res"] != "00":
            price = response["data"]["gasoline"]
            price.reverse()
            price = pd.DataFrame(price)
            price["Date"] = price["Date"].map(lambda x: x.replace("/", "-"))
            price.rename(
                columns={
                    "Date": "date",
                    "A92": "cpc_gasoline_92",
                    "A95": "cpc_gasoline_95",
                    "A98": "cpc_gasoline_98",
                    "Achai": "cpc_diesel",
                    "B92": "fpcc_gasoline_92",
                    "B95": "fpcc_gasoline_95",
                    "B98": "fpcc_gasoline_98",
                    "Bchai": "fpcc_diesel",
                },
                inplace=True,
            )

            price["week_start"] = price["date"].map(lambda x: x.replace("/", "-"))
            price = price[
                [
                    "week_start",
                    "cpc_gasoline_92",
                    "cpc_gasoline_95",
                    "cpc_gasoline_98",
                    "cpc_diesel",
                    "fpcc_gasoline_92",
                    "fpcc_gasoline_95",
                    "fpcc_gasoline_98",
                    "fpcc_diesel",
                ]
            ]
            return price
        else:
            return None

    def get_weekly_data(
        self, start_date: str = None, end_date: str = None
    ) -> pd.DataFrame:
        if all([start_date, end_date]):
            res = self.__get_data(start_date=start_date, end_date=end_date)
        elif start_date:
            res = self.__get_data(start_date=start_date, end_date=start_date)
        else:
            week = Week()
            start_date, end_date = week.get_week_range()
            start_date = start_date.strftime("%Y/%m/%d")
            end_date = end_date.strftime("%Y/%m/%d")
            res = self.__get_data(start_date=start_date, end_date=end_date)

        data = self.__process_data(res)
        return data
