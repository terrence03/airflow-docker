import json
from pathlib import Path
import requests
import pandas as pd


URL = {
    "GIS_PAGE": {
        "url": "https://www2.moeaea.gov.tw/oil111/LPG/GISAvg/",
        "name": "縣市零售價格主頁",
    },
    "COUNTY_PRICE": {
        "url": "https://www2.moeaea.gov.tw/oil111/LPG/CountyCityAvg/",
        "name": "縣市/區域平均價格",
    },
    "TOWN_PRICE": {
        "url": "https://www2.moeaea.gov.tw/oil111/LPG/GISTownAvg/",
        "name": "鄉鎮市區平均價格",
    },
}


def requests_post(url: str, payload: dict = None) -> dict:
    """
    Send a POST request with optional payload and return the JSON response.

    Parameters
    ----------
    url : str
        The URL to send the POST request to.
    payload : dict, optional
        The payload to include in the POST request, by default None.

    Returns
    -------
    dict
        The JSON response from the POST request.

    Raises
    ------
    requests.HTTPError
        If the POST request returns a non-200 status code.
    ValueError
        If the JSON response contains an error message.

    """
    res = requests.post(
        url + "load",
        data=payload,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=10,
    )
    res.raise_for_status()
    res.encoding = "UTF-8"
    res = res.json()
    if res["res"] != "01":
        raise ValueError(res["msg"])
    return res


def get_lastest_period() -> str:
    """
    Get the default period of the data.

    Returns
    -------
    str
        The default period of the data, in the format of "YYYY/MM".

    Notes
    -----
    This function sends a POST request to the specified URL with the payload
    containing the unit, oil, and start parameters. It expects a JSON response
    with a "data" field that contains an "end" value representing the default period.

    """
    res = requests_post(
        URL["GIS_PAGE"]["url"],
        payload={"unit": "month", "oil": "05", "start": "lastone"},
    )
    return res["data"]["end"]


def generate_payload(**kwargs) -> dict:
    """
    Generate a payload dictionary for the POST request.

    Parameters
    ----------
    **kwargs
        Additional parameters to include in the payload.

    Returns
    -------
    dict
        The generated payload dictionary.

    """
    period = get_lastest_period()
    payload = {"unit": "month", "oil": "05", "start": period}
    if kwargs:
        payload.update(kwargs)
    return payload


def get_area_info() -> dict[str, int]:
    """
    Get the latest retail price of LPG in different areas.

    Returns
    -------
    dict[str, int]
        A dictionary containing the latest retail price of LPG in different areas.
        The keys represent the area names, and the values represent the corresponding prices.
        The areas include:
        - mainland_survey_num: The number of surveys conducted in the main island
        - mainland_avg: The average retail price in the main island
        - mainland_high: The highest retail price in the main island
        - mainland_low: The lowest retail price in the main island
        - offshore_survey_num: The number of surveys conducted in the offshore islands
        - offshore_avg: The average retail price in the offshore islands
        - offshore_high: The highest retail price in the offshore islands
        - offshore_low: The lowest retail price in the offshore islands
        - mountain_survey_num: The number of surveys conducted in the mountainous areas
        - mountain_avg: The average retail price in the mountainous areas
        - mountain_high: The highest retail price in the mountainous areas
        - mountain_low: The lowest retail price in the mountainous areas

    """
    res = requests_post(URL["COUNTY_PRICE"]["url"], payload=generate_payload())
    info = {
        "mainland_survey_num": int(res["data"]["Anum"]),
        "mainland_avg": round(res["data"]["Aavg"]),
        "mainland_high": int(res["data"]["Ahigh"]),
        "mainland_low": int(res["data"]["Alow"]),
        "offshore_survey_num": int(res["data"]["Bnum"]),
        "offshore_avg": round(res["data"]["Bavg"]),
        "offshore_high": int(res["data"]["Bhigh"]),
        "offshore_low": int(res["data"]["Blow"]),
        "mountain_survey_num": int(res["data"]["Cnum"]),
        "mountain_avg": round(res["data"]["Cavg"]),
        "mountain_high": int(res["data"]["Chigh"]),
        "mountain_low": int(res["data"]["Clow"]),
    }
    return info


def get_counties_price(payload: dict = None) -> pd.DataFrame:
    """
    Get the latest retail price of LPG in counties and cities.

    Parameters
    ----------
    payload : dict, optional
        The payload for the post request, by default None.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the latest retail price of LPG in counties and cities.
        The DataFrame has the following columns:
        - county: The name of the county.
        - year: The year of the survey.
        - month: The month of the survey.
        - mainland_avg: The average price of LPG in the mainland.
        - mainland_high: The highest price of LPG in the mainland.
        - mainland_low: The lowest price of LPG in the mainland.
        - mainland_survey_num: The number of surveys conducted in the mainland.
        - mainland_total_num: The total number of LPG prices surveyed in the mainland.
        - offshore_avg: The average price of LPG offshore.
        - offshore_high: The highest price of LPG offshore.
        - offshore_low: The lowest price of LPG offshore.
        - offshore_survey_num: The number of surveys conducted offshore.
        - offshore_total_num: The total number of LPG prices surveyed offshore.
        - mountain_avg: The average price of LPG in the mountains.
        - mountain_high: The highest price of LPG in the mountains.
        - mountain_low: The lowest price of LPG in the mountains.
        - mountain_survey_num: The number of surveys conducted in the mountains.
        - mountain_total_num: The total number of LPG prices surveyed in the mountains.

    """
    payload = payload or generate_payload()
    res = requests_post(URL["COUNTY_PRICE"]["url"], payload)
    table = pd.DataFrame(res["data"]["dataarray"])
    table.rename(
        columns={
            "City": "county",
            "Aavg": "mainland_avg",
            "Ahigh": "mainland_high",
            "Alow": "mainland_low",
            "Asnum": "mainland_survey_num",
            "Atnum": "mainland_total_num",
            "Bavg": "offshore_avg",
            "Bhigh": "offshore_high",
            "Blow": "offshore_low",
            "Bsnum": "offshore_survey_num",
            "Btnum": "offshore_total_num",
            "Cavg": "mountain_avg",
            "Chigh": "mountain_high",
            "Clow": "mountain_low",
            "Csnum": "mountain_survey_num",
            "Ctnum": "mountain_total_num",
        },
        inplace=True,
    )

    table["year"] = table["SurDate"].str[:4].astype(int)
    table["month"] = table["SurDate"].str[5:7].astype(int)
    table = table[
        [
            "year",
            "month",
            "county",
            "mainland_avg",
            "mainland_high",
            "mainland_low",
            "mainland_survey_num",
            "mainland_total_num",
            "offshore_avg",
            "offshore_high",
            "offshore_low",
            "offshore_survey_num",
            "offshore_total_num",
            "mountain_avg",
            "mountain_high",
            "mountain_low",
            "mountain_survey_num",
            "mountain_total_num",
        ]
    ]
    return table


def get_towns_price(payload: dict = None) -> pd.DataFrame:
    """
    Get the price data for all towns.

    Parameters
    ----------
    payload : dict, optional
        The payload for the post request, by default None.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the price data for all towns.

    """
    with open(Path(__file__).parent / "CityNo.json", "r", encoding="UTF-8") as f:
        cityno_dict = json.load(f)
    payload = payload or generate_payload()
    data = pd.DataFrame()
    for county in cityno_dict.keys():
        payload.update({"cityno": cityno_dict[county]})
        county_data = get_town_price(county, payload)
        data = pd.concat([data, county_data])
    return data


def get_town_price(county: str, payload: dict = None) -> pd.DataFrame:
    """
    Get the latest retail price of LPG in towns within a specific county.

    Parameters
    ----------
    county : str
        The name of the county.
    payload : dict, optional
        The payload for the post request, by default None.

    Returns
    -------
    pd.DataFrame
        The latest retail price of LPG in towns within the specified county.

    Raises
    ------
    FileNotFoundError
        If the CityNo.json file is not found.

    Notes
    -----
    This function retrieves the latest retail price of LPG (liquefied petroleum gas) in towns and townships
    for a specific county. It makes a POST request to a specified URL with the given payload to fetch the data.
    The data is then processed and returned as a pandas DataFrame.

    The payload parameter is optional. If not provided, a default payload will be generated.

    The returned DataFrame contains the following columns:
    - year: The year of the data.
    - month: The month of the data.
    - county: The name of the county.
    - town: The name of the town or township.
    - avg: The average retail price of LPG.
    - high: The highest retail price of LPG.
    - low: The lowest retail price of LPG.

    If the CityNo.json file is not found in the same directory as this script, a FileNotFoundError will be raised.

    Example
    -------
    >>> payload = {"param1": "value1", "param2": "value2"}
    >>> data = get_town_price("Example County", payload)
    >>> print(data.head())

       year  month         county         town    avg  high   low
    0  2022      1  Example County  ExampleTown  10.50  11.0  10.0
    1  2022      1  Example County  AnotherTown  11.20  12.0  10.5
    2  2022      1  Example County   LastTown    9.80  10.5   9.0
    ...

    """
    with open(Path(__file__).parent / "CityNo.json", "r", encoding="UTF-8") as f:
        cityno_dict = json.load(f)
    payload = payload or generate_payload()
    payload.update({"cityno": cityno_dict[county]})
    county_data = requests_post(URL["TOWN_PRICE"]["url"], payload)
    table = pd.DataFrame(county_data["data"]["dataarray"])
    table.rename(
        columns={"City": "town", "Cavg": "avg", "Chigh": "high", "Clow": "low"},
        inplace=True,
    )

    table["county"] = county
    table["year"] = table["SurDate"].str[:4].astype(int)
    table["month"] = table["SurDate"].str[5:7].astype(int)
    table = table[
        [
            "year",
            "month",
            "county",
            "town",
            "avg",
            "high",
            "low",
        ]
    ]
    return table
