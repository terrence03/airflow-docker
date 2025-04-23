from io import StringIO
from urllib import parse
import hashlib
import string
import requests
from bs4 import BeautifulSoup
import pandas as pd

pd.set_option("future.no_silent_downcasting", True)


class CpcCrawler:
    def __init__(self):
        self.URL = "https://vipmbr.cpc.com.tw/mbwebs/service_search.aspx"
        self.session = requests.Session()

        response = self.session.get(self.URL)
        soup = BeautifulSoup(response.text, "html.parser")
        view_state = soup.find("input", {"id": "__VIEWSTATE"})["value"]
        view_state_generator = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})[
            "value"
        ]
        event_validation = soup.find("input", {"id": "__EVENTVALIDATION"})["value"]
        self.payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": view_state,
            "__VIEWSTATEGENERATOR": view_state_generator,
            "__EVENTVALIDATION": event_validation,
            "TypeGroup": "rbGroup1",
            "ddlCity": "全部縣市",
            "ddlSubCity": "全部鄉鎮區",
            "tbKWQuery": "",
            "TimeGroup": "rbGroup4",
            "btnQuery": "查+++詢",
        }

    def _process_table_1(self, table):
        columns = [
            "county",
            "town",
            "category",
            "name",
            "address",
            "phone",
            "business_hours",
            "98_gasoline",
            "95_gasoline",
            "92_gasoline",
            "alcohol_gasoline",
            "kerosene",
            "diesel",
            "gasoline_self_service",
            "diesel_self_service",
            "toliet",
            "accessible_toilet",
        ]
        table.columns = columns
        table["county"] = table["county"].str.replace("台", "臺")
        station_id_pattern = r"([A-Z]\d{3}[A-Z0-9])"
        table.insert(
            0,
            "station_id",
            table["name"].str.extract(station_id_pattern, expand=False),
        )
        table["name"] = table["name"].str.replace(station_id_pattern, "", regex=True)
        table["name"] = table["name"].str.replace(" ", "")
        for col in table.columns[8:]:
            table[col] = table[col].apply(lambda x: 1 if x == "●" else 0)
        return table

    def _process_table_2(self, table):
        columns = [
            "county",
            "town",
            "category",
            "name",
            "address",
            "phone",
            "business_hours",
            "98_gasoline",
            "95_gasoline",
            "92_gasoline",
            "alcohol_gasoline",
            "kerosene",
            "diesel",
            "gasoline_self_service",
            "diesel_self_service",
            "toliet",
            "accessible_toilet",
        ]
        table.insert(11, "_", None)
        table.columns = columns
        table["county"] = table["county"].str.replace("台", "臺")
        station_id_pattern = r"([A-Z]{2}\d{4}[A-Z]\d{2})"
        table.insert(
            0,
            "station_id",
            table["name"].str.extract(station_id_pattern, expand=False),
        )
        table["name"] = table["name"].str.replace(station_id_pattern, "", regex=True)
        table["name"] = table["name"].str.replace(" ", "")
        for col in table.columns[8:]:
            table[col] = table[col].apply(lambda x: 1 if x == "●" else 0)
        return table

    def crawl(self) -> list[pd.DataFrame]:
        response = self.session.post(self.URL, data=self.payload)
        soup = BeautifulSoup(response.text, "html.parser")
        table1 = soup.find("table", {"id": "MyGridView1"})
        table2 = soup.find("table", {"id": "MyGridView2"})
        table1 = pd.read_html(StringIO(str(table1)))[0]
        table2 = pd.read_html(StringIO(str(table2)))[0]
        table1 = self._process_table_1(table1)
        table2 = self._process_table_2(table2)
        return pd.concat([table1, table2], ignore_index=True)


class FpccCrawler:
    def __init__(self):
        self.URL = "https://www.fpcc.com.tw/tw/events/stations/"
        self.data_title = [
            "name",
            "address",
            "phone",
            "business_hours",
            "92_gasoline",
            "95_gasoline",
            "98_gasoline",
            "diesel",
            "self_service",
            "no_tune_off_serv",
            "APP",
            "fpcc_co_card",
            "fpcc_business_card",
            "travel_card",
            "taxi_card",
        ]
        self.counties = [
            "臺北市",
            "新北市",
            "基隆市",
            "桃園市",
            "新竹縣",
            "新竹市",
            "苗栗縣",
            "臺中市",
            "彰化縣",
            "南投縣",
            "雲林縣",
            "嘉義縣",
            "嘉義市",
            "臺南市",
            "高雄市",
            "屏東縣",
            "宜蘭縣",
            "花蓮縣",
            "臺東縣",
        ]
        self.BASE62_ALPHA = (
            string.digits + string.ascii_uppercase + string.ascii_lowercase
        )

    def crawl_by_county(self, county: str):
        """
        Crawl the FPCC website for the given county.
        Args:
            county (str): The name of the county to crawl.
        Returns:
            pd.DataFrame: A dataframe containing the data.
        """
        # url encoding
        county = parse.quote(county)
        URL = f"{self.URL}{county}"
        res = requests.get(URL)
        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.find("ul", {"class": "reload-layout"})

        rows = []
        for i in table.find_all("li"):
            row = []
            for j in i.find_all("div"):
                row.append(j.text.strip())
            rows.append(row)

        df = pd.DataFrame(rows, columns=self.data_title)
        df = df.drop(0)
        df = df.reset_index(drop=True)
        df = df.infer_objects(copy=True)
        df = df.replace("◯\n╳", 1).infer_objects(copy=True)
        df = df.replace("", 0).infer_objects(copy=True)

        return df

    def generate_station_id(self, county: str, name: str) -> str:
        """
        生成station_id, 格式為{縣市代碼}{加盟商代碼}{名稱代碼}
        縣市代碼: 由縣市對應字母A-S, 順序參考self.counties
        加盟商代碼: 透過站名前2位文字查詢加盟商名稱, 若為連鎖則依序轉換為A-H, 非連鎖則為Z
        名稱代碼: 將站名去除連鎖加盟商名稱及最後1碼(站)取得中間名稱, 轉為UTF-8後做SHA1 hash, 取前6碼

        Args:
            county (str): 縣市名稱
            name (str): 站名
        Returns:
            str: 生成的station_id
        例:
            generate_station_id("臺北市", "台亞基河站") -> "AAUBBTpw"
        例:
        """
        # county to code
        county_code = self.counties.index(county)
        county_code = chr(county_code + 65)

        # franchise to code
        franchises = {
            "台亞": "A",
            "全國": "B",
            "統一": "C",
            "福懋": "D",
            "山隆": "E",
            "全鋒": "F",
            "久井": "G",
            "中華": "H",
        }
        if name[:2] in franchises:
            franchise_code = franchises[name[:2]]
            last_name = name[2:-1]
        else:
            franchise_code = "Z"
            last_name = name[:-1]

        # name to code
        def to_base62(num):
            if num == 0:
                return self.BASE62_ALPHA[0]
            base62 = []
            while num > 0:
                num, rem = divmod(num, 62)
                base62.append(self.BASE62_ALPHA[rem])
            return "".join(reversed(base62))

        def name_to_base62_id(text, length=10):
            # 將文字轉為 UTF-8 並做 SHA1 hash
            sha1 = hashlib.sha1(
                text.encode("utf-8")
            ).digest()  # bytes (20 bytes = 160 bits)
            # 將 hash bytes 轉為整數
            num = int.from_bytes(sha1, byteorder="big")
            # 編碼為 base62
            base62_str = to_base62(num)
            return base62_str[:length]

        name_code = name_to_base62_id(last_name, 6)

        # 拼接
        station_id = f"{county_code}{franchise_code}{name_code}"
        return station_id

    def crawl(self) -> pd.DataFrame:
        df_condated = pd.DataFrame()
        for county in self.counties:
            df = self.crawl_by_county(county)
            df.insert(0, "county", county)
            df_condated = pd.concat([df_condated, df], ignore_index=True)
        df_condated.insert(
            0,
            "station_id",
            df_condated.apply(
                lambda x: self.generate_station_id(x["county"], x["name"]), axis=1
            ),
        )
        return df_condated


class MoeaCrawler:
    def __init__(self):
        self.URL = "https://www2.moeaea.gov.tw/oil111/Dealer/GasStations/load"

    def crawl(self) -> tuple[str, pd.DataFrame]:
        response = requests.post(
            self.URL,
            headers={"User-Agent": "Mozilla/5.0"},
            data={"city": "all"},
        )
        data = response.json()
        data = data["data"]
        update_time = data["UpdateTime"]
        stations = data["stations"]
        stations = pd.DataFrame(stations)
        return update_time, stations
