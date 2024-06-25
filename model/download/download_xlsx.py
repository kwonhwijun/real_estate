import os
import requests
from datetime import datetime
import sqlite3
import pandas as pd
from io import BytesIO
class RealEstateDataDownloader:
    def __init__(self, start_year, start_month, end_year, end_month):
        self.start_year = start_year
        self.start_month = start_month
        self.end_year = end_year
        self.end_month = end_month
        self.base_url = "https://rtmobile.molit.go.kr/pt/xls/ptXlsExcelDown.do"
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Cookie": "WMONID=gK5CTOLiNV8; JSESSIONID=S87fEuz60AduewvbQjreqdfo7MivA4MxTIH8KhTx.RT_DN20",
            "Host": "rtmobile.molit.go.kr",
            "Origin": "https://rtmobile.molit.go.kr",
            "Referer": "https://rtmobile.molit.go.kr/pt/xls/xls.do?mobileAt=",
            "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "iframe",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        }
        self.db_path = '/Users/hj/Dropbox/real_estate/data/조건별 자료제공/RealEstate_xlsx.db'
        self.conn = sqlite3.connect(self.db_path)

    def __del__(self):
        self.conn.close()

    def download_data(self, year, month, property_type, trans_type):
        month_str = f"{month:02d}"
        srhFromDt = f"{year}-{month_str}-01"
        srhToDt = f"{year}-{month_str}-{(datetime(year, month + 1, 1) - datetime(year, month, 1)).days:02d}" if month < 12 else f"{year}-12-31"

        type_mapping = {
            '아파트': 'A',
            '연립다세대': 'B',
            '단독다가구' : 'C',
            '오피스텔' : 'D'
        }

        trans_mapping = {
            '매매' : 1,
            '전월세' : 2
        }
        srhThingNo = type_mapping.get(property_type)
        trans_num = trans_mapping.get(trans_type)

        data = {
            "srhThingNo": srhThingNo,
            "srhDelngSecd": trans_num,
            "srhAddrGbn": "1",
            "srhLfstsSecd": "1",
            "sidoNm": "전체",
            "sggNm": "전체",
            "emdNm": "전체",
            "loadNm": "전체",
            "areaNm": "전체",
            "hsmpNm": "전체",
            "mobileAt": "",
            "srhFromDt": srhFromDt,
            "srhToDt": srhToDt,
            "srhSidoCd": "",
            "srhSggCd": "",
            "srhEmdCd": "",
            "srhRoadNm": "",
            "srhLoadCd": "",
            "srhHsmpCd": "",
            "srhArea": "",
            "srhFromAmount": "",
            "srhToAmount": ""
        }

        response = requests.post(self.base_url, headers=self.headers, data=data)
        return response.content

    def save_data_to_db(self, content, year, month, property_type, trans_type):
        month_str = f"{month:02d}"
        property_mapping = {
            '아파트': 'apt',
            '연립다세대': 'multi_house',
            '단독다가구': 'multi_unit',
            '오피스텔': 'officetel'
        }

        trans_mapping = {
            '매매': 'sale',
            '전월세': 'rent'
        }

        table_name = f"{property_mapping[property_type]}_{trans_mapping[trans_type]}"
        xls_df = pd.read_excel(content)
        xls_df.to_sql(table_name, self.conn, if_exists='append', index=False)
        print(f"Data saved to database table: {table_name}")


    def save_data(self, content, year, month, property_type, trans_type):
        month_str = f"{month:02d}"
        base_path = '/Users/hj/Dropbox/real_estate/data/조건별 자료제공'
        folder_name = os.path.join(str(trans_type), str(property_type))
        folder_name = os.path.join(base_path, folder_name)
        directory = os.path.join(folder_name, str(year))
        os.makedirs(directory, exist_ok=True)
        filename = os.path.join(directory, f"{property_type}({trans_type})_{year}_{month_str}.xlsx")

        with open(filename, "wb") as file:
            file.write(content)
        print(f"File saved: {filename}")

    def download_and_save_all_data(self):
        for year in range(self.start_year, self.end_year + 1):
            start_month = self.start_month if year == self.start_year else 1
            end_month = self.end_month if year == self.end_year else 12

            for month in range(start_month, end_month + 1):
                for property_type in ['아파트', '연립다세대', '단독다가구','오피스텔']:
                    for trans_type in ['매매', '전월세']:
                        content = self.download_data(year, month, property_type, trans_type)
                        self.save_data(content, year, month, property_type, trans_type)
                        #self.save_data_to_db(content, year, month, property_type, trans_type)

if __name__ == "__main__":
    downloader = RealEstateDataDownloader(2007, 5, 2024, 5) 
    downloader.download_and_save_all_data()