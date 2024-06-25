from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np

table_data_map = {
    'apt' : '아파트_매매',
    'office_raw' : '오피스텔_매매',
    'house_raw' : '단독다가구_매매',
    'multi_family_raw' : '다세대_매매',

    'apt_lease_raw' : '아파트_임대',
    'house_lease_raw' : '단독다가구_임대',
    'office_lease_raw' : '오피스텔_임대',
    'multi_family_lease_raw' : '다세대_임대'
}


class RealEstateAnalyzer:
    def __init__(self) :
        """
        RealEstate.db에 있는 테이블 목록
        -----------
        <매매>
        [v]아파트_매매 : apt
        [ ]오피스텔_매매 : office_raw
        [ ]다가구_매매 : house_raw
        [ ]다세대_매매 : multi_family_raw

        <임대>
        [ ]아파트_임대 : apt_lease_raw
        [ ]다가구_임대 : house_lease_raw
        [ ]오피스텔_임대 : office_lease_raw
        [ ]다세대_임대 : multi_family_lease_raw
        -----------
        """
        self.db_path = '/Users/hj/Dropbox/real_estate/data/api/db/RealEstate.db'
        self.eng = create_engine(f'sqlite:///{self.db_path}')
        with self.eng.connect() as conn:
            result = conn.execute(text("SELECT name FROM sqlite_master WHERE type = 'table' "))
            for row in result :
                print(row)

    def load_data(self, data_type, nrow = None):
        """
        데이터 유형별 불러오기
        """
        self.data_type = data_type
        with self.eng.connect() as conn:
            if nrow is None :
                self.df = pd.read_sql_query(f"SELECT * FROM {data_type}", con = conn)
            else :
                self.df = pd.read_sql_query(f"SELECT * FROM {data_type} LIMIT {nrow}", con = conn)
            
            self.code = pd.read_sql_query(f"SELECT * FROM conn_code", con = conn)
            
        print(f"{data_type} is loaded. 데이터의 수는 {self.df.shape}")
        
    def remove_duplicates_df(self) :
        """
        판다스로 중복 데이터 제거
        """
        print("중복 제거전 ", self.df.shape)
        self.df.drop_duplicates(inplace = True)
        print("중복 제거 후 ", self.df.shape)

    def preprocess(self) :
        """
        전처리
        1. 데이터 타입 변형
        2. 새로운 칼럼 생성 : 거래일자, 평당거래금액
        3. 칼럼 순서 변경 및 제거
        """
        # 거래금액 정수로 변경
        int_cols = ['거래금액', '월세금액', '보증금액', '건축년도', '년', '월', '일']
        for col in int_cols :
            if col in self.df.columns :
                self.df[col] = self.df[col].str.replace(',', '').fillna('0').astype(int)

        def lease_tranform(self):
            if "보증금액" in self.df.columns :
                rate = 6
                self.df["거래금액"] = self.df["보증금액"] + self.df["월세금액"] * 100/rate

        self.lease_tranform()

        if self.data_type == "house_raw" :
            self.df['면적'] = self.df['연면적'].astype(float)

        else :
            self.df['면적'] = self.df['전용면적'].astype(float)

        # 년월일을 합쳐서 거래일자 칼럼 생성
        date_str = self.df[['년', '월', '일']].astype(str).agg('-'.join, axis = 1)
        self.df['거래일자'] = pd.to_datetime(date_str)

        # 평당거래금액 
        self.df['평당거래금액'] = round(self.df['거래금액']/self.df['면적'] * 3.30579, 2)

        # 데이터프레임 칼럼 순서 변경 
        col_order = ["거래일자", "지역코드"] + [col for col in self.df.columns if col not in ['거래일자', '지역코드']]
        self.df = self.df[col_order]

        # 필요없는 칼럼 제거
        drop_cols = ['거래유형', '매수자', '매도자', '중개사소재지', '해제사유발생일', '해제여부', '월', '일']
        self.df.drop(columns = drop_cols,inplace = True, errors = 'ignore')
        print("전처리 완료")

    def lease_tranform(self):
        if "보증금액" in self.df.columns :
            rate = 6
            self.df["거래금액"] = self.df["보증금액"] + self.df["월세금액"] * 100/rate

    def map_hdong(self) :
        """
        행정동과 매핑
        """
        # 코드 정리
        self.code.drop_duplicates(subset=['시군구코드', '시군구명'], inplace = True) # conn_code의 중복제거
        self.code = self.code[["시도명", "시군구명", "시군구코드"]]

        self.df = pd.merge(self.df, self.code, left_on = '지역코드', right_on = '시군구코드', how = 'left')
    
    def avg_price_year(self, region) :
        self.region = region
        def gini(array):
            array = array.flatten().astype(float)

            if np.amin(array) < 0:
                array -= np.amin(array)
            array += 0.0000001
            array = np.sort(array)
            index = np.arange(1, array.shape[0] + 1)
            n = array.shape[0]

            if n > 1 :
                result = ((np.sum((2 * index - n - 1) * array)) / (n * np.sum(array)))
                return result
            else :
                return 1
        
        if region == '법정동' :
            group = ['년', '시도명', '시군구명', '법정동']

        elif region == '시군구':
            group = ['년', '시도명', '시군구명']

        grouped = self.df.groupby(by = group).agg(
            거래수 = ('거래금액', 'size'),
            평균거래금액 = ('거래금액', 'mean'),
            거래금액지니계수 = ('거래금액', lambda x : gini(x.to_numpy())),
            평균평당거래금액 = ('평당거래금액', 'mean'),
            평당거래지니계수 = ('평당거래금액', lambda x : gini(x.to_numpy()))
            ).reset_index()
        
        # 소수점
        avg_cols = ['평균평당거래금액', '평균거래금액']
        gini_cols = ['거래금액지니계수', '평당거래지니계수']
        grouped[avg_cols] = grouped[avg_cols].round(1)
        grouped[gini_cols] = grouped[gini_cols].round(4)

        # 시도명 순서 지정
        order = {'서울특별시' : 1, '경기도': 2}
        grouped['시도명순서'] = grouped['시도명'].map(order).fillna(99).astype(int)
        grouped = grouped.sort_values(by=['년', '시도명순서']).drop(columns=['시도명순서'])
        self.grouped = grouped

        print("그룹별 계산 완료!")
        return grouped
    
    def save_result(self) :
        import datetime
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        result_name = table_data_map[self.data_type]
        file_name = f"{result_name}_{self.region}_지니계수.csv"

        self.grouped.to_csv(f"report/result/{file_name}", index = False)
        print(f"{result_name} is saved to report/result/{file_name}")

    def main(self, dtype, region = 'bdong', nrow = None) :
            self.load_data(dtype, nrow = nrow)
            self.remove_duplicates_df() 
            self.preprocess()
            self.lease_tranform()
            self.map_hdong()
            self.avg_price_year(region)
            self.save_result()
    

RA = RealEstateAnalyzer()

for dtype in ['office_raw', 'house_raw', 'multi_family_raw']:
    for region in ['시군구', '법정동']:
        RA.main(dtype = dtype, region = region)
