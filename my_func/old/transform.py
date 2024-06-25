import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

class RealEstateDataAnalyzer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.column_mapping = {
            'apt_raw': '법정동시군구코드',
            'office_raw': '지역코드'
        }
    
    def load_data(self, table_name):
        with self.engine.connect() as conn:
            query = text(f"SELECT * FROM {table_name} ORDER BY ROWID DESC LIMIT 1000000")
            df = pd.read_sql_query(query, conn)
            
        return df
    
    def preprocess_data(self, df, code_column):
        # '거래금액' 열의 쉼표 제거 및 숫자형으로 변환
        df['거래금액'] = df['거래금액'].str.replace(',', '').astype(int)
        # 년, 월 칼럼을 날짜 형식으로 통합
        df['연월'] = pd.to_datetime(df['년'] + '-' + df['월'] + '-01')
        # 칼럼 이름을 통일
        df.rename(columns={code_column: '법정동코드'}, inplace=True)
        return df
    
    def calculate_average_price_by_region_and_month(self, df):
        avg_prices = df.groupby(['법정동코드', '연월'])['거래금액'].mean().reset_index()
        avg_prices['거래금액'] = avg_prices['거래금액'].round(1)
        return avg_prices
    
    def calculate_gini_coefficient(self, df):
        def gini(arr):
            # 모든 값을 동일하게 처리하고 배열을 1차원으로 변환:
            arr = np.array(arr, dtype=np.float64)
            if np.amin(arr) < 0:
                # 값이 음수일 수 없음:
                arr -= np.amin(arr)
            # 값이 0일 수 없음:
            arr += 0.0000001
            # 값을 정렬:
            arr = np.sort(arr)
            # 배열 요소당 인덱스:
            index = np.arange(1, arr.shape[0] + 1)
            # 배열 요소 수:
            n = arr.shape[0]
            # 지니계수:
            return ((np.sum((2 * index - n - 1) * arr)) / (n * np.sum(arr)))
        
        gini_by_region_and_month = df.groupby(['법정동코드', '연월'])['거래금액'].apply(gini).reset_index()
        gini_by_region_and_month.rename(columns={'거래금액': '지니계수'}, inplace=True)
        gini_by_region_and_month['지니계수'] = gini_by_region_and_month['지니계수'].round(3)
        return gini_by_region_and_month
    
    def calculate_transaction_count(self, df):
        return df.groupby(['법정동코드', '연월']).size().reset_index(name='거래수')
    
    def process_all_types(self):
        results = {}
        
        with self.engine.connect() as conn :
            query = text('SELECT 시군구코드, 시군구명 FROM conn_code')
            code_df = pd.read_sql_query(query, conn)
            code_df = code_df.drop_duplicates(subset='시군구코드')

        for house_type, code_column in self.column_mapping.items():
            df = self.load_data(house_type)
            df = self.preprocess_data(df, code_column)

             # Filter data to include only records before 2024-04
            df = df[df['연월'] < '2024-05-01']

            avg_prices = self.calculate_average_price_by_region_and_month(df)
            gini_coefficients = self.calculate_gini_coefficient(df)
            transaction_counts = self.calculate_transaction_count(df)
            combined = pd.merge(avg_prices, gini_coefficients, on=['법정동코드', '연월'])
            combined = pd.merge(combined, transaction_counts, on=['법정동코드', '연월'])

            # Merge with code_df to get the 시군구명
            combined = pd.merge(combined, code_df, left_on='법정동코드', right_on='시군구코드', how='left')
            combined.drop(columns=['시군구코드'], inplace=True)

            # Rename columns
            combined.rename(columns={'거래금액': '평균거래금액'}, inplace=True)
            # Format '연월' to 'YYYY-MM'
            combined['연월'] = combined['연월'].dt.strftime('%Y-%m')


            # Reorder columns to have 법정동코드 and 시군구명 first
            columns_order = ['법정동코드', '시군구명'] + [col for col in combined.columns if col not in ['법정동코드', '시군구명']]
            combined = combined[columns_order]


            combined = combined.sort_values(by = ['연월', '법정동코드'], ascending= [False, True])
            results[house_type] = combined
        
        return results
    def save_results(self, results, ouput_file_names):
        for house_type, filename in ouput_file_names.items():
            if house_type in results:
                results[house_type].to_csv(filename, index = False)
                print(f'Results for {house_type} saved to {filename}')

def main():
    db_path = '/Users/hj/Dropbox/real_estate/data/db/RealEstate.db'
    output_filenames = {
        'apt_raw': 'apartment_results.csv',
        'office_raw': 'office_results.csv'
    }
    analyzer = RealEstateDataAnalyzer(db_path)
    results = analyzer.process_all_types() 
    analyzer.save_results(results, output_filenames)

    for house_type, stats in results.items():
        print(f"Statistics for {house_type}:")
        print(stats)

if __name__ == "__main__":
    main()




