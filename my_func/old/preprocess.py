import sys
import os

sys.path.append('/Users/hj/Dropbox/real_estate/model/my_func')
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text
import api
import pandas as pd
import matplotlib.pylab as plt
from sklearn.linear_model import LinearRegression
import numpy as np
import seaborn as sns
from matplotlib import rc
import time


# 한글 폰트 설정
rc('font', family='AppleGothic')
plt.rcParams['axes.unicode_minus'] = False

db_path = r'/Users/hj/Dropbox/real_estate/data/db/RealEstate.db'
engine= create_engine(f'sqlite:///{db_path}')

def execute_query(query):
    with engine.connect() as conn :
        result = conn.execute(text(f"{query}"))
    return result

# db 내 테이블 명 리스트를 출력
def table_list():
    with engine.connect() as conn :
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE TYPE = 'table'"))
        tab_list = [tab_name[0] for tab_name in result]
    print(tab_list)

def table_info():
    table_details = []

    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE type = 'table'"))
        tab_list = [tab_name[0] for tab_name in result]

        for table in tab_list:
            row_count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            row_count = row_count_result.fetchone()[0]

            column_count_result = conn.execute(text(f"PRAGMA table_info({table})"))
            column_count = len(column_count_result.fetchall())

            table_details.append({
                'Table Name': table,
                'Row Count': row_count,
                'Column Count': column_count
            })

    df = pd.DataFrame(table_details)
    return df

def drop_table(tab_name):
    execute_query(f'DROP TABLE IF EXISTS {tab_name}')
    print(f"{tab_name} is delted")


db_raw = ['apt_lease_raw', 'house_lease_raw', 'office_lease_raw', 'multi_family_lease_raw', 'apt_raw', 'commerical_raw', 'multi_family_raw', 'house_raw']
db_lease = ['apt_lease_raw', 'house_lease_raw', 'office_lease_raw', 'multi_family_lease_raw']
db_sale = ['apt_raw', 'commerical_raw', 'multi_family_raw', 'house_raw']

def column_list(table_name):
    with engine.connect() as conn:
        # 테이블이 존재하는지 확인
        inspector = inspect(conn)
        if table_name in inspector.get_table_names():
            # 테이블의 컬럼 리스트 가져오기
            columns = [column['name'] for column in inspector.get_columns(table_name)]
            print(f"Columns in table '{table_name}': {columns}")
        else:
            print(f"Table '{table_name}' does not exist.")

#column_list('apt_raw')


def preprocess(table):
    query = f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT 2000000"
    df = pd.read_sql(query, engine)

    # 년, 월, 일 칼럼을 문자열로 변환 후 합쳐서 거래일자 칼럼 생성
    df['거래일자'] = pd.to_datetime(df['년'].astype(str) + '-' + df['월'].astype(str) + '-' + df['일'].astype(str))
    df['거래월'] = df['거래일자'].dt.to_period('M')
    df['거래년'] = df['거래일자'].dt.year
    
    # 전용면적과 거래금액을 숫자형으로 변환
    df['전용면적'] = pd.to_numeric(df['전용면적'].str.replace(',', ''), errors='coerce')
    df['거래금액'] = pd.to_numeric(df['거래금액'].str.replace(',', ''), errors='coerce')
    
    # 평당금액 칼럼 생성
    df['평당금액'] = df['거래금액'] / df['전용면적']
    
    # 지역코드의 앞 두 자리를 사용하여 광역시 칼럼 생성
    df['광역시'] = df['지역코드'].astype(str).str[:2]

    # 중복데이터 제거
    return df



def remove_duplicates_and_save_unique(table_name, unique_table_name):
    with engine.connect() as conn:
        # 제거 전 행 개수 계산
        result = conn.execute(text(f"SELECT COUNT(*) FROM  {table_name} LIMIT 10000 "))
        initial_count = result.scalar()
        
        # unique_table_name이 존재하면 제거
        conn.execute(text(f"DROP TABLE IF EXISTS {unique_table_name}"))
        
        # 중복 데이터 제거 시작 시간
        start_time = time.time()

        # 중복 데이터 제거를 위한 SQL 쿼리 작성 및 새로운 테이블 생성
        query = f"""
        CREATE TABLE {unique_table_name} AS
        SELECT DISTINCT 년, 월, 일, 지역코드, 거래금액, 전용면적
        FROM {table_name}
        """
        conn.execute(text(query))

        # 제거 후 unique_table_name의 행 개수 계산
        result = conn.execute(text(f"SELECT COUNT(*) FROM {unique_table_name}"))
        final_count = result.scalar()
        
        # 중복 데이터 제거 종료 시간
        end_time = time.time()

        # 제거 작업에 걸린 시간 계산
        elapsed_time = end_time - start_time

    print(f"Data cleaning completed: {initial_count} rows in {table_name}, {final_count} unique rows in {unique_table_name}")
    print(f"Time taken for removing duplicates and creating the unique table: {elapsed_time:.2f} seconds")

# 실행
# remove_duplicates_and_save_unique('apt_raw', 'apt')

def remove_same_row(df, col_list):
    # 중복 제거 전 데이터프레임의 길이
    initial_count = len(df)
    
    # 중복 데이터 제거
    col_list = ['거래일자', '지역코드', '거래금액', '전용면적']
    df = df.drop_duplicates(subset=col_list)
    
    # 중복 제거 후 데이터프레임의 길이
    final_count = len(df)
    
    # 제거된 중복 데이터의 수
    duplicates_removed = initial_count - final_count
    print(f"제거된 중복 데이터 수: {duplicates_removed}, {initial_count}개에서 {final_count}개로")
    return df

col_list = ['거래일자', '지역코드', '거래금액', '전용면적']

def remove_outliers_using_iqr(group, column):
    """IQR 방식을 이용하여 아웃라이어를 제거하는 함수"""
    Q1 = group[column].quantile(0.25)
    Q3 = group[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    filtered_group = group[(group[column] >= lower_bound) & (group[column] <= upper_bound)]
    return filtered_group, lower_bound, upper_bound

def remove_outliers_using_log_iqr(group, column):
    """로그 스케일을 적용한 IQR 방식을 이용하여 아웃라이어를 제거하는 함수"""
    # 로그 스케일로 변환 (양수만 처리)
    group = group[group[column] > 0]  # 0 이하의 값 제거
    log_column = np.log1p(group[column])  # log1p(x) = log(x + 1)

    Q1 = log_column.quantile(0.25)
    Q3 = log_column.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    # 로그 스케일에서 아웃라이어 제거
    filtered_group = group[(log_column >= lower_bound) & (log_column <= upper_bound)]
    
    # 원래 스케일로 변환
    original_lower_bound = np.expm1(lower_bound)
    original_upper_bound = np.expm1(upper_bound)
    
    return filtered_group, round(original_lower_bound), round(original_upper_bound)


def remove_outliers(table):
    df = preprocess(table)
    
    # 그룹별로 아웃라이어 제거
    grouped = df.groupby(['광역시', '거래년'])

    # 아웃라이어를 제거한 데이터프레임과 제거된 아웃라이어 수를 기록할 데이터프레임 생성
    filtered_df = pd.DataFrame()
    outlier_counts = []
    total_outliers_removed = 0

    for name, group in grouped:
        initial_count = len(group)
        group_no_outliers, lower_bound, upper_bound = remove_outliers_using_log_iqr(group, '평당금액')
        final_count = len(group_no_outliers)
        outliers_removed = initial_count - final_count
        
        # 그룹별 아웃라이어 수 및 경계값 기록
        outlier_counts.append({
            '광역시': name[0], 
            '거래년': name[1], 

            '아웃라이어 수': outliers_removed,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound
        })
        
        # 총 제거된 아웃라이어 수 갱신
        total_outliers_removed += outliers_removed
        
        filtered_df = pd.concat([filtered_df, group_no_outliers], ignore_index=True)
    
    # 제거된 아웃라이어 수를 담은 데이터프레임 생성
    outlier_counts_df = pd.DataFrame(outlier_counts)

    # 총 제거된 아웃라이어 수 출력
    print(f"제거된 총 아웃라이어 수: {total_outliers_removed}")

    return filtered_df, outlier_counts_df

#print("Hello")

# 결과 확인
#filtered_df, outlier_counts_df = remove_outliers('apt')
#print("Filtered DataFrame:")
#print("\nOutlier Counts DataFrame:")
#print(outlier_counts_df)


#df = preprocess('apt_raw')
#df = remove_same_row(df, col_list=['거래일자', '지역코드', '거래금액', '전용면적'])
#print(df.sort_values(by='거래금액', ascending= False).head(5))

from pandas.plotting import table
def dataframe_to_image(df, file_name):
    # 새로운 Figure 생성
    fig, ax = plt.subplots(figsize=(8, 2))  # figsize 조정 가능

    # 플롯을 비활성화
    ax.axis('off')

    # 데이터프레임을 테이블 형식으로 시각화
    tbl = table(ax, df, loc='center', cellLoc='center', colWidths=[0.15]*len(df.columns))

    # 테이블 스타일 설정
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    tbl.scale(1.2, 1.2)  # 테이블 크기 조정

    # 이미지로 저장
    plt.savefig(file_name, bbox_inches='tight', pad_inches=0.1)
    plt.close()

#outlier_counts_df['lower_bound'] = outlier_counts_df['lower_bound'].astype(int)
#outlier_counts_df['upper_bound'] = outlier_counts_df['upper_bound'].astype(int)

#print(outlier_counts_df)

# 데이터프레임을 이미지로 저장
#dataframe_to_image(outlier_counts_df, '/Users/hj/Dropbox/real_estate/report/dataframe_image3.png')
#print("DataFrame has been successfully saved as an image.")


result = execute_query("SELECT * FROM conn_code LIMIT 100")
for row in result:
    print(row)

table_info()
column_list('multi_family_raw')