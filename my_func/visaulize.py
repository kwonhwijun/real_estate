from my_func import gini
import matplotlib.pyplot as plt
import seaborn as sns

import matplotlib.font_manager as fm

RA = gini.RealEstateAnalyzer()
RA.load_data('house_raw')
RA.remove_duplicates_df()
RA.preprocess()
RA.map_hdong()
stat = RA.avg_price_year()

def set_korean_font():
    font_path = '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'  # 시스템에 설치된 한글 폰트 경로
    font_name = fm.FontProperties(fname=font_path).get_name()
    plt.rc('font', family=font_name)
    plt.rcParams['axes.unicode_minus'] = False

set_korean_font()



def plot_gini_by_specific_region(df, region_name):
    specific_region_df = df[df['시도명'] == region_name]
    
    plt.figure(figsize=(12, 8))
    sns.barplot(data=specific_region_df, x='시군구명', y='거래금액지니계수')
    plt.title(f'{region_name}의 시군구별 거래금액 지니계수')
    plt.xticks(rotation=45)
    plt.ylabel('거래금액 지니계수')
    plt.show()

# 사용 예시
plot_gini_by_specific_region(stat, '서울특별시')

def plot_gini_summary_by_region(df):
    summary_df = df.groupby('시도명')['거래금액지니계수'].agg(['mean', 'median', 'max']).reset_index()
    
    plt.figure(figsize=(12, 8))
    summary_df = summary_df.melt(id_vars='시도명', var_name='통계량', value_name='지니계수')
    sns.barplot(data=summary_df, x='시도명', y='지니계수', hue='통계량')
    plt.title('시도별 거래금액 지니계수 요약')
    plt.xticks(rotation=45)
    plt.ylabel('지니계수')
    plt.show()

# 사용 예시
plot_gini_summary_by_region(stat)