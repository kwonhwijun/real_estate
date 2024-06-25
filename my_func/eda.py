import matplotlib.pyplot as plt
import seaborn as sns

def plot_transaction_amount_histogram(df):
    # 거래금액을 10000으로 나누어 억원 단위로 변환
    df['거래금액'] = df['거래금액'] / 10000
    
    # 히스토그램을 그리기 위한 figure와 axes 생성
    plt.figure(figsize=(12, 8))

    # Seaborn의 FacetGrid를 사용하여 거래년별로 히스토그램을 그립니다.
    g = sns.FacetGrid(df, col="거래년", col_wrap=4, height=3, sharex=False, sharey=False)
    g.map(sns.histplot, "거래금액", bins=30, kde=False)

    # 그래프 제목 및 레이블 설정
    g.set_titles(col_template="{col_name}년")
    g.set_axis_labels("거래금액 (억원)", "빈도수")
    plt.suptitle('거래년별 거래금액 히스토그램 (억원 단위)', y=1.02)

    # 그래프 출력
    plt.tight_layout()
    plt.show()


#plot_transaction_amount_histogram(df)


def plot_log_histogram(df, col, bins=30):
    plt.figure(figsize=(12, 8))
    df[col] = df[col].apply(lambda x: np.log1p(x) if x > 0 else 0)  # 거래금액이 0이거나 음수일 경우 0으로 설정
    sns.histplot(df[col], bins=bins, kde=False)
    plt.xlabel(f'{col} (Log Scale)')
    plt.ylabel('Frequency')
    plt.title(f'{col} Histogram (Log Scale)')
    plt.show()

# 거래금액의 로그 스케일 히스토그램
#plot_log_histogram(df, '거래금액')