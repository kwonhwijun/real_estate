import pandas as pd
df = pd.read_csv('/Users/hj/Dropbox/real_estate/model/jini/2024-06-12/아파트_행정동별_지니계수.csv')
df[df['년'] == 2023].거래수.astype(int).sum()



from sqlalchemy import create_engine, text
engine = create_engine(f"sqlite:////Users/hj/Dropbox/real_estate/data/api/db/RealEstate.db")

with engine.connect() as conn:
    query = text(f'''SELECT COUNT(*)
                 FROM TEMP
                 ''')
    df = pd.read_sql_query(query, conn)

df