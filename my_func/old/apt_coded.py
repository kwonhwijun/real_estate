from sqlalchemy import create_engine, text
import pandas as pd
eng = create_engine('sqlite:////Users/hj/Dropbox/real_estate/data/api/db/RealEstate.db')
with eng.connect() as conn:
    #code = pd.read_sql_query('SELECT * FROM conn_code', con = conn)
    #df = pd.read_sql_query('SELECT * FROM apt_coded LIMIT 10009', con = conn)

    #conn.execute(text(query))
    #temp = pd.read_sql_query('SELECT * FROM TEMP', con = conn)
    #df = pd.read_sql_query('SELECT * FROM apt_coded', con = conn)
    result = conn.execute(text("SELECT COUNT(*) FROM apt_coded"))
    for row in result:
        print(row)

query = '''
            CREATE TABLE TEMP AS 
            SELECT a.*, c.읍면동명 AS 읍면동명
            FROM apt_coded a
            LEFT JOIN (
                SELECT DISTINCT 행정동코드, 읍면동명
                FROM conn_code
            ) c ON a.행정동코드 = c.행정동코드
            '''