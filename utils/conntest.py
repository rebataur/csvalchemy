import psycopg2

PG_NAME = 'postgres'
PG_USER = 'postgres'
PG_PWD = 'postgres'
PG_HOST = 'localhost'
PG_PORT  = 5432

sql = "with cte_0 as ( select low,close,last,prevclose,no_trades,no_of_shrs,sc_name,net_turnov,sc_group,sc_type,tdcloindi,bhavcopy_file_name,sc_code,open,high from bhavcopy) select * from cte_0 limit 1"
sql = "with cte_0 as ( select scripname, market, outlook, growwportfolio_file_name, pe, code, low, close, last, prevclose, no_trades, no_of_shrs, sc_name, net_turnov, sc_group, sc_type, tdcloindi, bhavcopy_file_name, sc_code, open, high from growwportfolio left join bhavcopy on growwportfolio.code = bhavcopy.sc_code ), cte_1 as ( select *, convert_str_to_date(bhavcopy_file_name) as trade_date from cte_0), cte_2 as ( select *, avg(close) over(partition by code order by trade_date asc rows between 200 preceding and current row) as sma200, rsi_sma(close) over(partition by code order by trade_date asc rows between 14 preceding and current row) as rsi_sma_14 from cte_1) , cte_3 as ( select *, RANK() OVER (PARTITION BY code ORDER BY trade_date desc) AS rnk from cte_2 ) select distinct * from cte_3 where outlook = 'LONGTERM' and close < sma200 and rsi_sma_14 < 30 and pe < 22 order by trade_date desc"

conn = psycopg2.connect(dbname=PG_NAME, user=PG_USER, password=PG_PWD,host=PG_HOST,port=PG_PORT)
cur = conn.cursor()
cur.execute(sql)
res = cur.fetchone()
print(res)


import asyncio
import asyncpg

async def run():
    conn = await asyncpg.connect(user='postgres', password='postgres',
                                 database='postgres', host='127.0.0.1')
    values = await conn.fetch(sql)
    print(values[0])
    await conn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())