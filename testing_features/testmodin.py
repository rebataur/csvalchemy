import modin.pandas as pd
from modin.db_conn import ModinDatabaseConnection
con = ModinDatabaseConnection(
    'psycopg2',
    host='localhost',
    dbname='postgres',
    user='postgres',
    password='postgres')
df = pd.read_sql('''with cte_0 as ( select pe_ratio,"growwportfolio_code","growwportfolio_scripname","growwportfolio_market","growwportfolio_outlook","growwportfolio_file_name","growwportfolio_pe",de_ratio,"bhavcopy_high","bhavcopy_low","bhavcopy_last","bhavcopy_prevclose","bhavcopy_no_trades","bhavcopy_no_of_shrs","bhavcopy_net_turnov","bhavcopy_tdcloindi","bhavcopy_open","bhavcopy_sc_group","bhavcopy_sc_type","bhavcopy_sc_name","bhavcopy_sc_code","bhavcopy_file_name","bhavcopy_close","predict_close_file_name","predict_close_predicted_close","predict_close_growwportfolio_code" from growwportfolio  left join bhavcopy on growwportfolio.growwportfolio_code = bhavcopy.bhavcopy_sc_code left join predict_close on growwportfolio.growwportfolio_code = predict_close.predict_close_growwportfolio_code ),cte_1 as ( select *,convert_str_to_date(bhavcopy_file_name) as trade_date from cte_0),cte_2 as ( select *,avg(bhavcopy_close) over(partition by growwportfolio_code order by trade_date asc rows between 200 preceding and current row ) as sma200 from cte_1),cte_3 as ( select *,(bhavcopy_close-sma200)/sma200*100  as close_diffto_sma200 from cte_2),cte_4 as ( 
select *,pe_ratio+de_ratio*100+close_diffto_sma200 as total_weight from cte_3) select pe_ratio,growwportfolio_code,growwportfolio_scripname,growwportfolio_market,growwportfolio_outlook,growwportfolio_pe,de_ratio,bhavcopy_file_name,bhavcopy_close,predict_close_predicted_close,trade_date,sma200,close_diffto_sma200,total_weight from cte_4 where  trade_date = '2023-06-13' ''',
            con,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        chunksize=None)
print(df.head())