sql = '''
            WITH test AS(
            SELECT
                trade_date,
                code,
                CLOSE,
                name,
                AVG(round(CLOSE)) OVER(PARTITION BY code
            ORDER BY
                trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma200
            FROM
                BSE
            ORDER BY
                trade_date DESC 
            
                
        ), lmt as(
                
        select close,sma200,trade_date,name
        FROM
            test
        WHERE
            code = 532540
            limit 200
        )select array_agg(name) as namearr,array_agg(close) as closearr,array_agg(sma200) as smaarr,array_agg(trade_date) as trade_datearr from lmt 

    '''

res = plpy.execute(sql)

name = res[0]['namearr'][0]
closearr = res[0]['closearr']
smaarr = res[0]['smaarr']
tradearr = res[0]['trade_datearr']

from io import BytesIO
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
# import numpy as np
plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True
# x = np.linspace(0, 2, 100)  # Sample data.
plt.figure(figsize=(10, 5), layout='constrained')
plt.plot(tradearr, closearr, label='close')  # Plot some data on the (implicit) axes.
plt.plot(tradearr,smaarr, label='sma200')  # etc.
plt.xlabel('close, sma200')
plt.ylabel('trade date')
plt.title(name)
plt.legend()
buffer = BytesIO()
plt.savefig(buffer, format="png")