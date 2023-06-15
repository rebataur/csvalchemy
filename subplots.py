


import matplotlib.pyplot as plt
import numpy as np
import psycopg2

# Create your views here.

PG_NAME = 'postgres'
PG_USER = 'postgres'
PG_PWD = 'postgres'
PG_HOST = 'localhost'
PG_PORT  = 5432


conn = psycopg2.connect(dbname=PG_NAME, user=PG_USER, password=PG_PWD,host=PG_HOST,port=PG_PORT)
cur = conn.cursor()

# get distinct codes from portfolio

sql = "select distinct(security_code) from growportfolioview limit 50"
cur.execute(sql)
res = cur.fetchall()

count = 0
rows = int(len(res) / 2)

# for r in res:
#     sql = f"select close,sma200,trade_date,company from test where scrip_code = {r[0]} order by trade_date limit 10"
#     cur.execute(sql)
#     res_val = cur.fetchall()
#     print(res_val)
    
#     x_axis = [x[2] for x in res_val]
#     y_close = [float(x[0]) for x in res_val]
#     y_sma200 = [float(x[1]) for x in res_val]
#     title = res_val[0][3]  

#     count += 1
#     plt.subplot(rows, 3, count)
#     plt.plot(x_axis,y_close)
#     plt.plot(x_axis,y_sma200)
#     plt.title(title)

#     if count == 29:
#         break

# plt.show()



from matplotlib import pyplot as plt
# plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True


stocks = []
close_price = []
sma200 = []

for r in res:
    sql = f"select close,sma200,trade_date,scrip_name from growportfolioview where security_code = {r[0]} order by trade_date desc limit 1"
    cur.execute(sql)
    res_val = cur.fetchone()
    print(res_val)    
    stocks.append(res_val[3])
    close_price.append(res_val[0])
    sma200.append(res_val[0]-res_val[1])



b1 = plt.barh(stocks, close_price, left=sma200,color="blue")

b2 = plt.barh(stocks, sma200,  color="lightblue")

plt.legend([b1, b2], ["close", "sma200"], title="Stocks", loc="upper right")

plt.show()

exit()


from io import BytesIO
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.dates as mdates
import matplotlib as mpl

# mpl.use('Agg')


plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True



for r in res:
    sql = f"select close,sma200,trade_date,company from test where scrip_code = {r[0]} and trade_date > now() - interval '21 day' order by trade_date"
    cur.execute(sql)
    res_val = cur.fetchall()
    # print(res_val)
    
    x_axis = [x[2] for x in res_val]
    y_close = [float(x[0]) for x in res_val]
    y_sma200 = [float(x[1]) for x in res_val]
    title = res_val[0][3]  

    count += 1
    plt.subplot(rows, 2, count)
    plt.plot(x_axis,y_close)
    plt.plot(x_axis,y_sma200)
    plt.title(title)
    plt.legend()
    ####### Use the below functions #######
    dtFmt = mdates.DateFormatter('%b') # define the formatting
    plt.gca().xaxis.set_major_formatter(dtFmt) # apply the format to the desired axis
    # plt.legend()

plt.show()

# buffer = BytesIO()
# plt.savefig(buffer, format="png")
# plt.clf()   
# plt.close()
# plt.figure().clear()

# buffer.seek(0)
# image_png = buffer.getvalue()
# buffer.close()




# return image_png

# graphic = base64.b64encode(image_png)
# graphic = graphic.decode('utf-8')

# return graphic

