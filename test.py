# import base64

from io import BytesIO
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib as mpl
mpl.use('Agg')


plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True


order_by_sql = f"order by {x_axis_column_name}" if x_axis_column_dtype == 'date' else ''
sql = f"select {x_axis_column_name},{','.join(y_axis_column_name)} from {table_name}  where scrip_code = 532215 {order_by_sql}"

res = plpy.execute(sql)
xaxis = []
if x_axis_column_dtype == 'date':
    for row in res:
        dstr = row[x_axis_column_name]
        ddate = datetime.strptime(dstr, '%Y-%m-%d')
        xaxis.append(ddate)


# print(xaxis)
count = 0
for col in y_axis_column_name:
    yaxis = []
    for row in res:
        d = row[col]
        yaxis.append(d)

    plt.plot(xaxis, yaxis, label=col)

plt.xlabel(x_axis_column_name)
plt.ylabel("Close Params")
plt.title("Simple Plot")
plt.legend()


buffer = BytesIO()
plt.savefig(buffer, format="png")
plt.clf()   
plt.close()

buffer.seek(0)
image_png = buffer.getvalue()
buffer.close()




return image_png

# graphic = base64.b64encode(image_png)
# graphic = graphic.decode('utf-8')

# return graphic
