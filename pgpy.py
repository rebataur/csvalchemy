
drop function get_table_meta(text);
CREATE or replace FUNCTION get_table_meta(table_name text)
  RETURNS text
AS $$

sql = f'''
        SELECT column_name 
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND table_schema = 'public';
    '''
res = plpy.execute(sql)
return res

col_dict = {}
for r in res:
	col_dict[r['column_name']] = r['data_type']
return col_dict
	
$$ LANGUAGE plpython3u;
select get_table_meta('sensex_meta')



drop function line_plot(text,text, text[],text);

CREATE or replace FUNCTION line_plot(table_name text, x_axis_column_name text, y_axis_column_name text[], x_axis_column_dtype text )
  RETURNS bytea
AS $$
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
sql = f"select {x_axis_column_name},{','.join(y_axis_column_name)} from {table_name}   {order_by_sql}"

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
plt.figure().clear()

buffer.seek(0)
image_png = buffer.getvalue()
buffer.close()




return image_png

# graphic = base64.b64encode(image_png)
# graphic = graphic.decode('utf-8')

# return graphic

$$ LANGUAGE plpython3u;


select line_plot('sensex_meta','trade_date', '{"close","sma50"}','date')

