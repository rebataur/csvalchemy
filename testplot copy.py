# import base64
import psycopg2.extras
import psycopg2
import matplotlib.dates
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from io import BytesIO
import matplotlib as mpl
# mpl.use('Agg')

conn = psycopg2.connect(
    "dbname=postgres user=postgres password=postgres")

cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


def fetch_raw_query(sql):
    print("============fetch_raw_query=====================")
    print(sql)

    cursor.execute(sql)
    rows = cursor.fetchall()
    return rows


def line_plot(table_name, x_axis_column_name, y_axis_column_name,x_axis_column_dtype):

    order_by_sql = f"order by {x_axis_column_name}" if x_axis_column_dtype == 'date' else ''
    sql = f"select {x_axis_column_name},{','.join(y_axis_column_name)} from {table_name}  {order_by_sql} "

    res = fetch_raw_query(sql)
    xaxis = []

    if x_axis_column_dtype == 'date':
        for row in res:
            d = row[x_axis_column_name]
            xaxis.append(d)

    # print(xaxis)

    plt.figure(figsize=(5, 2.7), layout='constrained')

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
    plt.show()


if __name__ == "__main__":
    line_plot('sensex_meta', 'trade_date', [
              'close', 'sma50'], 'date')
