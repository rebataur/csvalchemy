# Code source: Jaques Grobler
# License: BSD 3 clause
sql = '''
 with cte_0 as ( select "scripname","code","market","outlook","growwportfolio_file_name","pe","open","high","low","close","last","prevclose","no_trades","no_of_shrs","net_turnov","sc_name","sc_group","sc_type","tdcloindi","bhavcopy_file_name","sc_code" from growwportfolio left join bhavcopy on growwportfolio.code = bhavcopy.sc_code ),cte_1 as ( select *,convert_str_to_date(bhavcopy_file_name) as trade_date from cte_0),cte_2 as ( select *,avg(close) over(partition by code order by trade_date asc rows between 200 preceding and current row ) as sma200 from cte_1) select * from cte_2 order by trade_date asc
'''
import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import psycopg2

conn = psycopg2.connect('user=postgres password=postgres')
cur = conn.cursor()
cur.execute(sql)
res = cur.fetchall()

# Load the diabetes dataset
diabetes_X, diabetes_y = datasets.load_diabetes(return_X_y=True)
print(diabetes_X)
# Use only one feature
diabetes_X = diabetes_X[:, np.newaxis, 2]
print(diabetes_X)
# Split the data into training/testing sets
diabetes_X_train = diabetes_X[:-20]
diabetes_X_test = diabetes_X[-20:]

# Split the targets into training/testing sets
diabetes_y_train = diabetes_y[:-20]
diabetes_y_test = diabetes_y[-20:]

# Create linear regression object
regr = linear_model.LinearRegression()

# Train the model using the training sets
regr.fit(diabetes_X_train, diabetes_y_train)

# Make predictions using the testing set
diabetes_y_pred = regr.predict(diabetes_X_test)
print(diabetes_y_pred)
# The coefficients
print("Coefficients: \n", regr.coef_)
# The mean squared error
print("Mean squared error: %.2f" % mean_squared_error(diabetes_y_test, diabetes_y_pred))
# The coefficient of determination: 1 is perfect prediction
print("Coefficient of determination: %.2f" % r2_score(diabetes_y_test, diabetes_y_pred))

# Plot outputs
plt.scatter(diabetes_X_test, diabetes_y_test, color="black")
plt.plot(diabetes_X_test, diabetes_y_pred, color="blue", linewidth=3)

plt.xticks(())
plt.yticks(())

plt.show()