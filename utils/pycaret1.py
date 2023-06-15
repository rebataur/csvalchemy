import pandas as pd

data = pd.read_csv("C:\\Users\\pranjan24\\Downloads\\myview.csv")

import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pickle
# Load the stock data into a Pandas DataFrame

stock_data = data[['open','high','low','close','last','prevclose','no_trades','no_of_shrs','net_turnov','sma200']]


# Prepare the feature matrix X and the target variable y
X = stock_data[['open','high','low','last','prevclose','no_trades','no_of_shrs','net_turnov','sma200']]
y = stock_data['close']

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create a linear regression model
model = LinearRegression()

# Train the model on the training data
model.fit(X_train, y_train)

# Make predictions on the test data
y_pred = model.predict(X_test)

# Calculate the root mean squared error
rmse = mean_squared_error(y_test, y_pred, squared=False)
print('Root Mean Squared Error:', rmse)
# Save the trained model to a file
model_file = 'C:\\3Projects\\rapidiam\\models\\trained_model.pkl'
with open(model_file, 'wb') as file:
    pickle.dump(model, file)

# Load the saved model from file
with open(model_file, 'rb') as file:
    loaded_model = pickle.load(file)

# Predict the closing price for a new data point
new_data = pd.DataFrame([[150.00, 155.00, 148.50, 1000000, 160.00,155.00, 148.50, 1000000, 160.00],[150.00, 155.00, 148.50, 1000000, 160.00,155.00, 148.50, 1000000, 160.00]], columns=['open','high','low','last','prevclose','no_trades','no_of_shrs','net_turnov','sma200'])
predicted_close = model.predict(new_data).tolist()
s = str(predicted_close[0])
print(s)
