from pycaret.datasets import get_data
data = get_data('insurance')
print(data.head())
from pycaret.regression import RegressionExperiment
s = RegressionExperiment()

s = s.setup(data, target='charges', session_id=123 ,verbose=False)
best = s.compare_models(verbose=False)
# evaluate_model(best)
pred_holdout = s.predict_model(best,verbose=False)

new_data = data.copy().drop('charges', axis=1)

new_data = data.copy()
new_data.drop('charges', axis=1, inplace=True)
predictions = s.predict_model(best,data=new_data,verbose=False)
print("predicting")
print(predictions)
s.save_model(best,'best_pipeline',verbose=False)