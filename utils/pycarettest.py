### load sample dataset from pycaret dataset module
from pycaret.datasets import get_data
data = get_data('insurance')
print(data.info())
# import pycaret regression and init setup
from pycaret.regression import save_model,load_model,predict_model,setup,finalize_model
# import RegressionExperiment and init the class
from pycaret.regression import RegressionExperiment
exp = RegressionExperiment()
s = setup(data, target =  'charges', session_id = 123)
# init setup on exp
exp.setup(data, target = 'charges', session_id = 123)

# compare baseline models
best = exp.compare_models()
 	

best_model = finalize_model(best)

save_model(best_model, 'C:\\3Projects\\rapidiam\\models\\my_first_model')

# load model
loaded_from_disk = load_model('C:\\3Projects\\rapidiam\\models\\my_first_model')
print(loaded_from_disk)
# copy data and drop charges
new_data = data.copy()
# new_data.drop('charges', axis=1, inplace=True)
new_data.head()
# predict model on new_data
predictions = predict_model(loaded_from_disk, data = new_data)
print(predictions.head().to_json())
