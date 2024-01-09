# import pycaret.classification as cp

# import pickle
# from pycaret.datasets import get_data
# juice = get_data('juice')

# # trail1 = cp.setup(data = cancer_data, target = "Class", silent = True, n_jobs=None)
# exp_name = cp.setup(data = juice,  target = 'Purchase',  n_jobs=None)

# # Create Model*
# et = cp.create_model("et", verbose=False)


# #To improve our model further, we can tune hyper-parameters using tune_model function.
# #We can also optimize tuning based on an evaluation metric. As our choice of metric is F1-score, lets optimize our algorithm!*

# tuned_et = cp.tune_model(et, optimize = "F1", verbose=False)


# #The finalize_model() function fits the model onto the complete dataset.
# #The purpose of this function is to train the model on the complete dataset before it is deployed in production*

# final_model = cp.finalize_model(tuned_et)

# # Before saving the model to the DB table, convert it to a binary object*

# trained_model = []
# # prep = cp.get_config("prep_pipe")
# # trained_model.append(prep)
# trained_model.append(final_model)
# trained_model = pickle.dumps(trained_model)



import pycaret.classification as cp

import pickle
from pycaret.datasets import get_data
f = open('best_pipeline.pkl','rb')
cancer_model = pickle.loads(f.read())
juice = get_data('juice')

new_data = juice.copy().drop('Purchase', axis=1)
predictions = cp.predict_model(cancer_model, data=juice)

OutputDataSet = predictions

print(OutputDataSet)