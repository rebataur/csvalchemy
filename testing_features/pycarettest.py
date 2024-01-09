from pycaret.datasets import get_data
data = get_data('insurance')

from pycaret.regression import setup,compare_models,predict_model,save_model,load_model
s = setup(data, target='charges', session_id=123 ,verbose=False)
best = compare_models(verbose=False)
print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
print(type(best))
exit()
# evaluate_model(best)
pred_holdout = predict_model(best,verbose=False)

new_data = data.copy().drop('charges', axis=1)

new_data = data.copy()
new_data.drop('charges', axis=1, inplace=True)
predictions = predict_model(best,data=new_data,verbose=False)
print("predicting")
print(predictions)
save_model(best,'best_pipeline',verbose=False)