create or replace FUNCTION predict_linear_regression(model_name text,target text,model_refresh_in_days int, model_data_sql text,execution_mode text)
  RETURNS text
AS $$
import os
from datetime import datetime
from pycaret.regression import save_model,load_model,predict_model,setup
from pycaret.regression import RegressionExperiment
import pandas as pd

def train_model():    
    plpy.info("train models")
    ## load sample dataset from pycaret dataset module
    dat = plpy.execute(model_data_sql)
    plpy.notice(type(dat[0:]))
    data = pd.DataFrame(dat[0:])
    data = data[['pe','close']]
    # return str(data.head().to_csv())
    s = setup(data, target = target, session_id = 123)
    
    exp = RegressionExperiment()
    exp.setup(data, target = target, session_id = 123)
    
    best = exp.compare_models()
    save_model(best, f"C:\\3Projects\\rapidiam\\models\\{model_name}")


if execution_mode == 'datascience_training':
    if not os.path.exists(f"C:\\3Projects\\rapidiam\\models\\{model_name}"):
        train_model()
        
    else:
        cr_date =  oc.getmtime(f"C:\\3Projects\\rapidiam\\models\\{model_name}")
        delta_days = datetime.today() - datetime.fromtimestamp(cr_date)
        if delta_days > model_refresh_in_days:
            train_model()
            
    return execution_mode

elif execution_mode == 'datascience_testing':
    train_model()
    return execution_mode
    
elif execution_mode == 'datascience_execution':
   
    loaded_model = load_model(f"C:\\3Projects\\rapidiam\\models\\{model_name}")
    # return str(loaded_model)
    dat = plpy.execute(model_data_sql)
    plpy.notice(type(dat[0:]))
    data = pd.DataFrame(dat[0:])
    data = data[['pe']]
    predictions = predict_model(loaded_model[0], data=data)
    return "ok"

    
    
else:
    return "no mode provided"




$$ LANGUAGE plpython3u immutable;