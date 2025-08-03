#%%
import pandas as pd
import numpy as np
import xgboost as xgb
from pathlib import Path
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV,KFold,cross_val_score, train_test_split

input_2020 = r"C:\Users\Azeem\Documents\MS\FInal Proj\Data\Working Dataset\Wildfire_Weather_2020_2024.csv"
output_2020 = r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\CompletedModels\WildfireAtlas2020_2024_predictionvsactual.csv'
modelparamspath = Path(r'C:\Users\Azeem\Documents\MS\FInal Proj\Data')
model_output_path = r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\CompletedModels'
input_2025 = r"C:\Users\Azeem\Documents\MS\FInal Proj\Data\WFIGS 2025\matcheddata.csv"
output_2025 = r'C:\Users\Azeem\Documents\MS\FInal Proj\Data\WFIGS2025_predictions.csv'

#train model with best params against entire dataset for production, then predict against WfIGS Data
model_df = pd.read_csv(input_2020)
outputpath = output_2020
predictors = ['u10', 'v10', 'd2m', 't2m', 'msl', 'sp', 'lai_hv', 'lai_lv', 'tp', 'ssr', 'Spring','Winter', "Fall","Summer"]
target1 = 'size (acres)'
target2 = 'fire_spread (acres/day)'
target3 = 'duration'
match_dict = {'size':target1,'spread':target2,'duration':target3}
def get_season(month):
    if month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    elif month in [9, 10, 11]:
        return 'Fall'
    else:
        return 'Winter'

def encodedfs(df):
    df['startdateseason'] = df['startdatemonth'].map(get_season)
    encodeddf = pd.get_dummies(df['startdateseason'])
    df = pd.concat([df,encodeddf],axis=1)
    return df

model_df = encodedfs(model_df)



def grabmodelparams(paramspath):
    import pickle
    param_list = []
    for x in paramspath.glob('*.pkl'):
        with open(x,'rb') as f:
            params = pickle.load(f)
            param_list.append({x.stem:params}) 
    return param_list       
model_params = grabmodelparams(modelparamspath)
#%%

def createproductionmodels(model_info_list,full_df):
    import re
    modellist = []
    rematch = '^.+?(?=params)'
    full_df_X = full_df[predictors]
    for model_info in model_info_list:
        modelname = re.match(rematch,list(model_info.keys())[0])[0]
        label = f"{modelname}params"
        target = match_dict[modelname]
        full_df_y = full_df[target]
        model = RandomForestRegressor(n_estimators=model_info[label]['n_estimators'],min_samples_split=model_info[label]['min_samples_split'], min_samples_leaf=model_info[label]['min_samples_leaf'],max_features=model_info[label]['max_features'],max_depth=model_info[label]['max_depth'],bootstrap=model_info[label]['bootstrap'])
        model.fit(full_df_X,full_df_y)
        pred_col = f"predicted_{modelname}"
        full_df[pred_col] = model.predict(full_df_X)
        modellist.append((target,model))
    full_df.to_csv(outputpath, index=False)
    return modellist
models = createproductionmodels(model_params,model_df)
#%%




def exportmodelpikl(modellist):
    import pickle
    Path(model_output_path)
    for model in modellist:
        with open(Path(model_output_path).joinpath(f'{model[0].split(" ")[0]}_model.pkl'),'wb') as w:
            pickle.dump(model[1],w)

exportmodelpikl(models)
#%%

def predict2025values(modellist):
    df = pd.read_csv(Path(input_2025))
    #grab predictors with loc 
    df = encodedfs(df)
    for season in ['Winter','Spring','Summer','Fall']:
        if season not in df.columns:
            df[season] = False
    df = df[(df['DiscoveryAcres'] >= 1000) | (df['IncidentSize'] >= 1000)]
    df_X = df[predictors]
    #add check to add one hot encoded field for missing seasons, if missing will be FALSE
    for model in modellist:
        colname = f"{model[0]}_predicted"
        df[colname] = model[1].predict(df_X)
    print("All Predicted Values Added to DF")
    #round durration to nearest day
    df['duration'] = df['duration_predicted'].round(0)
    df.to_csv(output_2025, index=False)

    



# %%
