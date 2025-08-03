#%%
#feature importance plot - include all fields, convert to 
import pandas as pd
import numpy as np
import xgboost as xgb
from pathlib import Path
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import mean_squared_error, r2_score, accuracy_score
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV,KFold,cross_val_score, train_test_split
predictors = ['u10', 'v10', 'd2m', 't2m', 'msl', 'sp', 'lai_hv', 'lai_lv', 'tp', 'ssr', 'Fall','Spring','Summer','Winter']
#convert winter = 1, spring = 2, summer = 3, fall = 4
target1 = 'size (acres)'
target2 = 'fire_spread (acres/day)'
target3 = 'duration'
trainyears = range(2020,2023)
testyears = range(2023,2025)
#%%
workingdatasetpath = Path(r"C:\Users\Azeem\Documents\MS\FInal Proj\Data\Working Dataset\Wildfire_Weather_2020_2024.csv")
pickleparentdir = r'C:\Users\Azeem\Documents\MS\FInal Proj\Data'
df = pd.read_csv(workingdatasetpath)
df_encoded = pd.get_dummies(df['startdateseason'])
df = pd.concat([df,df_encoded], axis=1)
train = df[df['startdateyear'].isin(trainyears)]
test = df[df['startdateyear'].isin(testyears)]

X_train = train[predictors]
#wont work with datetimes
y1_train = train[target1]
y2_train = train[target2]
y3_train = train[target3]

y1_test = test[target1]
y2_test = test[target2]
y3_test = test[target3]

X1_rand_train, X1_rand_test, y1_rand_train, y1_rand_test = train_test_split(df[predictors], df[target1], random_state=42)
X2_rand_train, X2_rand_test, y2_rand_train, y2_rand_test = train_test_split(df[predictors], df[target2], random_state=42)
X3_rand_train, X3_rand_test, y3_rand_train, y3_rand_test = train_test_split(df[predictors], df[target3], random_state=42)
#%%
def tune_and_evaluate_rf(X, y, n_iter=5, random_state=42):

    kf = KFold(n_splits=5, shuffle=True, random_state=random_state)

    param_dist = {'n_estimators': [100, 200],'max_depth': [10, 20],'min_samples_split': [2, 5],'min_samples_leaf': [1],'max_features': ['sqrt'],'bootstrap':[True]}
    
    model = RandomForestRegressor(random_state=random_state)
    random_search = RandomizedSearchCV(
        estimator=model,
        param_distributions=param_dist,
        n_iter=n_iter,
        scoring='r2',
        cv=kf,
        verbose=1,
        random_state=random_state,
        n_jobs=-1,
        refit=True
    )

    random_search.fit(X, y)
    best_params = random_search.best_params_
    print(f"Best hyperparameters: {best_params}")

    best_model = RandomForestRegressor(**best_params, random_state=random_state)

    r2_scores = cross_val_score(best_model, X, y, cv=kf, scoring='r2')
    neg_mse_scores = cross_val_score(best_model, X, y, cv=kf, scoring='neg_mean_squared_error')
    rmse_scores = np.sqrt(-neg_mse_scores)
    resultdict = {'best_params': best_params,'r2_scores': r2_scores,'mse_scores': -neg_mse_scores,'rmse_scores': rmse_scores}
    best_model = random_search.best_estimator_

    print(f"5-fold CV Mean R²: {r2_scores.mean():.4f}")
    print(f"5-fold CV Mean MSE: {-neg_mse_scores.mean():.4f}")
    print(f"5-fold CV Mean RMSE: {rmse_scores.mean():.4f}")

    return resultdict,best_model


# model1_results, model1 = tune_and_evaluate_rf(X_train, y1_train)
# model2_results, model2 = tune_and_evaluate_rf(X_train, y2_train)
# model3_results, model3 = tune_and_evaluate_rf(X_train, y3_train)

model1_results, model1 = tune_and_evaluate_rf(X1_rand_train, y1_rand_train)
model2_results, model2 = tune_and_evaluate_rf(X2_rand_train, y2_rand_train)
model3_results, model3 = tune_and_evaluate_rf(X3_rand_train, y3_rand_train)
# %%
def tune_and_evaluate_xgb(X, y, cv_folds=5, n_iter=5, random_state=42):

    kf = KFold(n_splits=cv_folds, shuffle=True, random_state=random_state)

    param_dist = {
        'n_estimators': [100, 200, 300],
        'max_depth': [3, 6, 10],
        'learning_rate': [0.01, 0.1, 0.2],
        'subsample': [0.7, 0.8, 1.0],
        'colsample_bytree': [0.7, 0.8, 1.0],
        'gamma': [0, 1, 5],
        'reg_alpha': [0, 0.1, 1],
        'reg_lambda': [1, 1.5, 2]
    }

    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        random_state=random_state,
        n_jobs=-1
    )

    random_search = RandomizedSearchCV(
        estimator=model,
        param_distributions=param_dist,
        n_iter=n_iter,
        scoring='r2',
        cv=kf,
        verbose=1,
        random_state=random_state,
        n_jobs=-1,
        refit=True
    )

    random_search.fit(X, y)
    best_params = random_search.best_params_
    print(f"Best hyperparameters: {best_params}")

    best_model = xgb.XGBRegressor(**best_params, objective='reg:squarederror', random_state=random_state, n_jobs=-1)

    r2_scores = cross_val_score(best_model, X, y, cv=kf, scoring='r2')
    neg_mse_scores = cross_val_score(best_model, X, y, cv=kf, scoring='neg_mean_squared_error')
    rmse_scores = np.sqrt(-neg_mse_scores)

    print(f"5-fold CV Mean R²: {r2_scores.mean():.4f}")
    print(f"5-fold CV Mean MSE: {-neg_mse_scores.mean():.4f}")
    print(f"5-fold CV Mean RMSE: {rmse_scores.mean():.4f}")

    return {
        'best_params': best_params,
        'r2_scores': r2_scores,
        'mse_scores': -neg_mse_scores,
        'rmse_scores': rmse_scores
    }, random_search.best_estimator_

model4_results, model4  = tune_and_evaluate_xgb(X1_rand_train, y1_rand_train)
model5_results, model5 = tune_and_evaluate_xgb(X2_rand_train, y2_rand_train)
model6_results, model6 = tune_and_evaluate_xgb(X3_rand_train, y3_rand_train)
# %%
#now create a random forest using returned best params

params_1 = model1_results['best_params']
params_2 = model2_results['best_params']
params_3 = model3_results['best_params']



modelinfo = [
    (model1, 'Size (RF)'),
    (model2, 'Spread (RF)'),
    (model3, 'Duration (RF)'),
    (model4, 'Size (XGB)'),
    (model5, 'Spread (XGB)'),
    (model6, 'Duration (XGB)')
]

def plotimportanceinfoforeachmodel(modelinfo):
    import matplotlib.pyplot as plt
    import pandas as pd
    for info in modelinfo:
        model = info[0]
        title = info[1]
        feature_importance_array = model.feature_importances_
        feature_names = model.feature_names_in_
        df = pd.DataFrame({'Feature': feature_names, 'Gini Importance': feature_importance_array}).sort_values('Gini Importance', ascending=False)
        plt.barh(df['Feature'], df['Gini Importance'])
        plt.xlabel('Importance')
        plt.title(f'Feature Importances (Gini/Impurity) - {title}')
        plt.show()

plotimportanceinfoforeachmodel(modelinfo)
#%%
modelinfo = [
    (model1, X1_rand_test, y1_rand_test, 'Size (RF)'),
    (model2, X2_rand_test, y2_rand_test, 'Spread (RF)'),
    (model3, X3_rand_test, y3_rand_test, 'Duration (RF)'),
    (model4, X1_rand_test, y1_rand_test, 'Size (XGB)'),
    (model5, X2_rand_test, y2_rand_test, 'Spread (XGB)'),
    (model6, X3_rand_test, y3_rand_test, 'Duration (XGB)')
]

def plotmeandecreaseinaccuracyforeachmodel(modelinfo):
    import matplotlib.pyplot as plt
    from sklearn.inspection import permutation_importance
    import pandas as pd
    for model_set in modelinfo:
        model = model_set[0]
        X_test = model_set[1]
        y_test = model_set[2]
        title = model_set[-1]
        permut = permutation_importance(estimator=model,X=X_test,y=y_test.to_numpy(),random_state=42,scoring='r2')
        df = pd.DataFrame({'Feature': X_test.columns,'Mean Decrease in Accuracy': permut.importances_mean}).sort_values('Mean Decrease in Accuracy', ascending=False)
        plt.barh(df['Feature'], df['Mean Decrease in Accuracy'])
        plt.xlabel('Importance')
        plt.title(f'Mean Decrease in Accuracy (Permuatation) - {title}')
        plt.show()
plotmeandecreaseinaccuracyforeachmodel(modelinfo)


# %%


def evaluate_on_test(model, X_test, y_test, label) -> dict:
    from sklearn.metrics import PredictionErrorDisplay
    import matplotlib.pyplot as plt
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)
    result_dict = {'r2': r2, 'mse': mse, 'rmse': rmse, 'y_pred':y_pred,'y_test':y_test}

    print(f"\nTest Set Evaluation for {label}:")
    print(f"R²: {r2:.4f}")
    print(f"MSE: {mse:.4f}")
    print(f"RMSE: {rmse:.4f}")
    display = PredictionErrorDisplay(y_true=y_test,y_pred=y_pred)
    display.plot()
    plt.title(f'{label} residual plot')
    plt.show()

    return result_dict

Xtest = test[predictors]

test_eval_1 = evaluate_on_test(model1, X1_rand_test, y1_rand_test, label=target1)
test_eval_2 = evaluate_on_test(model2, X2_rand_test, y2_rand_test, label=target2)
test_eval_3 = evaluate_on_test(model3, X3_rand_test, y3_rand_test, label=target3)



#%%
params_1 = model1_results['best_params']
params_2 = model2_results['best_params']
params_3 = model3_results['best_params']

param_list = [(params_1, "sizeparams.pkl"),(params_2, "spreadparams.pkl"),(params_3,"durationparams.pkl")]
def exportmodelparampikl(param_list):
    import pickle
    for param in param_list:
        with open(Path(pickleparentdir).joinpath(param[1]),'wb') as w:
            pickle.dump(param[0],w)
exportmodelparampikl(param_list)
# %%
