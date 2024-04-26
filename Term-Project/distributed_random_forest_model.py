import pandas as pd
import sys
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.feature_selection import RFE
import numpy as np

def preprocess_data(model_data):
    model_data['publishedAt'] = pd.to_datetime(model_data['publishedAt'])

    start_date = pd.Timestamp('2020-01-01')
    mid_date = pd.Timestamp('2023-01-01')

    # Separate data based on publishedAt time
    train_data = model_data[(model_data['publishedAt'] >= start_date) & (model_data['publishedAt'] < mid_date)]
    test_data = model_data[(model_data['publishedAt'] >= mid_date)]

    return train_data, test_data

def train_model(X_train, y_train):
    model = RandomForestRegressor(n_estimators=150, random_state=42)
    model.fit(X_train, y_train)
    return model

def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)

    return mae, mse, rmse, y_pred

def select_features(train_data):
    features = ['view_count', 'likes', 'engagement_rate', 'like_dislike_ratio', 
            'comment_view_ratio', 'dislikes_per_comment', 'days_since_publication', 
            'likes_per_day', 'comments_per_day']
    # Split the data into features and target variable
    X_train = train_data[features]
    y_train = train_data['view_velocity']

    # Initialize RFE with RandomForestRegressor
    rfe = RFE(estimator=RandomForestRegressor(n_estimators=150, random_state=42), n_features_to_select=5)

    # Fit RFE to training data
    rfe.fit(X_train, y_train)

    # Get selected features
    selected_features = X_train.columns[rfe.support_]

    return selected_features

def train_predict_evaluate_model(model_data, region):   
    model_data['publishedAt'] = pd.to_datetime(model_data['publishedAt'])
    train_data, test_data = preprocess_data(model_data)

    # Recursively select features
    selected_features = select_features(train_data)

    # Training
    X_train = train_data[selected_features]
    y_train = train_data['view_velocity']
    model = train_model(X_train, y_train)

    # Predictions
    X_test = test_data[selected_features]
    y_pred = model.predict(X_test)

    # Evaluation
    y_test = test_data['view_velocity']
    mae, mse, rmse, y_pred = evaluate_model(model, X_test, y_test)

    prediction_results = pd.DataFrame({
                                    'Title': test_data['title'], 
                                    'Channel': test_data['channelTitle'],
                                    'Trending Date': test_data['trending_date'],
                                    'Actual View Velocity': y_test,
                                    'Predicted View Velocity': y_pred
                                    })
    prediction_results.to_csv(f"/s/bach/l/under/driva/csx55/Term-Project/data/rforest_results/{region}.csv")

    model_evals = pd.DataFrame({
                                'MAE': [mae],
                                'MSE': [mse],
                                'RMSE': [rmse],
                                'Selected Features': [", ".join(selected_features)]
                                })
    model_evals.to_csv(f"/s/bach/l/under/driva/csx55/Term-Project/data/rforest_results/{region}_metrics.csv")

def read_csv_and_forecast(region):
    model_data = pd.read_csv(f"/s/bach/l/under/driva/csx55/Term-Project/data/model_feature_data/{region}_model_data.csv", error_bad_lines=False)
    model_data.replace([np.inf, -np.inf], np.nan, inplace=True)
    model_data.dropna(inplace=True)

    train_predict_evaluate_model(model_data, region) 

def main():
    if (len(sys.argv) < 1):
        print("Please enter a region as argument to the python program")
    else:
        region = sys.argv[1]

        read_csv_and_forecast(region)

if __name__ == "__main__":
    main()