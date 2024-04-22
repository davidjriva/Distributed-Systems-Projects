import pandas as pd
import itertools
import os
import sys
from datetime import datetime
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation, performance_metrics

def train_and_forecast(column, prophet_df, category_title):    
    prophet_df = prophet_df.copy()    
    # Fix the dates
    prophet_df['year_month_day'] = prophet_df['week_year'].apply(lambda x: datetime.strptime(x + '-1', "%Y-%W-%w"))
    prophet_df['year_month_day'] = prophet_df['year_month_day']
    prophet_df = prophet_df[['year_month_day', 'y']]
    prophet_df.rename(columns={'year_month_day' : 'ds'}, inplace=True)

    # Tune hyperparameters
    param_file_path = f"/s/bach/l/under/driva/csx55/Term-Project/data/prophet_params/{column}_best_params.csv"
    rmse_file_path = f"/s/bach/l/under/driva/csx55/Term-Project/data/prophet_params/{column}_rmse.txt"

    # Generate params from scratch
    best_params, best_rmse = model_cross_validation(prophet_df)

    # Save the RMSE
    with open(rmse_file_path, "w") as rmse_file:
        rmse_file.write("Best RMSE: " + str(best_rmse))

    # Save best params to CSV
    params_df = pd.DataFrame.from_dict(best_params, orient='index', columns=[column])
    params_df.to_csv(param_file_path)

    # Create and fit model
    m = Prophet(**best_params)
    m.fit(prophet_df)
    
    # Make a forecast
    future = m.make_future_dataframe(periods=365)
    forecast = m.predict(future)
    
    # Write the forecast to CSV
    output_dir = f"/s/bach/l/under/driva/csx55/Term-Project/data/prophet_forecasts_{category_title}/"
    forecast.to_csv(os.path.join(output_dir, column + ".csv"), header=True, index=False)

'''
    Use cross validation to tune our model's hyperparameters
'''
def model_cross_validation(prophet_pd):
    param_grid = {
        'changepoint_prior_scale' : [0.001, 0.01, 0.1, 0.5],
        'seasonality_prior_scale' : [0.01, 0.1, 1.0, 10.0],
        'daily_seasonality' : [True, False],
        'weekly_seasonality' : [True, False]
    }

    # Generate all combinations of parameters
    all_params = [dict(zip(param_grid.keys(), v)) for v in itertools.product(*param_grid.values())]
    best_rmse = float('inf')
    best_params = None

    for params in all_params:
        m = Prophet(**params).fit(prophet_pd)
        df_cv = cross_validation(m, initial='730 days', period='180 days', horizon = '365 days')
        df_p = performance_metrics(df_cv, rolling_window=1)
        rmse = df_p['rmse'].values[0]

        if rmse < best_rmse:
            best_rmse = rmse
            best_params = params

    return best_params, best_rmse

def read_csv_and_forecast(category_title, region):
    joined_df = pd.read_csv("/s/bach/l/under/driva/csx55/Term-Project/data/joined_week_data/joined_week_data.csv")
    joined_df = joined_df[joined_df["categoryTitle"] == category_title]
    joined_df = joined_df.drop(columns=["categoryTitle"])

    column = f"{region}_total_views"
    prophet_df = joined_df[["week_year", column]].rename(columns={column: "y"})

    # Train and forecast for each column in parallel
    train_and_forecast(column, prophet_df, category_title) 

def main():
    if (len(sys.argv) < 1):
        print("Please enter a region as argument to the python program")
    else:
        # The locations & category we are modeling
        category_title = "News & Politics"
        region = sys.argv[1]

        read_csv_and_forecast(category_title, region)

if __name__ == "__main__":
    main()