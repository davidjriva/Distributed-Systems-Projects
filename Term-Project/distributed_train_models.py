import pandas as pd
import numpy as np
import itertools
import os
import csv
from datetime import datetime
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation, performance_metrics
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def read_parameters_from_csv(file_path):
    params = {}
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        for row in reader:
            params[row[0]] = float(row[1])
    return params

def train_and_forecast(column, prophet_df, category_title):
    # Convert the Spark DataFrame to a Pandas DataFrame
    prophet_pd = prophet_df.toPandas()
    
    # Fix the dates
    prophet_pd['year_month_day'] = prophet_pd['week_year'].apply(lambda x: datetime.strptime(x + '-1', "%Y-%W-%w"))
    prophet_pd['year_month_day'] = prophet_pd['year_month_day']
    prophet_pd = prophet_pd[['year_month_day', 'y']]
    prophet_pd.rename(columns={'year_month_day' : 'ds'}, inplace=True)

    # Tune hyperparameters
    param_file_path = f"/s/bach/l/under/driva/csx55/Term-Project/data/prophet_params/{column}_best_params.csv"
    if os.path.exists(param_file_path):
        # Load params to speed up training
        best_params = read_parameters_from_csv(param_file_path)
    else:
        # Generate params from scratch
        best_params = model_cross_validation(prophet_pd)

        # Save best params to CSV
        params_df = pd.DataFrame.from_dict(best_params, orient='index', columns=[column])
        params_df.to_csv(f"/s/bach/l/under/driva/csx55/Term-Project/data/prophet_params/{column}_best_params.csv")

    # Create and fit model
    m = Prophet(weekly_seasonality=True, daily_seasonality=True, **best_params)
    m.fit(prophet_pd)
    
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
        'seasonality_prior_scale' : [0.01, 0.1, 1.0, 10.0]
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

    return best_params



def read_csv_and_forecast(spark, category_title):
    joined_df = spark.read.option("header", "true").csv("file:////s/bach/l/under/driva/csx55/Term-Project/data/joined_week_data/joined_week_data.csv")
    joined_df = joined_df.filter(col("categoryTitle") == category_title)
    joined_df = joined_df.drop("categoryTitle")

    for column in joined_df.columns[2:]:
        # Get the column and dates for training
        prophet_df = joined_df.select("week_year", column).withColumnRenamed(column, "y")

        # Train and forecast for each column in parallel
        train_and_forecast(column, prophet_df, category_title) 

def main():
    # Instantiate a session
    spark = SparkSession.builder.appName("Training FB Prophet Distributed").master("local").config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").config("spark.executor.memoryOverhead", "1g").getOrCreate()

    # The locations & category we are modeling
    category_title = "News & Politics"

    read_csv_and_forecast(spark, category_title)

if __name__ == "__main__":
    main()