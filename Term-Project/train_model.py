import pandas as pd
from fbprophet import Prophet
from datetime import datetime, timedelta
import os
import matplotlib.pyplot as plt
from fbprophet.plot import plot_plotly
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import time


def year_week_to_date(year_week_str):
    # Parse the year and week from the input string
    year, week = map(int, year_week_str.split('-'))

    # Find the first day of the week for the given ISO week number
    first_day = datetime.strptime(f'{year}-W{week}-1', "%Y-W%W-%w")

    # Add 6 days to get the last day of the week
    last_day = first_day + timedelta(days=6)

    return first_day, last_day

def read_csv_train_and_plot(csv_fp, category_title, verbose=True):
    df = pd.read_csv(csv_fp)
    df['year_month_day'] = df['week_year'].apply(lambda x: datetime.strptime(x + '-1', "%Y-%W-%w"))

    category_df = df[df["categoryTitle"] == category_title][["year_month_day", "total_views"]].rename(columns={"year_month_day" : "ds", "total_views" : "y"})
    
    if verbose: print("Training Data:   \n", category_df.head(), "\n")

    # Create model
    m = Prophet(weekly_seasonality=True, daily_seasonality=True)

    # Train on data
    if verbose: print("Model Fit Data:  ")
    m.fit(category_df)
    if verbose: print("\n")

    # Make predictions
    future = m.make_future_dataframe(periods=365)
    forecast = m.predict(future)
    if verbose: print("Future Predictions: ")
    if verbose: print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())

    # Plot forecast results
    plot_plotly(m, forecast).show()

def train_and_forecast(fp, category_title, verbose=True):
    df = pd.read_csv(fp)
    df['year_month_day'] = df['week_year'].apply(lambda x: datetime.strptime(x + '-1', "%Y-%W-%w"))

    category_df = df[df["categoryTitle"] == category_title][["year_month_day", "total_views"]].rename(columns={"year_month_day" : "ds", "total_views" : "y"})
    
    if verbose: print("Training Data:   \n", category_df.head(), "\n")

    # Create model
    m = Prophet(weekly_seasonality=True, daily_seasonality=True)

    # Train on data
    if verbose: print("Model Fit Data:  ")
    m.fit(category_df)
    if verbose: print("\n")

    # Make predictions
    future = m.make_future_dataframe(periods=365)
    forecast = m.predict(future)
    if verbose: print("Future Predictions: ")
    if verbose: print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())

    return forecast

def normalize_forecast(forecast):
    forecast['yhat'] = forecast['yhat'] / forecast['yhat'].max()
    forecast['yhat_lower'] = forecast['yhat_lower'] / forecast['yhat_lower'].max()
    forecast['yhat_upper'] = forecast['yhat_upper'] / forecast['yhat_upper'].max()
    return forecast

def read_csvs_train_and_plot(regions, csv_fps, category_title, verbose=True):
    forecasts = []
    for fp, region in zip(csv_fps, regions):
        forecast = train_and_forecast(fp, category_title, verbose)

        normalized_forecast = normalize_forecast(forecast)

        forecasts.append((region, forecast))

    # Plot forecasts
    fig = go.Figure()

    for region, forecast in forecasts:
        fig.add_trace(go.Scatter(x=forecast['ds'], y=forecast['yhat'], mode='lines', name=region))

    fig.update_layout(title="Forecasts by Region", xaxis_title="Date", yaxis_title="Total Views")
    fig.show()

def main():
    category_title = "Education"
    regions = [
        "US",
        "CA",
        "GB",
        "JP",
        "RU"
    ]

    # Dynamically generate file paths
    file_paths = []
    data_directory = "/s/bach/l/under/driva/csx55/Term-Project/data/"
    for region in regions:
        region_directory = os.path.join(data_directory, f"{region}_week_data")
        
        files = os.listdir(region_directory)
        
        csv_files = [file for file in files if file.endswith(".csv")]
        
        if csv_files:
            csv_file_path = os.path.join(region_directory, csv_files[0])
            file_paths.append(csv_file_path)

    read_csvs_train_and_plot(regions, file_paths, category_title, False)


if __name__ == "__main__":
    main()