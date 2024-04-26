import pandas as pd
import itertools
import os
import sys
from datetime import datetime
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation, performance_metrics

def calculate_z_score(df):
    return (((df['y']) - df['y'].mean()) / df['y'].std())

def train_and_forecast(column, prophet_df, category_title):    
    prophet_df = prophet_df.copy()    
    # Fix the dates
    prophet_df['year_month_day'] = prophet_df['week_year'].apply(lambda x: datetime.strptime(x + '-1', "%Y-%W-%w"))
    prophet_df['year_month_day'] = prophet_df['year_month_day']
    prophet_df = prophet_df[['year_month_day', 'y']]
    prophet_df.rename(columns={'year_month_day' : 'ds'}, inplace=True)

    # Remove outliers from the data (points that are more than three standard deviations from the mean)
    prophet_df = prophet_df[calculate_z_score(prophet_df) < 3]

    # Tune hyperparameters
    param_file_path = f"/s/bach/l/under/driva/csx55/Term-Project/data/prophet_params/{column}_best_params.csv"
    performance_file_path = f"/s/bach/l/under/driva/csx55/Term-Project/data/prophet_params/{column}_model_performance.csv"

    # Generate params from scratch
    best_params, df_p = model_cross_validation(prophet_df)

    # Save the RMSE
    df_p.to_csv(performance_file_path)

    # Save best params to CSV
    params_df = pd.DataFrame.from_dict(best_params, orient='index', columns=[column])
    params_df.to_csv(param_file_path)

    # Create and fit model
    m = Prophet(**best_params)

    # Consider country specific holidays
    m.add_country_holidays(country_name=f'{column[0:2]}')
    m.fit(prophet_df)
    
    # Make a forecast
    future = m.make_future_dataframe(periods=365)
    forecast = m.predict(future)
    
    fig1 = m.plot(forecast)
    fig1.suptitle(f"Prophet Plot for {column}", fontsize=16)
    fig1.axes[0].set_xlabel("Date")
    fig1.axes[0].set_ylabel("Value")

    fig2 = m.plot_components(forecast)
    fig2.suptitle(f"Prophet Components Plot for {column}", fontsize=16)
    fig2.axes[0].set_xlabel("Date")
    fig2.axes[0].set_ylabel("Value")    

    # Save the plots as files
    output_dir = f"/s/bach/l/under/driva/csx55/Term-Project/data/prophet_plots_{category_title}/"
    os.makedirs(output_dir, exist_ok=True)

    # Remove existing files if they exist
    plot1_path = os.path.join(output_dir, f"{column}_plot.png")
    if os.path.exists(plot1_path):
        os.remove(plot1_path)
    fig1.savefig(plot1_path)
    
    plot2_path = os.path.join(output_dir, f"{column}_components.png")
    if os.path.exists(plot2_path):
        os.remove(plot2_path)
    fig2.savefig(plot2_path)

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
    best_performance = pd.DataFrame()
    best_params = None

    for params in all_params:
        m = Prophet(**params).fit(prophet_pd)
        df_cv = cross_validation(m, initial='730 days', period='180 days', horizon = '365 days')
        df_p = performance_metrics(df_cv, rolling_window=1)
        rmse = df_p['rmse'].values[0]

        if best_performance.empty or rmse < best_performance['rmse'].values[0]:
            best_performance = df_p
            best_params = params

    return best_params, df_p

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
        category_title = "Education"
        region = sys.argv[1]

        read_csv_and_forecast(category_title, region)

if __name__ == "__main__":
    main()