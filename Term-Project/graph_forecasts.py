import pandas as pd
import sys
import plotly.graph_objs as go
from plotly.subplots import make_subplots

def normalize_forecast(forecast):
    forecast['yhat'] = forecast['yhat'] / forecast['yhat'].max()
    forecast['yhat_lower'] = forecast['yhat_lower'] / forecast['yhat_lower'].max()
    forecast['yhat_upper'] = forecast['yhat_upper'] / forecast['yhat_upper'].max()
    return forecast

def main():
    regions = [
        "US", "CA",
        "JP", "RU", "BR",
        "DE", "FR", "IN",
        "KR", "MX"
    ]

    if (len(sys.argv) < 1):
        print("Please enter a region as argument to the python program")
        quit()
    else:
        category_title = sys.argv[1]

    forecasts = []
    for region in regions:
        forecast = pd.read_csv(f"/s/bach/l/under/driva/csx55/Term-Project/data/prophet_forecasts_{category_title}/{region}_total_views.csv")
        forecasts.append((region, normalize_forecast(forecast)))

    # Create subplots
    fig = make_subplots(rows=len(regions), cols=1, subplot_titles=regions)

    # Add traces to subplots
    for i, (region, forecast) in enumerate(forecasts, start=1):
        fig.add_trace(go.Scatter(x=forecast['ds'], y=forecast['yhat'], mode='lines', name=region), row=i, col=1)
        fig.add_vline(x="2024-02-07", line_dash="dash", line_color="black", row=i, col=1)
        fig.add_annotation(x="2024-02-07", y=1, text="Start of Prophet Forecasts", showarrow=False, row=i, col=1)

    # Update layout
    fig.update_layout(title="Forecasts by Region", xaxis_title="Date", yaxis_title="Total Views(Normalized)")
    fig.show()

if __name__ == "__main__":
    main()
