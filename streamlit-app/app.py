import plotly.graph_objects as go
import streamlit as st

from config.db_config import DatabaseConfigError, load_db_config
from utils.db_utils import get_db_connection
from utils.sql_utils import execute_sql_query


st.set_page_config(page_title="Weather vs IBM Stock Price")

st.title("Do Weather conditions affect the stock price of IBM?")

# Connect to the database
try:
    connection_details = load_db_config()
    conn = get_db_connection(connection_details)

    current_df = execute_sql_query("sql/select_current_weather.sql", conn)
    merged_hourly_df = execute_sql_query("sql/select_hourly_data.sql", conn)
    merged_daily_df = execute_sql_query("sql/select_daily_data.sql", conn)

except DatabaseConfigError as e:
    print(f"Database not configured correctly: {e}")
    raise
finally:
    conn.close()


with st.sidebar:
    # Weather Section
    st.markdown("---")
    st.header("Current Weather")
    st.image(f"http:{current_df.iloc[-1]['condition_icon']}", width=80)

    st.markdown(
        f"""
        <div style="font-size: 14px;">
            <b>Local Time:</b> {current_df.iloc[-1]['local_time']} <br>
            <b>Location:</b> {current_df.iloc[-1]['location_name']}, USA <br>
            <b>Condition:</b> {current_df.iloc[-1]['condition_text']} <br>
            <b>Temperature:</b> {current_df.iloc[-1]['temp_c']}°C <br>
            <b>Feels Like:</b> {current_df.iloc[-1]['feelslike_c']}°C <br>
            <b>Humidity:</b> {current_df.iloc[-1]['humidity']}% <br>
            <b>Wind:</b> {current_df.iloc[-1]['wind_mph']} mph
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

data_interval = st.radio("Select Data Interval:", ["Hourly", "Daily"])

selected_df = merged_hourly_df if data_interval == "Hourly" else merged_daily_df

stock_metric = st.selectbox(
    "Select IBM Stock Metric:", ["Open", "Close", "High", "Low", "Volume"]
)

selected_stock_column = stock_metric.lower()

weather_metric = st.selectbox(
    "Select Weather Metric:", ["Temperature (°C)", "Wind Speed (mph)", "Humidity (%)"]
)

weather_column_map = {
    "Temperature (°C)": "temp_c" if data_interval == "Hourly" else "avgtemp_c",
    "Wind Speed (mph)": "wind_mph" if data_interval == "Hourly" else "maxwind_mph",
    "Humidity (%)": "humidity" if data_interval == "Hourly" else "avghumidity",
}
selected_weather_column = weather_column_map[weather_metric]

fig = go.Figure()

fig.add_trace(
    go.Scatter(
        x=selected_df["time" if data_interval == "Hourly" else "date"],
        y=selected_df[selected_weather_column],
        mode="lines",
        name=weather_metric if data_interval == "Hourly" else f"Avg {weather_metric}",
        yaxis="y1",
        line=dict(color="#1f77b4", width=2),
    )
)

fig.add_trace(
    go.Scatter(
        x=selected_df["time" if data_interval == "Hourly" else "date"],
        y=selected_df[selected_stock_column],
        mode="lines",
        name=f"IBM {stock_metric}",
        yaxis="y2",
        line=dict(color="#ff7f0e", width=2),
    )
)

fig.update_layout(
    title=f"{weather_metric if data_interval == "Hourly" else f"Avg {weather_metric}"} vs IBM Stock Trend in a {"30-day" if data_interval == "Hourly" else "3-month"} {"Hourly" if data_interval == "Hourly" else "Daily"} Period",
    xaxis=dict(
        title="Date",
        tickformat="%Y-%m-%d %H:%M" if data_interval == "Hourly" else "%Y-%m-%d",
        tickangle=-30,
        showgrid=True,
        gridcolor="rgba(200, 200, 200, 0.3)",
    ),
    yaxis=dict(
        title=weather_metric if data_interval == "Hourly" else f"Avg {weather_metric}",
        titlefont=dict(color="#1f77b4"),
        tickfont=dict(color="#1f77b4"),
        showgrid=True,
        gridcolor="rgba(200, 200, 200, 0.3)",
    ),
    yaxis2=dict(
        title=f"IBM {stock_metric} ($)",
        titlefont=dict(color="#ff7f0e"),
        tickfont=dict(color="#ff7f0e"),
        overlaying="y",
        side="right",
        showgrid=False,
    ),
    hovermode="x unified",
    legend=dict(
        x=1,
        y=1,
        xanchor="left",
        yanchor="top",
        bgcolor="rgba(255,255,255,0.6)",
        bordercolor="black",
        borderwidth=1,
    ),
    template="plotly_dark",
)

st.plotly_chart(fig, use_container_width=True)


correlation = (
    selected_df[[selected_weather_column, selected_stock_column]].corr().iloc[0, 1]
)


def interpret_correlation(corr_value):
    if corr_value == 1:
        return "Perfect Positive Correlation"
    elif corr_value >= 0.7:
        return "Strong Positive Correlation"
    elif corr_value >= 0.5:
        return "Moderate Positive Correlation"
    elif corr_value > 0:
        return "Weak Positive Correlation"
    elif corr_value == 0:
        return "No Correlation"
    elif corr_value <= -0.5:
        return "Moderate Negative Correlation"
    elif corr_value <= -0.7:
        return "Strong Negative Correlation"
    else:
        return "Weak Negative Correlation"


correlation_description = interpret_correlation(correlation)

st.markdown("***")
st.write(
    f"##### Correlation between {weather_metric if data_interval == "Hourly" else f"Avg {weather_metric}"} and IBM Stock {stock_metric} ({data_interval} Data)"
)

st.markdown(
    f"""
    <style>
    .styled-table {{
        width: 100%;
        border-collapse: collapse;
        margin: 20px 0;
        font-size: 16px;
        text-align: left;
        background-color: #f9f9f9;
    }}
    .styled-table th {{
        background-color: #f4f4f4;
        font-weight: bold;
        padding: 10px;
        border-bottom: 2px solid #ddd;
    }}
    .styled-table td {{
        padding: 10px;
        border-bottom: 1px solid #ddd;
    }}
    .styled-table tr:nth-child(even) {{
        background-color: #ffffff;
    }}
    .styled-table tr:nth-child(odd) {{
        background-color: #f9f9f9;
    }}
    </style>

    <table class="styled-table">
        <tr>
            <td><b>Correlation Coefficient</b></td>
            <td><b>Interpretation</b></td>
        </tr>
        <tr>
            <td><b>{correlation:.4f}</b></td>
            <td>{correlation_description}</td>
        </tr>
    </table>
    """,
    unsafe_allow_html=True,
)

correlations = (
    selected_df[
        [
            selected_weather_column,
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ]
    .corr()
    .iloc[0, 1:]
)

table_rows = ""
for metric, corr_value in correlations.items():
    interpretation = interpret_correlation(corr_value)
    table_rows += f"""
        <tr>
            <td><b>{metric.capitalize()}</b></td>
            <td><b>{corr_value:.4f}</b></td>
            <td>{interpretation}</td>
        </tr>
    """

with st.expander(
    f"View Correlation Coefficients between {weather_metric if data_interval == "Hourly" else f"Avg {weather_metric}"} and for All IBM Stock Metrics"
):
    st.markdown(
        f"""
        <style>
        .styled-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 16px;
            text-align: left;
            background-color: #f9f9f9;
        }}
        .styled-table th {{
            background-color: #f4f4f4;
            font-weight: bold;
            padding: 10px;
            border-bottom: 2px solid #ddd;
        }}
        .styled-table td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }}
        .styled-table tr:nth-child(even) {{
            background-color: #ffffff;
        }}
        .styled-table tr:nth-child(odd) {{
            background-color: #f9f9f9;
        }}
        </style>

        <table class="styled-table">
            <tr>
                <th>Stock Metric</th>
                <th>Correlation Coefficient</th>
                <th>Interpretation</th>
            </tr>
            {table_rows}
        </table>
        """,
        unsafe_allow_html=True,
    )
