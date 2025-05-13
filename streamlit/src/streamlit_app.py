import os
import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from datetime import date
import requests

# --- Page Configuration ---
st.set_page_config(
    page_title="Sales and Customer Analysis",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Global Color Palette ---
PRIMARY_COLOR = '#2E5077'   # Sophisticated dark blue
SECONDARY_COLOR = '#3498db' # Bright blue accent
ACCENT_COLOR = '#FFA630'    # Orange highlight color
NEUTRAL_LIGHT = '#F5F5F5'   # Light gray background
NEUTRAL_DARK = '#7f8c8d'    # Dark gray text

CUSTOM_CONTINUOUS_SCALE = [PRIMARY_COLOR, SECONDARY_COLOR]
CUSTOM_DISCRETE_SEQUENCE = [SECONDARY_COLOR, ACCENT_COLOR, '#2ecc71', '#9b59b6', '#e74c3c']
px.defaults.template = "plotly_white"
px.defaults.color_continuous_scale = CUSTOM_CONTINUOUS_SCALE
px.defaults.color_discrete_sequence = CUSTOM_DISCRETE_SEQUENCE


def inject_custom_css():
    """
    Inject custom CSS styles for consistent branding and layout.
    """
    custom_css = f"""
    <style>
        .stApp {{ background-color: {NEUTRAL_LIGHT}; }}
        [data-testid=stSidebar] {{ background-color: {PRIMARY_COLOR}; color: {NEUTRAL_LIGHT}; }}
        [data-testid=stWidgetLabel] p {{ color: {NEUTRAL_LIGHT}; }}
        h1, h2, h3, h4, h5, h6 {{ color: {PRIMARY_COLOR}; font-family: 'Helvetica Neue', Arial, sans-serif; }}
        .metric-value {{ font-size: 20px !important; color: black !important; }}
        .metric-label {{ color: {NEUTRAL_DARK} !important; }}
        .stPlotlyChart, .stDataFrame, .stCard {{
            background-color: white;
            border-radius: 15px;
            box-shadow: 2px 2px 5px #aaaaaa;
            margin-bottom: 10px;
        }}
    </style>
    """
    st.markdown(custom_css, unsafe_allow_html=True)


def get_snowflake_connection():
    """
    Establish a Snowflake connection using environment variables for credentials.

    Returns:
        snowflake.connector.SnowflakeConnection or None
    """
    try:
        return snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
        )
    except Exception as error:
        st.error(f"Connection error: {error}")
        return None


def run_query(connection, query: str, params: list = None) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a DataFrame.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    except Exception as error:
        st.error(f"Query execution error: {error}")
        return pd.DataFrame()
    finally:
        cursor.close()


@st.cache_data(ttl=600)
def load_data(start_date: date,
              end_date: date,
              location: str,
              category: str,
              gender: str,
              order_type: str,
              payment_method: str,
              season: str
             ) -> pd.DataFrame:
    """
    Load and filter order and customer data from Snowflake.
    Returns a DataFrame with deduplicated customer info.
    """
    conn = get_snowflake_connection()
    if conn is None:
        return pd.DataFrame()

    # Deduplicate customers
    query = """
    WITH unique_customers AS (
        SELECT
            CUSTOMER_ID,
            MAX(AGE) AS AGE,
            MAX(GENDER) AS GENDER,
            MAX(PREVIOUS_PURCHASES) AS PREVIOUS_PURCHASES
        FROM STOREDATABASE.STOREDATA.CUSTOMERS
        GROUP BY CUSTOMER_ID
    )
    SELECT
        o.ORDER_ID, o.ORDER_TYPE, o.CUSTOMER_ID, o.ITEM_PURCHASED,
        o.CATEGORY, o.PURCHASE_AMOUNT_USD, o.LOCATION, o.SIZE,
        o.COLOR, o.SEASON, o.REVIEW_RATING, o.SHIPPING_TYPE,
        o.DISCOUNT_APPLIED, o.PROMO_CODE_USED, o.PAYMENT_METHOD,
        o.CREATED_AT, uc.AGE, uc.GENDER, uc.PREVIOUS_PURCHASES
    FROM STOREDATABASE.STOREDATA.ORDERS o
    LEFT JOIN unique_customers uc
      ON o.CUSTOMER_ID = uc.CUSTOMER_ID
    WHERE o.CREATED_AT BETWEEN %s AND %s
    """
    params = [start_date, end_date]

    filters = {
    'o.LOCATION': location,
    'o.CATEGORY': category,
    'uc.GENDER': gender,
    'o.ORDER_TYPE': order_type,
    'o.PAYMENT_METHOD': payment_method,
    'o.SEASON': season
    }
    for field, value in filters.items():
        if value and value.lower() != "all":
            query += f" AND {field} = %s"
            params.append(value)
    df = run_query(conn, query, params)
    conn.close()

    # Ensure numeric type and drop invalid rows
    df['PURCHASE_AMOUNT_USD'] = pd.to_numeric(df['PURCHASE_AMOUNT_USD'], errors='coerce')
    return df.dropna(subset=['PURCHASE_AMOUNT_USD'])


def create_sidebar_filters():
    """
    Display sidebar filters and retrieve user selections.
    """
    st.sidebar.header('Filters')
    start_date = st.sidebar.date_input('Start Date', date(2024, 1, 1))
    end_date = st.sidebar.date_input('End Date', date.today())

    conn = get_snowflake_connection()
    if conn is None:
        return None

    # Retrieve distinct options
    locs = run_query(conn, "SELECT DISTINCT LOCATION FROM STOREDATABASE.STOREDATA.ORDERS")['LOCATION'].tolist()
    cats = run_query(conn, "SELECT DISTINCT CATEGORY FROM STOREDATABASE.STOREDATA.ORDERS")['CATEGORY'].tolist()
    gens = run_query(conn, "SELECT DISTINCT GENDER FROM STOREDATABASE.STOREDATA.CUSTOMERS")['GENDER'].tolist()
    types = run_query(conn, "SELECT DISTINCT ORDER_TYPE FROM STOREDATABASE.STOREDATA.ORDERS")['ORDER_TYPE'].tolist()
    pays = run_query(conn, "SELECT DISTINCT PAYMENT_METHOD FROM STOREDATABASE.STOREDATA.ORDERS")['PAYMENT_METHOD'].tolist()
    seasons = run_query(conn, "SELECT DISTINCT SEASON FROM STOREDATABASE.STOREDATA.ORDERS")['SEASON'].tolist()
    conn.close()

    location = st.sidebar.selectbox('Location', ['All'] + locs)
    category = st.sidebar.selectbox('Category', ['All'] + cats)
    gender = st.sidebar.selectbox('Gender', ['All'] + gens)
    order_type = st.sidebar.selectbox('Order Type', ['All'] + types)
    payment_method = st.sidebar.selectbox('Payment Method', ['All'] + pays)
    season = st.sidebar.selectbox('Season', ['All'] + seasons)

    return start_date, end_date, location, category, gender, order_type, payment_method, season


def main():
    """
    Main application logic for the Streamlit dashboard, including KPIs and visualizations.
    """
    inject_custom_css()
    filters = create_sidebar_filters()
    if filters is None:
        st.error("Unable to load filters due to connection error.")
        return

    # Unpack filter selections
    start_date, end_date, location, category, gender, order_type, payment_method, season = filters
    df = load_data(start_date, end_date, location, category, gender, order_type, payment_method, season)

    # --- Key Performance Indicators ---
    st.header("Sales and Customer Analysis")
    st.subheader("Key Metrics")
    total_sales = df['PURCHASE_AMOUNT_USD'].sum() if not df.empty else 0
    order_count = len(df)
    unique_customers = df['CUSTOMER_ID'].nunique() if not df.empty else 0
    avg_order_value = total_sales / order_count if order_count else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Sales", f"${total_sales:,.2f}")
    k2.metric("Orders", order_count)
    k3.metric("Unique Customers", unique_customers)
    k4.metric("Average Order Value", f"${avg_order_value:,.2f}")

    # Only generate visualizations if data is present
    if not df.empty:
        st.subheader("Visualizations")
        # Sales trend over time
        df_time = df.groupby(pd.Grouper(key='CREATED_AT', freq='D'))['PURCHASE_AMOUNT_USD']\
                     .sum().reset_index()
        fig_trend = px.line(
            df_time,
            x='CREATED_AT',
            y='PURCHASE_AMOUNT_USD',
            title='Daily Sales Trend'
        )
        fig_trend.update_layout(margin=dict(l=20, r=20, t=40, b=20))

        # Forecast bar chart
        forecast_available = False
        try:
            resp = requests.get('http://fastapi_app:8081/forecast')
            resp.raise_for_status()
            df_f = pd.DataFrame(resp.json())
            df_f['ds'] = pd.to_datetime(df_f['ds'])
            fig_forecast = px.bar(
                df_f,
                x='ds',
                y='yhat',
                title='7-Day Sales Forecast',
                color_discrete_sequence=[ACCENT_COLOR]
            )
            fig_forecast.update_layout(margin=dict(l=20, r=20, t=40, b=20))
            forecast_available = True
        except Exception:
            st.warning("Sales forecast not available.")

        # Display time series and forecast side by side
        col1, col2 = st.columns(2)
        col1.plotly_chart(fig_trend, use_container_width=True)
        if forecast_available:
            col2.plotly_chart(fig_forecast, use_container_width=True)

        # Category and gender breakdowns
        col_cat, col_gen = st.columns(2)
        fig_cat = px.bar(
            df.groupby('CATEGORY')['PURCHASE_AMOUNT_USD'].sum().reset_index(),
            x='CATEGORY',
            y='PURCHASE_AMOUNT_USD',
            title='Sales by Category',
            color='CATEGORY'
        )
        fig_cat.update_layout(margin=dict(l=20, r=20, t=40, b=20))
        col_cat.plotly_chart(fig_cat, use_container_width=True)

        fig_gen = px.bar(
            df.groupby('GENDER')['PURCHASE_AMOUNT_USD'].sum().reset_index(),
            x='GENDER',
            y='PURCHASE_AMOUNT_USD',
            title='Sales by Gender',
            color='GENDER'
        )
        fig_gen.update_layout(margin=dict(l=20, r=20, t=40, b=20))
        col_gen.plotly_chart(fig_gen, use_container_width=True)

        # Payment method distribution pie chart
        fig_pay = px.pie(
            df.groupby('PAYMENT_METHOD').size().reset_index(name='count'),
            names='PAYMENT_METHOD',
            values='count',
            title='Payment Methods Breakdown',
            color_discrete_sequence=CUSTOM_DISCRETE_SEQUENCE
        )
        fig_pay.update_layout(margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_pay, use_container_width=True)

        # Geographic sales map
        df_map = df.groupby('LOCATION', as_index=False)['PURCHASE_AMOUNT_USD'].sum()
        coords = {
            'Kentucky': {'latitude': 37.8393, 'longitude': -84.2700},
            'Maine': {'latitude': 45.2538, 'longitude': -69.4455},
            'Massachusetts': {'latitude': 42.4072, 'longitude': -71.3824},
            'Rhode Island': {'latitude': 41.5801, 'longitude': -71.4774},
            'Oregon': {'latitude': 43.8041, 'longitude': -120.5542},
            'Wyoming': {'latitude': 43.0759, 'longitude': -107.2903},
            'Montana': {'latitude': 46.8797, 'longitude': -110.3626},
            'Louisiana': {'latitude': 31.3833, 'longitude': -92.5671},
            'West Virginia': {'latitude': 38.5976, 'longitude': -80.4549},
            'Missouri': {'latitude': 38.4561, 'longitude': -92.2884},
            'Arkansas': {'latitude': 34.9697, 'longitude': -92.3731},
            'Hawaii': {'latitude': 21.3069, 'longitude': -157.8583},
            'Delaware': {'latitude': 39.3185, 'longitude': -75.5071},
            'New Hampshire': {'latitude': 43.1939, 'longitude': -71.5724},
            'New York': {'latitude': 40.7128, 'longitude': -74.0060},
            'Alabama': {'latitude': 32.8067, 'longitude': -86.7911},
            'Mississippi': {'latitude': 32.3547, 'longitude': -89.3985},
            'North Carolina': {'latitude': 35.6301, 'longitude': -79.8064},
            'California': {'latitude': 36.1162, 'longitude': -119.6832},
            'Oklahoma': {'latitude': 35.5653, 'longitude': -96.9289},
            'Florida': {'latitude': 27.9944, 'longitude': -81.7602},
            'Texas': {'latitude': 31.0545, 'longitude': -97.5635},
            'Nevada': {'latitude': 38.3135, 'longitude': -117.0554},
            'Kansas': {'latitude': 38.5266, 'longitude': -98.4660},
            'Colorado': {'latitude': 39.0598, 'longitude': -105.3111},
            'North Dakota': {'latitude': 47.5289, 'longitude': -99.7840},
            'Illinois': {'latitude': 40.0009, 'longitude': -89.8639},
            'Indiana': {'latitude': 39.8494, 'longitude': -86.2500},
            'Arizona': {'latitude': 33.7291, 'longitude': -111.4312},
            'Alaska': {'latitude': 61.3707, 'longitude': -152.4044},
            'Tennessee': {'latitude': 35.7478, 'longitude': -86.6927},
            'Ohio': {'latitude': 40.3674, 'longitude': -82.9962},
            'New Jersey': {'latitude': 40.2989, 'longitude': -74.5210},
            'Maryland': {'latitude': 39.0458, 'longitude': -76.6413},
            'Vermont': {'latitude': 44.5588, 'longitude': -72.5778},
            'New Mexico': {'latitude': 34.5199, 'longitude': -105.8701},
            'South Carolina': {'latitude': 33.8361, 'longitude': -81.1637},
            'Idaho': {'latitude': 44.2405, 'longitude': -114.8675},
            'Pennsylvania': {'latitude': 40.5773, 'longitude': -77.2640},
            'Connecticut': {'latitude': 41.6032, 'longitude': -73.0877},
            'Utah': {'latitude': 40.1500, 'longitude': -111.8624},
            'Virginia': {'latitude': 37.7693, 'longitude': -78.1700},
            'Georgia': {'latitude': 33.0406, 'longitude': -83.6431},
            'Nebraska': {'latitude': 41.1254, 'longitude': -98.2681},
            'Iowa': {'latitude': 42.0046, 'longitude': -93.2140},
            'South Dakota': {'latitude': 44.2996, 'longitude': -99.4388},
            'Minnesota': {'latitude': 45.6945, 'longitude': -93.9003},
            'Washington': {'latitude': 47.7511, 'longitude': -120.7401},
            'Wisconsin': {'latitude': 44.2685, 'longitude': -89.6165},
            'Michigan': {'latitude': 44.3148, 'longitude': -85.6024}
            }
        df_map['lat'] = df_map['LOCATION'].map(lambda x: coords.get(x, {}).get('latitude'))
        df_map['lon'] = df_map['LOCATION'].map(lambda x: coords.get(x, {}).get('longitude'))
        df_map = df_map.dropna(subset=['lat', 'lon'])
        if not df_map.empty:
            df_map['marker_size'] = df_map['PURCHASE_AMOUNT_USD'] / df_map['PURCHASE_AMOUNT_USD'].max() * 100
            fig_map = px.scatter_mapbox(
                df_map,
                lat='lat',
                lon='lon',
                size='marker_size',
                color='PURCHASE_AMOUNT_USD',
                hover_name='LOCATION',
                hover_data=['PURCHASE_AMOUNT_USD'],
                mapbox_style='carto-positron',
                zoom=4,
                title='Sales by Location',
                size_max=75
            )
            fig_map.update_layout(margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.warning("No geographic data available for map.")

    # --- Detailed Orders Table with Pagination ---
    st.subheader("Detailed Orders")
    items_per_page = 50
    total_pages = max(1, (len(df) + items_per_page - 1) // items_per_page)
    page = st.number_input('Page', min_value=1, max_value=total_pages, value=1)
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    st.dataframe(df.iloc[start_idx:end_idx], use_container_width=True)
    st.caption(f"Page {page} of {total_pages}")

if __name__ == "__main__":
    main()
