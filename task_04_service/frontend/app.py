import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Настройка API URL
API_URL = "http://backend:8000"  # В Docker используем имя сервиса

st.set_page_config(page_title="Energy Dashboard", layout="wide")
st.title("⚡ Energy Consumption Dashboard")

# Функция для загрузки данных из API
@st.cache_data(ttl=0)  # ttl=0 означает, что кэш не используется (всегда свежие данные)
def load_data():
    try:
        response = requests.get(f"{API_URL}/records")
        if response.status_code == 200:
            return pd.DataFrame(response.json())
        else:
            st.error(f"Error loading data: {response.status_code}")
            return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        st.error(f"Connection error: {e}")
        return pd.DataFrame()

# Основная часть
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📊 Data Table")
    df = load_data()
    
    if not df.empty:
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No data available")

with col2:
    st.subheader("➕ Add New Record")
    
    with st.form("add_record_form"):
        timestamp = st.text_input("Timestamp (YYYY-MM-DD HH:MM:SS)", 
                                 value="2024-01-01 00:00:00")
        col_a, col_b = st.columns(2)
        with col_a:
            cons_europe = st.number_input("Consumption Europe", min_value=0.0, value=100.0)
            price_europe = st.number_input("Price Europe", min_value=0.0, value=10.0)
        with col_b:
            cons_asia = st.number_input("Consumption Asia", min_value=0.0, value=100.0)
            price_asia = st.number_input("Price Asia", min_value=0.0, value=10.0)
        
        submitted = st.form_submit_button("Add Record")
        
        if submitted:
            new_record = {
                "timestamp": timestamp,
                "consumption_europe": cons_europe,
                "consumption_asia": cons_asia,
                "price_europe": price_europe,
                "price_asia": price_asia
            }
            
            try:
                response = requests.post(f"{API_URL}/records", json=new_record)
                if response.status_code == 201:
                    st.success("Record added successfully!")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"Error: {response.text}")
            except requests.exceptions.RequestException as e:
                st.error(f"Connection error: {e}")

# Графики
if not df.empty:
    st.subheader("📈 Visualizations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # График потребления
        fig_consumption = px.line(df, x='timestamp', 
                                 y=['consumption_europe', 'consumption_asia'],
                                 title='Energy Consumption Over Time',
                                 labels={'value': 'Consumption', 'variable': 'Region'})
        st.plotly_chart(fig_consumption, use_container_width=True)
    
    with col2:
        # График цен
        fig_prices = px.line(df, x='timestamp', 
                            y=['price_europe', 'price_asia'],
                            title='Energy Prices Over Time',
                            labels={'value': 'Price', 'variable': 'Region'})
        st.plotly_chart(fig_prices, use_container_width=True)
    
    # Удаление записи
    st.subheader("🗑️ Delete Record")
    
    col1, col2, col3 = st.columns([2, 1, 2])
    with col1:
        record_id = st.number_input("Enter ID to delete", min_value=1, step=1)
    with col2:
        if st.button("Delete"):
            try:
                response = requests.delete(f"{API_URL}/records/{record_id}")
                if response.status_code == 200:
                    st.success(f"Record {record_id} deleted!")
                    st.cache_data.clear()
                    st.rerun()
                elif response.status_code == 404:
                    st.error(f"Record with ID {record_id} not found")
                else:
                    st.error(f"Error: {response.text}")
            except requests.exceptions.RequestException as e:
                st.error(f"Connection error: {e}")