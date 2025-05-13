import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import requests
import json
from datetime import datetime

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')

# Setup MongoDB
client = MongoClient(MONGO_URI)
db = client['bps_db']
collection = db['pekerja_terdaftar']

# Get latest document
latest = collection.find_one(sort=[("timestamp", -1)])

# Mapping key ID from variables (from raw BPS JSON)
column_map = {
    "iihviv2ocw": "Pencari Kerja Terdaftar - Laki-Laki",
    "ijuxru3lvl": "Pencari Kerja Terdaftar - Perempuan",
    "b1xjkdn0vw": "Pencari Kerja Terdaftar - Jumlah",
    "kgpd8jp9bs": "Lowongan Kerja Terdaftar - Laki-Laki",
    "b4ox1vczyq": "Lowongan Kerja Terdaftar - Perempuan",
    "yeloqirlpp": "Lowongan Kerja Terdaftar - Jumlah",
    "2ikzujodce": "Penempatan/Pemenuhan Tenaga Kerja - Laki-Laki",
    "lfbbv5gdz2": "Penempatan/Pemenuhan Tenaga Kerja - Perempuan",
    "ytis9poht5": "Penempatan/Pemenuhan Tenaga Kerja - Jumlah",
    "ksybbjfehm": "Pencari Kerja Terdaftar - Laki-Laki atau Perempuan"
}

# Streamlit UI Configuration
st.set_page_config(
    page_title="Statistik Pekerjaan BPS",
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
        .main {
            background-color: #f8f9fa;
        }
        .st-bw {
            background-color: white;
            border-radius: 10px;
            padding: 15px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .header {
            color: #2c3e50;
        }
        .metric-card {
            background: white;
            border-radius: 10px;
            padding: 15px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 15px;
        }
        .plot-container {
            background: white;
            border-radius: 10px;
            padding: 15px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 20px;
        }
    </style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=86400)  # Cache 1 day
def get_bps_geojson():
    try:
        # Alternative GeoJSON if BPS portal fails
        backup_url = "https://raw.githubusercontent.com/superpikar/indonesia-geojson/master/indonesia-province-simple.json"
        bps_url = "https://geoportal.bps.go.id/maps/sharing/portals/self?f=pjson"
        
        response = requests.get(bps_url)
        if response.status_code == 200:
            return response.json()
        else:
            st.warning("Menggunakan GeoJSON alternatif (BPS portal tidak merespon)")
            return requests.get(backup_url).json()
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return None

# Main App
st.title("📊 Statistik Ketenagakerjaan Indonesia (2024)")
st.markdown("Visualisasi data pekerja terdaftar, lowongan kerja, dan penempatan tenaga kerja dari BPS")

if latest:
    data = latest["data"]
    rows = []

    for item in data:
        row = {"Provinsi": item["label"]}
        variables = item.get("variables", {})
        for key, label in column_map.items():
            raw_val = variables.get(key, {}).get("value_raw", "0").replace(".", "").replace(",", ".")
            try:
                row[label] = float(raw_val)
            except ValueError:
                row[label] = 0.0
        rows.append(row)

    df = pd.DataFrame(rows)
    
    # Clean province names for geo matching
    df['Provinsi_Clean'] = df['Provinsi'].str.upper().str.replace('DKI ', '').str.replace('DI ', '')
    
    # Calculate national totals
    total_pencari = df["Pencari Kerja Terdaftar - Jumlah"].sum()
    total_lowongan = df["Lowongan Kerja Terdaftar - Jumlah"].sum()
    total_penempatan = df["Penempatan/Pemenuhan Tenaga Kerja - Jumlah"].sum()
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
            <div class="metric-card">
                <h3>Total Pencari Kerja</h3>
                <h1>{total_pencari:,.0f}</h1>
            </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
            <div class="metric-card">
                <h3>Total Lowongan Kerja</h3>
                <h1>{total_lowongan:,.0f}</h1>
            </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
            <div class="metric-card">
                <h3>Total Penempatan</h3>
                <h1>{total_penempatan:,.0f}</h1>
            </div>
        """, unsafe_allow_html=True)
    
    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["📊 Visualisasi", "📋 Tabel Data", "🗺️ Peta"])
    
    with tab1:
        # Visualization section
        st.subheader("Analisis Ketenagakerjaan per Provinsi")
        
        # Row 1: Top provinces
        col1, col2 = st.columns(2)
        with col1:
            with st.container():
                st.markdown('<div class="plot-container">', unsafe_allow_html=True)
                fig1 = px.bar(df.sort_values("Pencari Kerja Terdaftar - Jumlah", ascending=False).head(10),
                              x="Provinsi", y="Pencari Kerja Terdaftar - Jumlah",
                              title="Top 10 Provinsi: Pencari Kerja Terdaftar",
                              color_discrete_sequence=['#3498db'])
                st.plotly_chart(fig1, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            with st.container():
                st.markdown('<div class="plot-container">', unsafe_allow_html=True)
                fig2 = px.bar(df.sort_values("Lowongan Kerja Terdaftar - Jumlah", ascending=False).head(10),
                              x="Provinsi", y="Lowongan Kerja Terdaftar - Jumlah",
                              title="Top 10 Provinsi: Lowongan Kerja Terdaftar",
                              color_discrete_sequence=['#2ecc71'])
                st.plotly_chart(fig2, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
        
        # Row 2: Scatter plot and ratio
        col1, col2 = st.columns(2)
        with col1:
            with st.container():
                st.markdown('<div class="plot-container">', unsafe_allow_html=True)
                fig3 = px.scatter(df,
                                  x="Pencari Kerja Terdaftar - Jumlah",
                                  y="Penempatan/Pemenuhan Tenaga Kerja - Jumlah",
                                  size="Lowongan Kerja Terdaftar - Jumlah",
                                  color="Provinsi",
                                  hover_name="Provinsi",
                                  title="Penempatan vs. Pencari Kerja (Ukuran = Lowongan)",
                                  height=500)
                st.plotly_chart(fig3, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            with st.container():
                st.markdown('<div class="plot-container">', unsafe_allow_html=True)
                df_ratio = df.copy()
                df_ratio['Rasio Lowongan/Pencari'] = df_ratio['Lowongan Kerja Terdaftar - Jumlah'] / df_ratio['Pencari Kerja Terdaftar - Jumlah']
                df_ratio = df_ratio.sort_values('Rasio Lowongan/Pencari', ascending=False)
                
                fig4 = px.bar(df_ratio.head(10),
                             x="Provinsi", y="Rasio Lowongan/Pencari",
                             title="Top 10 Provinsi: Rasio Lowongan/Pencari Kerja",
                             color_discrete_sequence=['#e74c3c'])
                st.plotly_chart(fig4, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
    
    with tab2:
        # Data table section
        st.subheader("Data Lengkap Ketenagakerjaan per Provinsi")
        st.dataframe(df.style.format({
            "Pencari Kerja Terdaftar - Jumlah": "{:,.0f}",
            "Lowongan Kerja Terdaftar - Jumlah": "{:,.0f}",
            "Penempatan/Pemenuhan Tenaga Kerja - Jumlah": "{:,.0f}"
        }), use_container_width=True)
        
        # Download button
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Download Data sebagai CSV",
            data=csv,
            file_name="statistik_ketenagakerjaan_2024.csv",
            mime="text/csv",
            help="Unduh seluruh data dalam format CSV"
        )
    
    with tab3:
        st.subheader("🗺️ Peta Distribusi Ketenagakerjaan")
        
        geojson_data = get_bps_geojson()
        
        if geojson_data:
            # Metric selection
            metric_options = {
                "Pencari Kerja": "Pencari Kerja Terdaftar - Jumlah",
                "Lowongan Kerja": "Lowongan Kerja Terdaftar - Jumlah",
                "Penempatan Kerja": "Penempatan/Pemenuhan Tenaga Kerja - Jumlah"
            }
            
            selected_metric = st.selectbox(
                "Pilih Indikator:",
                options=list(metric_options.keys()),
                index=0
            )
            
            # Create choropleth map
            fig_map = px.choropleth(
                df,
                geojson=geojson_data,
                locations='Provinsi_Clean',
                featureidkey="properties.Propinsi",  # Adjust based on actual GeoJSON structure
                color=metric_options[selected_metric],
                hover_name="Provinsi",
                hover_data={metric_options[selected_metric]: ":.0f"},
                color_continuous_scale="YlOrRd",
                range_color=(0, df[metric_options[selected_metric]].max()),
                labels={metric_options[selected_metric]: "Jumlah"},
                title=f"Distribusi {selected_metric} per Provinsi"
            )
            
            # Map configuration
            fig_map.update_geos(
                visible=False,
                center={"lat": -2.5, "lon": 118},
                projection_scale=15,
                fitbounds="locations"
            )
            
            fig_map.update_layout(
                height=600,
                margin={"r":0,"t":40,"l":0,"b":0},
                coloraxis_colorbar={
                    "title": "Jumlah",
                    "thickness": 15
                }
            )
            
            st.plotly_chart(fig_map, use_container_width=True)
            
            # Data table
            with st.expander("🔍 Lihat Data Per Provinsi"):
                st.dataframe(
                    df[["Provinsi", metric_options[selected_metric]]]
                    .sort_values(metric_options[selected_metric], ascending=False)
                    .style.format({metric_options[selected_metric]: "{:,.0f}"}),
                    height=300
                )
        else:
            st.error("Tidak dapat memuat data geografis. Silakan coba lagi nanti.")

else:
    st.error("⚠️ Data belum tersedia. Jalankan scraper terlebih dahulu.", icon="🚨")

# Footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #7f8c8d;">
        <p>Data Sumber: Badan Pusat Statistik (BPS) | Diperbarui: {}</p>
    </div>
""".format(latest["timestamp"].strftime("%d %B %Y, %H:%M") if latest else "N/A"), unsafe_allow_html=True)