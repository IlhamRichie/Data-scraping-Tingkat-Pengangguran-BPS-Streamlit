import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime
import numpy as np
import requests  # <--- TAMBAHKAN INI
import json      # <--- TAMBAHKAN INI

# --------------------------------------------------------------------------
# 1. KONFIGURASI HALAMAN STREAMLIT
# --------------------------------------------------------------------------
st.set_page_config(
    page_title="Statistik Ketenagakerjaan BPS",
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="expanded"
)

# --------------------------------------------------------------------------
# 2. LOAD ENVIRONMENT VARIABLES & KONEKSI DB
# --------------------------------------------------------------------------
load_dotenv()
MONGO_URI = os.getenv('MONGO_URI')

@st.cache_resource # Cache koneksi resource MongoDB
def init_connection():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client
    except Exception as e:
        st.sidebar.error(f"MongoDB Connection Error: {e}")
        return None

client = init_connection()

if not client:
    st.error("Kritis: Gagal terhubung ke MongoDB. Aplikasi tidak dapat melanjutkan.")
    st.stop()

db = client.bps_db
collection = db.data
st.sidebar.success("Terhubung ke MongoDB.")

# --------------------------------------------------------------------------
# 3. FUNGSI PENGAMBILAN DATA DARI DB
# --------------------------------------------------------------------------
# @st.cache_data(ttl=3600) # Aktifkan jika sudah stabil, nonaktifkan selama debugging
def get_latest_data_from_db():
    try:
        # Mengambil dokumen terbaru berdasarkan timestamp
        # Pastikan scraper menyimpan 'timestamp' dengan benar
        latest_document = collection.find_one(sort=[("timestamp", -1)])
        return latest_document
    except Exception as e:
        st.error(f"Error mengambil data dari MongoDB: {e}")
        return None

latest_doc = get_latest_data_from_db()

# --------------------------------------------------------------------------
# 4. VALIDASI DATA AWAL DARI DB & DEBUG
# --------------------------------------------------------------------------
if not latest_doc:
    st.error("⚠️ Tidak ada dokumen yang ditemukan di MongoDB. Jalankan scraper terlebih dahulu.", icon="🚨")
    st.stop()

# Akses data provinsi dari field yang benar (sesuai scraper.py)
list_data_provinsi_mentah = latest_doc.get("data_provinsi") # Sesuaikan dengan nama field di scraper

if not list_data_provinsi_mentah or not isinstance(list_data_provinsi_mentah, list):
    st.error(f"⚠️ Field 'data_provinsi' tidak ditemukan dalam dokumen MongoDB, bukan list, atau kosong. Periksa output scraper dan struktur dokumen di DB. Isi latest_doc: {latest_doc}", icon="🚨")
    st.stop()

st.sidebar.info(f"Dokumen terakhir diambil pada: {latest_doc.get('timestamp', 'N/A')}")
if latest_doc.get("metadata_tabel"):
    st.sidebar.caption(f"Judul: {latest_doc['metadata_tabel'].get('judul_tabel', 'N/A')}")
    
    
# --------------------------------------------------------------------------
# FUNGSI UNTUK MENGAMBIL GEOJSON (SEHARUSNYA ADA DI SINI)
# --------------------------------------------------------------------------
@st.cache_data(ttl=86400)  # Cache GeoJSON selama 1 hari
def get_geojson_data():
    primary_geojson_url = "https://raw.githubusercontent.com/superpikar/indonesia-geojson/master/indonesia-province-simple.json"
    try:
        st.sidebar.info(f"Memuat GeoJSON dari: {primary_geojson_url.split('/')[-1]}") # Info di sidebar
        response = requests.get(primary_geojson_url, timeout=15)
        response.raise_for_status()  # Akan raise error untuk status 4xx/5xx
        geojson = response.json()
        # Validasi sederhana apakah ini terlihat seperti GeoJSON
        if isinstance(geojson, dict) and geojson.get("type") in ["FeatureCollection", "Feature"]:
            st.sidebar.success("GeoJSON berhasil dimuat.")
            return geojson
        else:
            st.sidebar.warning("Format GeoJSON tidak sesuai dari URL utama.")
            return None
            
    except requests.exceptions.Timeout:
        st.sidebar.error(f"Timeout saat memuat GeoJSON.")
        return None
    except requests.exceptions.RequestException as e:
        st.sidebar.error(f"GeoJSON Request Error: {e}")
        return None
    except json.JSONDecodeError:
        st.sidebar.error(f"GeoJSON: Respons bukan JSON valid.")
        return None
    except Exception as e:
        st.sidebar.error(f"GeoJSON: Error lain tidak diketahui - {e}")
        return None

# --------------------------------------------------------------------------
# 5. COLUMN MAPPING & PEMROSESAN DATA KE DATAFRAME
# --------------------------------------------------------------------------
column_map = {
    "iihviv2ocw": "Pencari Kerja Laki-Laki",
    "ijuxru3lvl": "Pencari Kerja Perempuan",
    "b1xjkdn0vw": "Pencari Kerja Jumlah",
    "kgpd8jp9bs": "Lowongan Kerja Laki-Laki",
    "b4ox1vczyq": "Lowongan Kerja Perempuan",
    "yeloqirlpp": "Lowongan Kerja Jumlah",
    "2ikzujodce": "Penempatan Tenaga Kerja Laki-Laki",
    "lfbbv5gdz2": "Penempatan Tenaga Kerja Perempuan",
    "ytis9poht5": "Penempatan Tenaga Kerja Jumlah",
    # "ksybbjfehm": "Pencari Kerja Laki-Laki atau Perempuan" # Kolom ini mungkin redundant jika sudah ada L, P, dan Jumlah
}

rows = []
processed_rows_for_debug = []
keys_not_found_summary = {}

for item_idx, item_provinsi in enumerate(list_data_provinsi_mentah):
    # Filter entri "Indonesia" dari daftar provinsi jika tidak ingin ditampilkan sebagai provinsi individual
    label_provinsi = item_provinsi.get("label", f"Provinsi Tidak Ada #{item_idx}")
    if label_provinsi.lower() == "indonesia":
        continue # Lewati data agregat nasional untuk daftar provinsi

    row = {"Provinsi": label_provinsi}
    variables_provinsi = item_provinsi.get("variables", {})
    
    debug_item_data = {"Provinsi": row["Provinsi"], "API_VARIABLES_KEYS": list(variables_provinsi.keys())}

    for api_key, df_col_name in column_map.items():
        if api_key not in variables_provinsi:
            debug_item_data[f"KEY_NOT_FOUND: {api_key}"] = df_col_name
            keys_not_found_summary.setdefault(api_key, 0)
            keys_not_found_summary[api_key] += 1
            raw_value_object = {}
        else:
            raw_value_object = variables_provinsi.get(api_key, {})

        # Akses 'value_raw' dengan aman
        raw_value_string = "0" # Default jika tidak ada atau tidak bisa diakses
        if isinstance(raw_value_object, dict):
            raw_value_string = str(raw_value_object.get("value_raw", "0"))
        
        debug_item_data[f"{df_col_name} (RAW_FROM_API)"] = raw_value_string
        
        # Pembersihan string angka (titik sebagai ribuan)
        cleaned_value_string = raw_value_string.replace(".", "")
        try:
            row[df_col_name] = float(cleaned_value_string)
        except ValueError:
            row[df_col_name] = 0.0 # Gagal konversi, set ke 0
            debug_item_data[f"{df_col_name} (CONVERSION_ERROR)"] = cleaned_value_string
        
        debug_item_data[f"{df_col_name} (PROCESSED_FLOAT)"] = row.get(df_col_name)

    rows.append(row)
    if item_idx < 3: # Debug beberapa item awal
        processed_rows_for_debug.append(debug_item_data)

df = pd.DataFrame(rows)

# --- Bagian Debug Lanjutan ---
with st.sidebar.expander("🔬 Detail Debug Data Processing", expanded=False):
    st.subheader("Contoh Proses Konversi Variabel (Beberapa Provinsi Awal)")
    st.json(processed_rows_for_debug if processed_rows_for_debug else "Tidak ada data provinsi yang diproses (mungkin semua 'Indonesia' atau list kosong).", expanded=False)
    
    st.subheader("Ringkasan Kunci dari `column_map` yang TIDAK DITEMUKAN")
    if keys_not_found_summary:
        st.warning("Kunci berikut dari `column_map` tidak ditemukan di `item['variables']` di API:")
        st.json(keys_not_found_summary)
    else:
        st.success("Semua kunci dari `column_map` ditemukan.")

    st.subheader("DataFrame `df` (Info & 5 Baris Awal)")
    if not df.empty:
        st.write(f"Shape: {df.shape}")
        st.dataframe(df.head())
        st.subheader("Tipe Data Kolom `df`")
        st.json({col: str(df[col].dtype) for col in df.columns})
    else:
        st.warning("DataFrame `df` kosong.")
# --- Akhir Debug Lanjutan ---

if df.empty:
    st.error("DataFrame kosong setelah pemrosesan. Tidak ada data untuk ditampilkan. Periksa output debug.", icon="🚨")
    st.stop()

# --------------------------------------------------------------------------
# 6. PENAMBAHAN KOLOM & TRANSFORMASI DATA LANJUTAN
# --------------------------------------------------------------------------
df['Provinsi_Clean'] = df['Provinsi'].str.upper() \
                                 .str.replace('DKI ', '', regex=False) \
                                 .str.replace('DI ', '', regex=False) \
                                 .str.replace('DAERAH ISTIMEWA ', '', regex=False) \
                                 .str.replace('KEPULAUAN ', 'KEP. ', regex=False) \
                                 .str.strip()

# Rasio untuk grafik (menggunakan df_ratio agar df utama tetap bersih jika perlu)
df_ratio = df.copy()
df_ratio['Rasio Lowongan/Pencari'] = np.where(
    df_ratio['Pencari Kerja Jumlah'] > 0,
    df_ratio['Lowongan Kerja Jumlah'] / df_ratio['Pencari Kerja Jumlah'], 0)
df_ratio['Rasio Lowongan/Pencari'] = df_ratio['Rasio Lowongan/Pencari'].replace([np.inf, -np.inf], 0)

# Rasio untuk scatter plot (ditambahkan ke df utama)
df['Rasio Penempatan/Pencari'] = np.where(
    df['Pencari Kerja Jumlah'] > 0,
    df['Penempatan Tenaga Kerja Jumlah'] / df['Pencari Kerja Jumlah'], 0)
df['Rasio Penempatan/Pencari'] = df['Rasio Penempatan/Pencari'].replace([np.inf, -np.inf], 0)

# --------------------------------------------------------------------------
# 7. LAYOUT UTAMA & METRIK NASIONAL (JIKA PERLU DIHITUNG DARI df)
# --------------------------------------------------------------------------
st.title("Dashboard Ketenagakerjaan Indonesia")
st.markdown(f"Sumber: BPS (Data per {latest_doc.get('timestamp', datetime.now()).strftime('%d %B %Y, %H:%M')})")

# Hitung total dari DataFrame provinsi (bukan dari entri "Indonesia" yang sudah difilter)
total_pencari = df["Pencari Kerja Jumlah"].sum()
total_lowongan = df["Lowongan Kerja Jumlah"].sum()
total_penempatan = df["Penempatan Tenaga Kerja Jumlah"].sum()

st.subheader("Ringkasan Nasional (Agregat dari Provinsi)")
col_met1, col_met2, col_met3 = st.columns(3)
col_met1.metric("Total Pencari Kerja", f"{total_pencari:,.0f}")
col_met2.metric("Total Lowongan Kerja", f"{total_lowongan:,.0f}")
col_met3.metric("Total Penempatan", f"{total_penempatan:,.0f}")
st.markdown("---")

# --------------------------------------------------------------------------
# 8. TABS UNTUK VISUALISASI
# --------------------------------------------------------------------------
tab_grafik, tab_tabel, tab_peta = st.tabs(["📊 Visualisasi Grafik", "📋 Tabel Data", "🗺️ Peta Distribusi"])

with tab_grafik:
    st.subheader("Analisis Ketenagakerjaan per Provinsi")
    
    # Pastikan kolom ada sebelum membuat grafik
    pencari_col = "Pencari Kerja Jumlah"
    lowongan_col = "Lowongan Kerja Jumlah"
    rasio_lp_col = "Rasio Lowongan/Pencari"
    penempatan_col = "Penempatan Tenaga Kerja Jumlah"
    rasio_pp_col = "Rasio Penempatan/Pencari"

    plot_cols = st.columns(2)
    with plot_cols[0]: # FIG 1: Top 10 Pencari Kerja
        if pencari_col in df.columns:
            with st.container(border=True): # Menggunakan border dari st.container
                top_10_pencari = df.nlargest(10, pencari_col)
                fig1 = px.bar(top_10_pencari, x=pencari_col, y="Provinsi", orientation='h',
                              title=f"<b>Top 10 Provinsi: {pencari_col}</b>",
                              color_discrete_sequence=px.colors.qualitative.Plotly,
                              labels={pencari_col: "Jumlah Orang"})
                fig1.update_layout(yaxis={'categoryorder':'total ascending'}, title_x=0.5, yaxis_title=None)
                st.plotly_chart(fig1, use_container_width=True)
        else: st.warning(f"Kolom '{pencari_col}' tidak ditemukan untuk grafik.")

    with plot_cols[1]: # FIG 2: Top 10 Lowongan Kerja
        if lowongan_col in df.columns:
            with st.container(border=True):
                top_10_lowongan = df.nlargest(10, lowongan_col)
                desired_order_lowongan = top_10_lowongan["Provinsi"].tolist()
                fig2 = px.bar(top_10_lowongan, x="Provinsi", y=lowongan_col,
                              title=f"<b>Top 10 Provinsi: {lowongan_col}</b>",
                              color_discrete_sequence=px.colors.qualitative.Pastel,
                              labels={lowongan_col: "Jumlah Lowongan"},
                              category_orders={"Provinsi": desired_order_lowongan})
                fig2.update_layout(title_x=0.5, xaxis_title=None)
                st.plotly_chart(fig2, use_container_width=True)
        else: st.warning(f"Kolom '{lowongan_col}' tidak ditemukan untuk grafik.")

    st.markdown("<br>", unsafe_allow_html=True)
    plot_cols_2 = st.columns(2)

    with plot_cols_2[0]: # FIG 3: Scatter Plot
        if pencari_col in df.columns and penempatan_col in df.columns and lowongan_col in df.columns and rasio_pp_col in df.columns:
            with st.container(border=True):
                fig3 = px.scatter(df, x=pencari_col, y=penempatan_col, size=lowongan_col, color=rasio_pp_col,
                                  color_continuous_scale=px.colors.sequential.Plasma, hover_name="Provinsi",
                                  hover_data={col: (":,.0f" if "Jumlah" in col else ":.2f") for col in [pencari_col, penempatan_col, lowongan_col, rasio_pp_col]},
                                  title="<b>Analisis Penempatan (Pencari vs Penempatan)</b>",
                                  labels={pencari_col: "Pencari Kerja", penempatan_col: "Penempatan", lowongan_col: "Lowongan", rasio_pp_col: "Rasio Penempatan/Pencari"},
                                  size_max=50, height=500)
                fig3.update_layout(title_x=0.5, coloraxis_colorbar_title_text='Rasio Penempatan')
                st.plotly_chart(fig3, use_container_width=True)
        else: st.warning("Satu atau lebih kolom untuk scatter plot tidak ditemukan.")
        
    with plot_cols_2[1]: # FIG 4: Top 10 Rasio Lowongan/Pencari
        if rasio_lp_col in df_ratio.columns:
            with st.container(border=True):
                top_10_rasio = df_ratio.nlargest(10, rasio_lp_col)
                fig4 = px.bar(top_10_rasio, x=rasio_lp_col, y="Provinsi", orientation='h',
                              title=f"<b>Top 10 Rasio: Lowongan / Pencari</b>",
                              color_discrete_sequence=px.colors.qualitative.Safe,
                              text=rasio_lp_col)
                fig4.update_traces(texttemplate='%{text:.2f}', textposition='auto')
                fig4.update_layout(yaxis={'categoryorder':'total ascending'}, title_x=0.5, xaxis_title="Rasio", yaxis_title=None)
                st.plotly_chart(fig4, use_container_width=True)
        else: st.warning(f"Kolom '{rasio_lp_col}' tidak ditemukan untuk grafik rasio.")

with tab_tabel:
    st.subheader("Tabel Data Lengkap Ketenagakerjaan per Provinsi")
    # Kolom yang ingin ditampilkan dan diformat
    cols_to_display_in_table = ["Provinsi"] + list(column_map.values()) + ["Rasio Penempatan/Pencari"]
    # Filter hanya kolom yang ada di df
    cols_to_display_in_table = [col for col in cols_to_display_in_table if col in df.columns]

    format_dict_table = {}
    for col in cols_to_display_in_table:
        if "Rasio" in col: format_dict_table[col] = '{:,.2f}'
        elif "Jumlah" in col or "Laki-Laki" in col or "Perempuan" in col : format_dict_table[col] = '{:,.0f}'
    
    st.dataframe(df[cols_to_display_in_table].style.format(format_dict_table), use_container_width=True, hide_index=True)
    
    csv_export = df[cols_to_display_in_table].to_csv(index=False).encode("utf-8")
    st.download_button("📥 Download Data sebagai CSV", data=csv_export,
                       file_name=f"statistik_ketenagakerjaan_provinsi_{latest_doc.get('timestamp', datetime.now()).strftime('%Y%m%d')}.csv",
                       mime="text/csv")

with tab_peta:
    st.subheader("🗺️ Peta Distribusi Ketenagakerjaan")
    
    # Panggil fungsi untuk memuat GeoJSON (fungsi ini sudah di-cache)
    geojson_data = get_geojson_data() 
    
    if not geojson_data:
        st.error("Data GeoJSON tidak dapat dimuat. Peta tidak dapat ditampilkan. Periksa sidebar untuk detail error GeoJSON.")
    elif df.empty:
        st.warning("DataFrame kosong. Tidak ada data untuk ditampilkan di peta.")
    else:
        map_metric_options = {
            "Jumlah Pencari Kerja": "Pencari Kerja Jumlah",
            "Jumlah Lowongan Kerja": "Lowongan Kerja Jumlah",
            "Jumlah Penempatan Tenaga Kerja": "Penempatan Tenaga Kerja Jumlah",
            "Rasio Lowongan/Pencari": "Rasio Lowongan/Pencari", # Menggunakan data dari df_ratio
            "Rasio Penempatan/Pencari": "Rasio Penempatan/Pencari" # Menggunakan data dari df
        }
        
        selected_map_metric_display = st.selectbox(
            "Pilih Indikator untuk Ditampilkan di Peta:",
            options=list(map_metric_options.keys()),
            index=0, # Default ke metrik pertama
            key="map_indicator_selectbox" # Key unik untuk widget
        )
        selected_map_metric_col = map_metric_options[selected_map_metric_display]

        # Tentukan DataFrame mana yang akan digunakan berdasarkan metrik yang dipilih
        # Untuk rasio Lowongan/Pencari, kita gunakan df_ratio
        # Untuk metrik lain, kita gunakan df utama
        current_df_for_map = df_ratio if selected_map_metric_col == "Rasio Lowongan/Pencari" else df

        if selected_map_metric_col not in current_df_for_map.columns:
            st.error(f"Kolom '{selected_map_metric_col}' tidak ditemukan dalam DataFrame yang sesuai untuk pemetaan. Periksa `column_map` dan perhitungan rasio.")
        else:
            # Pilih skala warna berdasarkan jenis metrik
            if "Rasio" in selected_map_metric_display:
                color_scale = "RdYlGn" # Skala divergen cocok untuk rasio (misal, merah rendah, hijau tinggi)
                hover_format = ":.2f" # Format 2 desimal untuk rasio
            else: # Untuk jumlah
                color_scale = "Blues" # Skala sekuensial (misalnya, Biru muda ke tua)
                if selected_map_metric_display == "Jumlah Lowongan Kerja":
                    color_scale = "Oranges"
                elif selected_map_metric_display == "Jumlah Penempatan Tenaga Kerja":
                    color_scale = "Greens"
                hover_format = ":,.0f" # Format ribuan untuk jumlah

            # Tentukan nilai minimum dan maksimum untuk range warna, hindari (0,0)
            min_val = current_df_for_map[selected_map_metric_col].min()
            max_val = current_df_for_map[selected_map_metric_col].max()
            if min_val == max_val: # Jika semua nilai sama
                 if max_val == 0: range_color_map = (0, 1) # Hindari (0,0)
                 else: range_color_map = (min_val * 0.9, max_val * 1.1) # Beri sedikit rentang
            else:
                range_color_map = (min_val, max_val)

            # Membuat peta choropleth dengan Mapbox
            fig_map = px.choropleth_mapbox(
                current_df_for_map,
                geojson=geojson_data,
                locations='Provinsi_Clean',         # Kolom di DataFrame yang cocok dengan ID di GeoJSON
                featureidkey="properties.Propinsi", # Path ke ID fitur di GeoJSON (sesuaikan jika GeoJSON Anda berbeda)
                color=selected_map_metric_col,      # Kolom DataFrame untuk pewarnaan
                color_continuous_scale=color_scale, # Skala warna yang dipilih
                range_color=range_color_map,        # Rentang nilai untuk skala warna
                mapbox_style="carto-positron",      # Gaya peta dasar (opsi lain: "open-street-map", "stamen-terrain", dll.)
                zoom=3.8,                           # Tingkat zoom awal peta
                center={"lat": -2.5, "lon": 118},   # Pusat peta (Indonesia)
                opacity=0.7,                        # Opasitas layer warna
                hover_name="Provinsi",              # Kolom yang ditampilkan sebagai judul saat hover
                hover_data={                        # Data tambahan yang ditampilkan saat hover
                    selected_map_metric_col: hover_format, # Format angka sesuai jenis metrik
                    "Provinsi_Clean": False # Sembunyikan kolom internal ini dari hover
                },
                labels={selected_map_metric_col: selected_map_metric_display} # Label untuk legenda warna
            )
            
            fig_map.update_layout(
                title_text=f"<b>Peta Distribusi: {selected_map_metric_display} per Provinsi</b>", # Judul dinamis
                title_x=0.5, # Judul rata tengah
                height=650,  # Tinggi peta
                margin={"r":0,"t":40,"l":0,"b":0}, # Margin peta
                coloraxis_colorbar={
                    "title": selected_map_metric_display, # Judul untuk bar legenda warna
                    "thickness": 15
                }
            )
            
            st.plotly_chart(fig_map, use_container_width=True)

            # Tambahan: Menampilkan tabel data yang digunakan untuk peta (opsional)
            with st.expander("Lihat Data Tabel untuk Peta Saat Ini (Diurutkan)"):
                display_cols_map_table = ["Provinsi", selected_map_metric_col]
                st.dataframe(
                    current_df_for_map[display_cols_map_table]
                    .sort_values(selected_map_metric_col, ascending=False)
                    .style.format({selected_map_metric_col: hover_format}), # Gunakan format yang sama
                    height=300,
                    use_container_width=True,
                    hide_index=True
                )

# --------------------------------------------------------------------------
# 9. FOOTER
# --------------------------------------------------------------------------
st.markdown("---")
st.markdown(f"""
<div style="text-align: center; font-size: 0.9em; color: #7f8c8d;">
    <p>Sumber Data: Badan Pusat Statistik (BPS) | Diperbarui dari DB: {latest_doc.get('timestamp', datetime.now()).strftime('%d %B %Y, %H:%M:%S')}</p>
    <p><i>Dashboard ini bersifat demonstrasi dan interpretasi data adalah tanggung jawab pengguna.</i></p>
</div>
""", unsafe_allow_html=True)