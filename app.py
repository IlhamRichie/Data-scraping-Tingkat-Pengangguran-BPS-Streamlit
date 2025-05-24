# app_final.py

import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient, errors as pymongo_errors
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
import numpy as np
import requests
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
import html

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s")

PAGE_TITLE: str = "Dashboard Ketenagakerjaan Indonesia (BPS)"
PAGE_ICON: str = "📊"
MONGO_DATABASE_NAME: str = os.getenv("MONGO_DATABASE_NAME", "bps_db")
BPS_ID_TABEL_TARGET: str = "TE9UUDFUV3Bpa3ovMHJJVGtuUHZVdz09" # Sesuai scraper
BPS_TAHUN_TARGET: str = "2024" # Sesuai scraper

CLEANED_ID_TABEL_TARGET = BPS_ID_TABEL_TARGET.replace('=', '').replace('/', '')
MONGO_COLLECTION_NAME: str = os.getenv("MONGO_COLLECTION_NAME", f"data_bps_{CLEANED_ID_TABEL_TARGET}_{BPS_TAHUN_TARGET}")

GEOJSON_URL: str = "https://raw.githubusercontent.com/superpikar/indonesia-geojson/master/indonesia-province-simple.json"
GEOJSON_FEATURE_ID_KEY: str = "properties.Propinsi"

# !! PENTING SEKALI: VERIFIKASI KUNCI ('id_var') DI BAWAH INI !!
# Gunakan fitur debug di sidebar aplikasi untuk membandingkan dengan
# "Definisi Variabel Aktual dari API BPS (tersimpan di DB)".
# Kunci ('id_var') di COLUMN_MAP ini HARUS SAMA PERSIS dengan id_var dari API.
COLUMN_MAP: Dict[str, str] = {
    "iihviv2ocw": "Pencari Kerja Terdaftar - Laki-Laki",
    "ijuxru3lvl": "Pencari Kerja Terdaftar - Perempuan",
    "b1xjkdn0vw": "Pencari Kerja Terdaftar - Jumlah",
    "kgpd8jp9bs": "Lowongan Kerja Terdaftar - Laki-Laki",
    "b4ox1vczyq": "Lowongan Kerja Terdaftar - Perempuan",
    "yeloqirlpp": "Lowongan Kerja Terdaftar - Jumlah",
    "2ikzujodce": "Penempatan Tenaga Kerja - Laki-Laki",
    "lfbbv5gdz2": "Penempatan Tenaga Kerja - Perempuan",
    "ytis9poht5": "Penempatan Tenaga Kerja - Jumlah",
    "ksybbjfehm": "Pencari Kerja Terdaftar - Laki-Laki atau Perempuan" # Verifikasi id_var ini!
}

st.set_page_config(page_title=PAGE_TITLE, layout="wide", page_icon=PAGE_ICON, initial_sidebar_state="expanded")

load_dotenv()
MONGO_URI: Optional[str] = os.getenv("MONGO_URI")

@st.cache_resource(ttl=3600)
def init_connection() -> Optional[MongoClient]: #... (fungsi init_connection sama seperti sebelumnya)
    if not MONGO_URI:
        st.sidebar.error("MONGO_URI tidak diatur. Atur di Secrets (Cloud) atau .env (Lokal).")
        logging.error("MONGO_URI tidak diatur.")
        return None
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000)
        client.admin.command("ping")
        logging.info(f"Berhasil terhubung ke MongoDB (DB: {MONGO_DATABASE_NAME}).")
        return client
    except Exception as e:
        st.sidebar.error(f"Error koneksi MongoDB: {e}")
        logging.error(f"Error init_connection: {e}", exc_info=True)
    return None

client = init_connection()
if not client: st.error("Kritis: Gagal terhubung ke MongoDB.", icon="🚨"); st.stop()
db = client[MONGO_DATABASE_NAME]
collection = db[MONGO_COLLECTION_NAME]
st.sidebar.success(f"Terhubung ke MongoDB (Collection: {MONGO_COLLECTION_NAME}).")

@st.cache_data(ttl=900)
def get_latest_data_from_db(target_id_tabel: str, target_tahun: str) -> Optional[Dict[str, Any]]: #... (fungsi get_latest_data_from_db sama)
    try:
        query_filter = {"bps_id_tabel": target_id_tabel, "bps_tahun_data_request": target_tahun}
        latest_document = collection.find_one(query_filter)
        if latest_document:
            logging.info(f"Data terbaru diambil dari DB (filter: {query_filter}), ts scrape: {latest_document.get('timestamp_scraped_utc')}")
        else:
            logging.warning(f"Tidak ada dokumen ditemukan di MongoDB dengan filter: {query_filter}.")
        return latest_document
    except Exception as e:
        st.error(f"Error mengambil data dari MongoDB: {e}")
        logging.error(f"Error get_latest_data_from_db: {e}", exc_info=True)
    return None

@st.cache_data(ttl=86400)
def get_geojson_data(url: str) -> Optional[Dict[str, Any]]: #... (fungsi get_geojson_data sama)
    try:
        st.sidebar.info(f"Memuat GeoJSON dari: {url.split('/')[-1]}")
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        geojson = response.json()
        if isinstance(geojson, dict) and geojson.get("type") in ["FeatureCollection", "Feature"]:
            st.sidebar.success("GeoJSON berhasil dimuat.")
            return geojson
        st.sidebar.warning(f"Format GeoJSON tidak sesuai dari URL: {url}")
    except Exception as e:
        st.sidebar.error(f"Error memuat GeoJSON: {e}")
        logging.error(f"Error get_geojson_data dari {url}: {e}", exc_info=True)
    return None


latest_doc = get_latest_data_from_db(BPS_ID_TABEL_TARGET, BPS_TAHUN_TARGET)
geojson_data = get_geojson_data(GEOJSON_URL)

if not latest_doc:
    st.error(f"⚠️ Tidak ada data untuk ID Tabel '{BPS_ID_TABEL_TARGET}' Tahun '{BPS_TAHUN_TARGET}'. Pastikan scraper sudah jalan & simpan ke koleksi '{MONGO_COLLECTION_NAME}'.", icon="🚨")
    st.stop()
list_data_provinsi_mentah: Optional[List[Dict[str, Any]]] = latest_doc.get("data_provinsi")
if not list_data_provinsi_mentah or not isinstance(list_data_provinsi_mentah, list):
    st.error(f"⚠️ 'data_provinsi' tidak ditemukan/valid di dokumen MongoDB.", icon="🚨")
    with st.expander("Detail Dokumen Mentah"): st.json(latest_doc or "Tidak ada dokumen.")
    st.stop()

doc_timestamp = latest_doc.get("timestamp_scraped_utc", "N/A")
doc_timestamp_str = str(doc_timestamp)
if isinstance(doc_timestamp, datetime):
    try: doc_timestamp_str = doc_timestamp.astimezone(timezone.utc).strftime("%d %B %Y, %H:%M:%S %Z")
    except: doc_timestamp_str = doc_timestamp.strftime("%d %B %Y, %H:%M:%S (Waktu DB)")

st.sidebar.info(f"Data dari DB per: {doc_timestamp_str}")
metadata_tabel_scraped = latest_doc.get("metadata_tabel_scraped")
if isinstance(metadata_tabel_scraped, dict):
    st.sidebar.caption(f"Judul Data (DB): {metadata_tabel_scraped.get('judul_tabel', 'N/A')}")
    st.sidebar.caption(f"Tahun Aktual (DB): {latest_doc.get('bps_tahun_data_actual', metadata_tabel_scraped.get('tahun_data', 'N/A'))}")
st.sidebar.caption(f"ID Tabel Target: {latest_doc.get('bps_id_tabel', BPS_ID_TABEL_TARGET)}")

def parse_bps_value(raw_value_object: Any) -> float: #... (fungsi parse_bps_value sama)
    raw_value_string = "0"
    if isinstance(raw_value_object, dict):
        possible_keys = ["value_raw", "val", "nilai"]
        for key in possible_keys:
            if key in raw_value_object and raw_value_object[key] is not None:
                raw_value_string = str(raw_value_object[key]); break
        else:
            if len(raw_value_object) == 1 and list(raw_value_object.values())[0] is not None:
                 raw_value_string = str(list(raw_value_object.values())[0])
    elif isinstance(raw_value_object, (str, int, float)) and raw_value_object is not None:
        raw_value_string = str(raw_value_object)
    cleaned_value_string = raw_value_string.replace(".", "").replace(",", ".")
    try: return float(cleaned_value_string)
    except ValueError: return 0.0

def create_dataframe_from_bps_data(data_prov_list: List[Dict[str, Any]], col_map: Dict[str, str]) -> Tuple[pd.DataFrame, Dict[str, Dict[str, Any]], List[Dict[str, Any]]]: #... (fungsi create_dataframe_from_bps_data sama)
    rows, debug_rows, missing_keys = [], [], {}
    for idx, item_prov in enumerate(data_prov_list):
        label_prov = item_prov.get("label", f"Prov Unknown #{idx}")
        if label_prov.strip().upper() == "INDONESIA": continue
        row_data: Dict[str, Any] = {"Provinsi": label_prov}
        vars_prov: Dict[str, Any] = item_prov.get("variables", {})
        debug_item: Dict[str, Any] = {"Provinsi": label_prov, "_API_VAR_KEYS": list(vars_prov.keys())}
        for api_id, col_name in col_map.items():
            raw_val_obj = vars_prov.get(api_id)
            if raw_val_obj is None:
                row_data[col_name] = 0.0
                debug_item[f"NOT_FOUND: {api_id} (as {col_name})"] = "MISSING"
                missing_keys.setdefault(api_id, {"col_name": col_name, "miss_count": 0})["miss_count"] += 1
            else:
                row_data[col_name] = parse_bps_value(raw_val_obj)
                debug_item[f"RAW: {api_id} ({col_name})"] = str(raw_val_obj)
            debug_item[f"PROCESSED: {col_name}"] = row_data.get(col_name)
        rows.append(row_data)
        if idx < 3: debug_rows.append(debug_item)
    return pd.DataFrame(rows), missing_keys, debug_rows

df_provinsi, keys_not_found_in_api, debug_data_processing_examples = create_dataframe_from_bps_data(list_data_provinsi_mentah, COLUMN_MAP)

# --- Enhanced Debugging Sidebar ---
with st.sidebar.expander("🔬 WAJIB DICEK: Validasi `COLUMN_MAP`", expanded=True):
    st.error("PERHATIAN: Pastikan KUNCI (`id_var`) di `COLUMN_MAP` di bawah ini SAMA PERSIS dengan KUNCI (`id_var`) di 'Definisi Variabel Aktual dari API BPS'. Jika berbeda, data di grafik akan salah (nol).")
    st.subheader("`COLUMN_MAP` yang Digunakan Aplikasi Ini:")
    st.json(COLUMN_MAP)

    scraped_var_defs = None
    if isinstance(metadata_tabel_scraped, dict) and "kolom" in metadata_tabel_scraped:
        scraped_var_defs = metadata_tabel_scraped["kolom"]
        if isinstance(scraped_var_defs, dict):
            st.subheader("Definisi Variabel Aktual dari API BPS (tersimpan di DB):")
            st.caption("Cocokkan KUNCI (`id_var`) dari `COLUMN_MAP` Anda dengan KUNCI di sini.")
            st.json(scraped_var_defs, expanded=False)
        else:
            st.warning("Format `metadata_tabel_scraped.kolom` tidak sesuai (bukan dictionary).")
            scraped_var_defs = None # Reset jika format salah
    else:
        st.warning("Metadata 'kolom' (definisi variabel dari API) tidak ditemukan di data DB. Scraper perlu menyimpan `metadata_tabel_scraped.kolom` untuk validasi otomatis.")

    # Tabel Perbandingan untuk Validasi COLUMN_MAP
    validation_data = []
    for app_id_var, app_col_name in COLUMN_MAP.items():
        status = "❌ TIDAK DITEMUKAN DI API"
        api_nama_variabel = "N/A"
        if scraped_var_defs and app_id_var in scraped_var_defs:
            status = "✅ DITEMUKAN"
            api_nama_variabel = scraped_var_defs[app_id_var].get("nama_variabel", "Nama variabel tidak ada di API def")
        elif keys_not_found_in_api.get(app_id_var): # Fallback jika scraped_var_defs tidak ada tapi keys_not_found ada
             status = "❌ TIDAK DITEMUKAN DI DATA PROVINSI"

        validation_data.append({
            "ID Var (COLUMN_MAP)": app_id_var,
            "Nama Kolom Aplikasi": app_col_name,
            "Status Pemetaan": status,
            "Nama Var. Aktual di API (jika ditemukan)": api_nama_variabel
        })
    st.subheader("Tabel Validasi Pemetaan `COLUMN_MAP`:")
    st.dataframe(pd.DataFrame(validation_data), use_container_width=True, hide_index=True)
    
    if not df_provinsi.empty:
        st.subheader("Contoh Proses Konversi Variabel (Beberapa Provinsi Awal)")
        st.json(debug_data_processing_examples, expanded=False)
        if keys_not_found_in_api: # Hanya tampilkan jika ada yang tidak ditemukan
            st.subheader("Ringkasan ID Variabel dari `COLUMN_MAP` yang TIDAK DITEMUKAN di Data Provinsi:")
            st.warning("ID Variabel (KUNCI) berikut dari `COLUMN_MAP` tidak ditemukan di data provinsi yang di-scrape. Perbaiki `COLUMN_MAP` Anda!")
            st.json(keys_not_found_in_api)
        elif scraped_var_defs: # Jika semua ada di data provinsi, cek lagi vs definisi kolom API
            mismatched_but_found_in_data = []
            for app_id_var in COLUMN_MAP.keys():
                if app_id_var not in scraped_var_defs:
                    mismatched_but_found_in_data.append(f"'{app_id_var}' (ada di data provinsi) tapi tidak ada di definisi kolom API BPS.")
            if mismatched_but_found_in_data:
                 st.warning("Beberapa id_var di COLUMN_MAP ada di data provinsi tapi tidak terdefinisi di metadata 'kolom' dari API:" + "; ".join(mismatched_but_found_in_data))


        st.subheader("DataFrame `df_provinsi` (Info & 5 Baris Awal)")
        st.dataframe(df_provinsi.head())
    else: st.warning("DataFrame `df_provinsi` kosong setelah pemrosesan.")

if df_provinsi.empty:
    st.error("DataFrame kosong setelah pemrosesan. Visualisasi tidak bisa ditampilkan. Periksa `COLUMN_MAP` Anda!", icon="🚨")
    st.stop()

# --- 6. Transformasi Data Lanjutan --- (Sama seperti sebelumnya)
df_calc = df_provinsi.copy()
df_calc['Provinsi_Clean'] = df_calc['Provinsi'].str.upper() \
    .str.replace('DKI ', '', regex=False).str.replace('DI ', '', regex=False) \
    .str.replace('DAERAH ISTIMEWA ', '', regex=False) \
    .str.replace(r'KEP\.\s', 'KEPULAUAN ', regex=True).str.replace('KEP ', 'KEPULAUAN ', regex=False) \
    .str.replace(r'PROP\.\s', 'PROVINSI ', regex=True).str.replace('PROV ', 'PROVINSI ', regex=False) \
    .str.strip()

pencari_lk_col = COLUMN_MAP.get("iihviv2ocw")
pencari_pr_col = COLUMN_MAP.get("ijuxru3lvl")
pencari_jml_col = COLUMN_MAP.get("b1xjkdn0vw")
lowongan_lk_col = COLUMN_MAP.get("kgpd8jp9bs")
lowongan_pr_col = COLUMN_MAP.get("b4ox1vczyq")
lowongan_jml_col = COLUMN_MAP.get("yeloqirlpp")
penempatan_lk_col = COLUMN_MAP.get("2ikzujodce")
penempatan_pr_col = COLUMN_MAP.get("lfbbv5gdz2")
penempatan_jml_col = COLUMN_MAP.get("ytis9poht5")

rasio_lp_col, rasio_pp_col = "Rasio Lowongan/Pencari", "Rasio Penempatan/Pencari"
if pencari_jml_col and lowongan_jml_col and penempatan_jml_col and \
   all(col in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[col]) for col in [pencari_jml_col, lowongan_jml_col, penempatan_jml_col]):
    df_calc[rasio_lp_col] = np.where(df_calc[pencari_jml_col] > 0, df_calc[lowongan_jml_col] / df_calc[pencari_jml_col], 0.0)
    df_calc[rasio_pp_col] = np.where(df_calc[pencari_jml_col] > 0, df_calc[penempatan_jml_col] / df_calc[pencari_jml_col], 0.0)
    for col_r in [rasio_lp_col, rasio_pp_col]: df_calc[col_r] = df_calc[col_r].replace([np.inf, -np.inf], 0.0).round(4)
else:
    st.warning(f"Tidak dapat menghitung rasio. Kolom dasar mungkin tidak ada/valid karena `COLUMN_MAP` belum tepat.")
    df_calc[rasio_lp_col], df_calc[rasio_pp_col] = 0.0, 0.0

# --- 7. Layout Utama & Metrik Nasional --- (Sama seperti sebelumnya)
st.title(PAGE_TITLE)
st.markdown(f"Data dari DB per: {doc_timestamp_str} (Tahun Data Aktual: {latest_doc.get('bps_tahun_data_actual', 'N/A')})")
total_pencari = df_calc[pencari_jml_col].sum() if pencari_jml_col and pencari_jml_col in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[pencari_jml_col]) else 0
total_lowongan = df_calc[lowongan_jml_col].sum() if lowongan_jml_col and lowongan_jml_col in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[lowongan_jml_col]) else 0
total_penempatan = df_calc[penempatan_jml_col].sum() if penempatan_jml_col and penempatan_jml_col in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[penempatan_jml_col]) else 0
st.subheader("Ringkasan Nasional (Agregat dari Provinsi)")
col_met1, col_met2, col_met3 = st.columns(3)
col_met1.metric("Total Pencari Kerja", f"{total_pencari:,.0f}")
col_met2.metric("Total Lowongan Kerja", f"{total_lowongan:,.0f}")
col_met3.metric("Total Penempatan", f"{total_penempatan:,.0f}")
st.markdown("---")

# --- 8. Tabs untuk Visualisasi --- (Logika plot sama, tapi definisi hover lebih eksplisit)
tab_ringkasan, tab_gender, tab_hubungan, tab_tabel, tab_peta = st.tabs([
    "📊 Ringkasan Umum", "🚻 Analisis Gender", "🔗 Analisis Hubungan",
    "📋 Tabel Data", "🗺️ Peta Distribusi"
])

def safe_plot_bar(df: pd.DataFrame, val_col: Optional[str], cat_col: str, title: str, orientation: str = 'v', color_seq=None, is_ratio=False): #... (fungsi safe_plot_bar sama seperti versi terakhir, dengan hover eksplisit)
    if not val_col: st.warning(f"Nama kolom untuk nilai pada grafik '{title}' tidak terdefinisi (cek `COLUMN_MAP`)."); return
    if val_col in df.columns and cat_col in df.columns and pd.api.types.is_numeric_dtype(df[val_col]):
        with st.container(border=True):
            top_n = df.dropna(subset=[val_col]).nlargest(10, val_col)
            if top_n.empty or (top_n[val_col].sum() == 0 and not is_ratio):
                st.info(f"Tidak ada data signifikan (>0) untuk ditampilkan pada grafik '{title}'.")
                return
            
            fig_x, fig_y = (cat_col, val_col) if orientation == 'v' else (val_col, cat_col)
            text_format_on_bar = '{text:,.2%}' if is_ratio else '{text:,.0f}'
            hover_format = ':.2%' if is_ratio else ',.0f'

            fig = px.bar(top_n, x=fig_x, y=fig_y, orientation=orientation,
                         title=f"<b>{title}</b>", color_discrete_sequence=color_seq,
                         text=val_col,
                         hover_name=cat_col, # Menampilkan nama provinsi/kategori di judul hover
                         hover_data={val_col: hover_format, cat_col:False} # Menampilkan nilai dengan format, sembunyikan kategori karena sudah di hover_name
                        )
            fig.update_traces(texttemplate=text_format_on_bar, 
                              textposition='outside' if orientation=='h' and not is_ratio and top_n[val_col].max() > 0 else 'auto',
                              textfont_size=10)
            
            axis_title = "Rasio" if is_ratio else "Jumlah"
            if orientation == 'h': fig.update_layout(xaxis_title=axis_title, yaxis_title=None, yaxis={'categoryorder':'total ascending'})
            else: fig.update_layout(yaxis_title=axis_title, xaxis_title=None, xaxis={'categoryorder':'total descending'})
            fig.update_layout(title_x=0.5, uniformtext_minsize=8, uniformtext_mode='hide')
            st.plotly_chart(fig, use_container_width=True)
    else: st.warning(f"Grafik '{title}' tidak dapat ditampilkan. Kolom '{val_col}' atau '{cat_col}' tidak valid/lengkap.")


with tab_ringkasan: #... (Konten tab_ringkasan sama seperti versi terakhir)
    st.subheader("Peringkat Provinsi (Top 10)")
    plot_cols_r1 = st.columns(2)
    with plot_cols_r1[0]: safe_plot_bar(df_calc, val_col=pencari_jml_col, cat_col="Provinsi", title=f"Top 10: {pencari_jml_col or 'Pencari Kerja Jumlah'}", orientation='h', color_seq=px.colors.qualitative.Plotly)
    with plot_cols_r1[1]: safe_plot_bar(df_calc, val_col=lowongan_jml_col, cat_col="Provinsi", title=f"Top 10: {lowongan_jml_col or 'Lowongan Kerja Jumlah'}", orientation='v', color_seq=px.colors.qualitative.Pastel)
    st.markdown("<br>", unsafe_allow_html=True)
    plot_cols_r2 = st.columns(2)
    with plot_cols_r2[0]:
        scatter_req_cols = [pencari_jml_col, penempatan_jml_col, lowongan_jml_col, rasio_pp_col]
        if pencari_jml_col and penempatan_jml_col and lowongan_jml_col and rasio_pp_col and \
           all(col in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[col]) for col in scatter_req_cols):
            with st.container(border=True):
                fig_scatter_penempatan = px.scatter(df_calc, x=pencari_jml_col, y=penempatan_jml_col, size=lowongan_jml_col, color=rasio_pp_col,
                                          color_continuous_scale=px.colors.sequential.Plasma, hover_name="Provinsi",
                                          hover_data={pencari_jml_col: ":,.0f", penempatan_jml_col: ":,.0f", lowongan_jml_col: ":,.0f", rasio_pp_col: ":.2%"},
                                          title="<b>Analisis Penempatan vs Pencari Kerja</b>",
                                          labels={pencari_jml_col: "Pencari Kerja (Jml)", penempatan_jml_col: "Penempatan (Jml)", lowongan_jml_col: "Lowongan (Jml)", rasio_pp_col: "Rasio Penempatan"},
                                          size_max=50, height=500)
                fig_scatter_penempatan.update_layout(title_x=0.5, coloraxis_colorbar_title_text='Rasio Penempatan')
                st.plotly_chart(fig_scatter_penempatan, use_container_width=True)
        else: st.warning(f"Scatter plot Penempatan vs Pencari tidak dapat ditampilkan. Kolom dibutuhkan tidak valid/lengkap.")
    with plot_cols_r2[1]: safe_plot_bar(df_calc, val_col=rasio_lp_col, cat_col="Provinsi", title=f"Top 10 Rasio: Lowongan / Pencari", orientation='h', color_seq=px.colors.qualitative.Safe, is_ratio=True)


with tab_gender: #... (Konten tab_gender sama seperti versi terakhir, dengan hover eksplisit)
    st.subheader("Analisis Gender dalam Ketenagakerjaan")
    st.caption("Menampilkan Top 10 Provinsi berdasarkan jumlah total pada kategori masing-masing.")
    hover_format_jumlah = ":,.0f"
    
    if pencari_lk_col and pencari_pr_col and pencari_jml_col and \
       all(col in df_calc.columns for col in ["Provinsi", pencari_lk_col, pencari_pr_col, pencari_jml_col]):
        with st.container(border=True):
            top_provinces_pencari = df_calc.nlargest(10, pencari_jml_col)
            if not top_provinces_pencari.empty:
                df_melted_pencari = top_provinces_pencari.melt(id_vars=["Provinsi"], value_vars=[pencari_lk_col, pencari_pr_col], var_name="Jenis Kelamin", value_name="Jumlah Pencari Kerja")
                fig_gender_pencari = px.bar(df_melted_pencari, x="Provinsi", y="Jumlah Pencari Kerja", color="Jenis Kelamin", barmode="group",
                                            title=f"Pencari Kerja L/P (Top 10 Prov. by Total)", text_auto=True,
                                            hover_name="Provinsi", hover_data={"Jenis Kelamin": True, "Jumlah Pencari Kerja": hover_format_jumlah},
                                            labels={"Jumlah Pencari Kerja": "Jumlah Orang"}, category_orders={"Provinsi": top_provinces_pencari["Provinsi"].tolist()})
                fig_gender_pencari.update_traces(texttemplate='%{text:,.0f}')
                fig_gender_pencari.update_layout(title_x=0.5, uniformtext_minsize=8, uniformtext_mode='hide')
                st.plotly_chart(fig_gender_pencari, use_container_width=True)
            else: st.info(f"Tidak ada data pencari kerja yang signifikan untuk ditampilkan pada analisis gender.")
    else: st.warning(f"Analisis gender pencari kerja tidak bisa ditampilkan karena kolom tidak ditemukan/valid.")

    if lowongan_lk_col and lowongan_pr_col and lowongan_jml_col and \
       all(col in df_calc.columns for col in ["Provinsi", lowongan_lk_col, lowongan_pr_col, lowongan_jml_col]):
        with st.container(border=True):
            top_provinces_lowongan = df_calc.nlargest(10, lowongan_jml_col)
            if not top_provinces_lowongan.empty:
                df_melted_lowongan = top_provinces_lowongan.melt(id_vars=["Provinsi"], value_vars=[lowongan_lk_col, lowongan_pr_col], var_name="Jenis Kelamin", value_name="Jumlah Lowongan")
                fig_gender_lowongan = px.bar(df_melted_lowongan, x="Provinsi", y="Jumlah Lowongan", color="Jenis Kelamin", barmode="group",
                                            title=f"Lowongan Kerja L/P (Top 10 Prov. by Total)", text_auto=True,
                                            hover_name="Provinsi", hover_data={"Jenis Kelamin": True, "Jumlah Lowongan": hover_format_jumlah},
                                            labels={"Jumlah Lowongan": "Jumlah Jabatan"}, category_orders={"Provinsi": top_provinces_lowongan["Provinsi"].tolist()})
                fig_gender_lowongan.update_traces(texttemplate='%{text:,.0f}')
                fig_gender_lowongan.update_layout(title_x=0.5, uniformtext_minsize=8, uniformtext_mode='hide')
                st.plotly_chart(fig_gender_lowongan, use_container_width=True)
            else: st.info(f"Tidak ada data lowongan kerja yang signifikan untuk ditampilkan pada analisis gender.")
    else: st.warning(f"Analisis gender lowongan kerja tidak bisa ditampilkan karena kolom tidak ditemukan/valid.")

    if penempatan_lk_col and penempatan_pr_col and penempatan_jml_col and \
       all(col in df_calc.columns for col in ["Provinsi", penempatan_lk_col, penempatan_pr_col, penempatan_jml_col]):
        with st.container(border=True):
            top_provinces_penempatan = df_calc.nlargest(10, penempatan_jml_col)
            if not top_provinces_penempatan.empty:
                df_melted_penempatan = top_provinces_penempatan.melt(id_vars=["Provinsi"], value_vars=[penempatan_lk_col, penempatan_pr_col], var_name="Jenis Kelamin", value_name="Jumlah Penempatan")
                fig_gender_penempatan = px.bar(df_melted_penempatan, x="Provinsi", y="Jumlah Penempatan", color="Jenis Kelamin", barmode="group",
                                            title=f"Penempatan L/P (Top 10 Prov. by Total)", text_auto=True,
                                            hover_name="Provinsi", hover_data={"Jenis Kelamin": True, "Jumlah Penempatan": hover_format_jumlah},
                                            labels={"Jumlah Penempatan": "Jumlah Orang"}, category_orders={"Provinsi": top_provinces_penempatan["Provinsi"].tolist()})
                fig_gender_penempatan.update_traces(texttemplate='%{text:,.0f}')
                fig_gender_penempatan.update_layout(title_x=0.5, uniformtext_minsize=8, uniformtext_mode='hide')
                st.plotly_chart(fig_gender_penempatan, use_container_width=True)
            else: st.info(f"Tidak ada data penempatan kerja yang signifikan untuk ditampilkan pada analisis gender.")
    else: st.warning(f"Analisis gender penempatan kerja tidak bisa ditampilkan karena kolom tidak ditemukan/valid.")


with tab_hubungan: #... (Konten tab_hubungan sama seperti versi terakhir dengan perbaikan kondisi)
    st.subheader("Analisis Hubungan Antar Indikator Ketenagakerjaan")
    required_numeric_cols_for_scatter1 = [pencari_jml_col, lowongan_jml_col, penempatan_jml_col]
    provinsi_col_exists_scatter1 = "Provinsi" in df_calc.columns
    numeric_cols_valid_scatter1 = pencari_jml_col and lowongan_jml_col and penempatan_jml_col and all(
        col in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[col]) for col in required_numeric_cols_for_scatter1)

    if provinsi_col_exists_scatter1 and numeric_cols_valid_scatter1:
        with st.container(border=True):
            fig_lk_vs_pk = px.scatter(df_calc, x=pencari_jml_col, y=lowongan_jml_col, size=penempatan_jml_col, color="Provinsi",
                                      hover_name="Provinsi", title="Hubungan: Total Pencari Kerja vs Total Lowongan Kerja (Ukuran Bubble: Total Penempatan)",
                                      hover_data={pencari_jml_col:':,.0f', lowongan_jml_col:':,.0f', penempatan_jml_col:':,.0f', "Provinsi":False},
                                      labels={pencari_jml_col: "Total Pencari Kerja", lowongan_jml_col: "Total Lowongan Kerja", penempatan_jml_col: "Total Penempatan"},
                                      size_max=40, height=550)
            fig_lk_vs_pk.update_layout(title_x=0.5, showlegend=False)
            st.plotly_chart(fig_lk_vs_pk, use_container_width=True)
    else:
        missing_details_scatter1 = []
        if not provinsi_col_exists_scatter1: missing_details_scatter1.append("'Provinsi' column is missing.")
        for col_name_var in required_numeric_cols_for_scatter1: # col_name_var is the variable holding the actual column name string
            actual_col_name = col_name_var # It's already the name
            if not actual_col_name or actual_col_name not in df_calc.columns: missing_details_scatter1.append(f"Column '{actual_col_name or 'N/A'}' is missing.")
            elif not pd.api.types.is_numeric_dtype(df_calc[actual_col_name]): missing_details_scatter1.append(f"Column '{actual_col_name}' not numeric (type: {df_calc[actual_col_name].dtype}).")
        st.warning(f"Scatter plot Lowongan vs Pencari tidak dapat ditampilkan. Masalah: {'; '.join(missing_details_scatter1) if missing_details_scatter1 else 'Kolom tidak lengkap/valid.'}")

    numeric_cols_for_corr = [pencari_lk_col, pencari_pr_col, pencari_jml_col, lowongan_lk_col, lowongan_pr_col, lowongan_jml_col, penempatan_lk_col, penempatan_pr_col, penempatan_jml_col, rasio_lp_col, rasio_pp_col]
    valid_numeric_cols_for_corr = [col for col in numeric_cols_for_corr if col and col in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[col])]
    if len(valid_numeric_cols_for_corr) > 2 :
        with st.container(border=True):
            corr_df = df_calc[valid_numeric_cols_for_corr].fillna(0)
            if not corr_df.empty and not corr_df.isnull().all().all() and len(corr_df.columns) >1: # Need at least 2 cols for .corr()
                corr_matrix = corr_df.corr()
                fig_corr = px.imshow(corr_matrix, text_auto=".2f", aspect="auto", color_continuous_scale='RdBu_r', zmin=-1, zmax=1, title="Matriks Korelasi Antar Indikator")
                fig_corr.update_layout(title_x=0.5, height=700, coloraxis_colorbar_tickformat=".2f")
                fig_corr.update_xaxes(tickangle=-45)
                st.plotly_chart(fig_corr, use_container_width=True)
            else: st.info("Tidak ada data yang valid untuk dihitung korelasinya setelah filtering (butuh min. 2 kolom numerik).")
    else: st.warning("Tidak cukup kolom numerik valid untuk matriks korelasi (dibutuhkan >2).")


with tab_tabel: #... (Konten tab_tabel sama seperti versi terakhir)
    st.subheader("Tabel Data Lengkap Ketenagakerjaan per Provinsi")
    cols_from_map_valid = [name for id_var, name in COLUMN_MAP.items() if name and name in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[name])]
    cols_rasio_valid = [col for col in [rasio_lp_col, rasio_pp_col] if col and col in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[col])]
    display_order_cols = ["Provinsi"] + \
                     [COLUMN_MAP[id_var] for id_var in COLUMN_MAP if COLUMN_MAP.get(id_var) and COLUMN_MAP.get(id_var) in cols_from_map_valid] + \
                     sorted(list(set(cols_rasio_valid)))
    cols_to_display_in_table = []
    for col in display_order_cols:
        if col and col not in cols_to_display_in_table and col in df_calc.columns: cols_to_display_in_table.append(col)
    format_dict_table = {"Provinsi": None}
    for col_name in cols_to_display_in_table:
        if col_name == "Provinsi": continue
        format_dict_table[col_name] = '{:,.2%}' if "Rasio" in col_name else '{:,.0f}'
    if "Provinsi" in cols_to_display_in_table and len(cols_to_display_in_table) > 1:
        st.dataframe(df_calc[cols_to_display_in_table].style.format(format_dict_table, na_rep="-"), use_container_width=True, hide_index=True, height=600)
        csv_export = df_calc[cols_to_display_in_table].to_csv(index=False).encode("utf-8")
        ts_for_file = datetime.now().strftime('%Y%m%d_%H%M')
        if isinstance(latest_doc.get("timestamp_scraped_utc"), datetime):
            try: ts_for_file = latest_doc['timestamp_scraped_utc'].astimezone(timezone.utc).strftime('%Y%m%d_%H%M')
            except: ts_for_file = latest_doc['timestamp_scraped_utc'].strftime('%Y%m%d_%H%M')
        st.download_button("📥 Download Data sebagai CSV", data=csv_export, file_name=f"statistik_ketenagakerjaan_prov_{ts_for_file}.csv", mime="text/csv")
    else: st.warning("Tidak ada data valid untuk ditampilkan dalam tabel.")


with tab_peta: #... (Konten tab_peta sama seperti versi terakhir, menggunakan choropleth_map)
    st.subheader("🗺️ Peta Distribusi Ketenagakerjaan")
    if not geojson_data: st.error("Data GeoJSON tidak dapat dimuat.", icon="🗺️")
    elif df_calc.empty or 'Provinsi_Clean' not in df_calc.columns: st.warning("DataFrame kosong atau 'Provinsi_Clean' tidak ada.", icon="🗺️")
    else:
        map_opts_all = {key: val for key, val in { # Gunakan nama kolom dari variabel yang sudah di-resolve
            (pencari_jml_col or "Pencari Kerja Jumlah"): pencari_jml_col,
            (lowongan_jml_col or "Lowongan Kerja Jumlah"): lowongan_jml_col,
            (penempatan_jml_col or "Penempatan Tenaga Kerja Jumlah"): penempatan_jml_col,
            rasio_lp_col: rasio_lp_col,
            rasio_pp_col: rasio_pp_col
        }.items() if val}
        
        map_opts_valid = {disp: col for disp, col in map_opts_all.items() if col and col in df_calc.columns and pd.api.types.is_numeric_dtype(df_calc[col])}
        if not map_opts_valid: st.warning("Tidak ada metrik valid untuk peta.", icon="🗺️")
        else:
            sel_map_metric_disp = st.selectbox("Pilih Indikator Peta:", options=list(map_opts_valid.keys()), index=0)
            sel_map_metric_col = map_opts_valid[sel_map_metric_disp]
            color_s, hover_f = ("Blues", ":,.0f")
            if "Rasio" in sel_map_metric_disp: color_s, hover_f = "RdYlGn", ":.2%"
            elif "Lowongan" in sel_map_metric_disp: color_s = "Oranges"
            elif "Penempatan" in sel_map_metric_disp: color_s = "Greens"
            min_v, max_v = df_calc[sel_map_metric_col].min(), df_calc[sel_map_metric_col].max()
            range_c = (min_v, max_v) if min_v != max_v else (min_v - (0.1 * abs(min_v)) if min_v !=0 else 0, max_v + (0.1*abs(max_v)) if max_v !=0 else 1)
            
            fig_map = px.choropleth_map(df_calc, geojson=geojson_data, locations='Provinsi_Clean',
                                          featureidkey=GEOJSON_FEATURE_ID_KEY, color=sel_map_metric_col,
                                          color_continuous_scale=color_s, range_color=range_c,
                                          zoom=3.8, center={"lat": -2.5, "lon": 118},
                                          opacity=0.7, hover_name="Provinsi", 
                                          hover_data={sel_map_metric_col: hover_f, "Provinsi_Clean": False},
                                          labels={sel_map_metric_col: sel_map_metric_disp})
            fig_map.update_layout(title_text=f"<b>Peta Distribusi: {sel_map_metric_disp} per Provinsi</b>", title_x=0.5, 
                                  height=650, margin={"r":0,"t":40,"l":0,"b":0}, 
                                  coloraxis_colorbar={"title": sel_map_metric_disp, "thickness": 15},
                                  map_style="carto-positron"
                                 )
            st.plotly_chart(fig_map, use_container_width=True)
            with st.expander("Lihat Data Tabel untuk Peta Saat Ini (Diurutkan)", expanded=False):
                st.dataframe(df_calc[["Provinsi", sel_map_metric_col]].sort_values(sel_map_metric_col, ascending=False).style.format({sel_map_metric_col: hover_f}), height=300, use_container_width=True, hide_index=True)

# --- 9. Footer --- (Sama seperti sebelumnya)
st.markdown("---")
footer_api_url = html.escape(str(latest_doc.get('api_url_requested','N/A'))) if latest_doc else 'N/A'
footer_bps_id_tabel = html.escape(str(latest_doc.get('bps_id_tabel','N/A'))) if latest_doc else 'N/A'
st.markdown(f"""<div style="text-align: center; font-size: 0.85em; color: #555;">
    <p>Sumber Data: Badan Pusat Statistik (BPS) | Data dari DB per: {doc_timestamp_str}</p>
    <p style="font-size: 0.9em; color: #777;">ID Tabel Data (dari DB): {footer_bps_id_tabel} | <a href="{footer_api_url}" target="_blank" style="color: #007bff;">Contoh API Scraper</a></p>
    <p><i>Dashboard ini bersifat demonstrasi. Validitas dan interpretasi data adalah tanggung jawab pengguna.</i></p>
</div>""", unsafe_allow_html=True)