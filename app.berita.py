import streamlit as st
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from urllib.parse import urlparse
from bson.objectid import ObjectId
from wordcloud import WordCloud

# --- Konfigurasi Halaman ---
st.set_page_config(page_title="Analisis Artikel", layout="wide")

# --- Header ---
st.title("📚 Analisis Artikel: Bisnis Karir dan Ekonomi")

# --- Koneksi ke MongoDB ---
client = MongoClient("mongodb://localhost:27017/")
db = client["FLUENT"]
collection = db["berita"]

# --- Ambil dan Proses Data ---
data = list(collection.find())
df = pd.DataFrame(data)

if df.empty:
    st.warning("⚠ Tidak ada data artikel yang tersedia di database.")
    st.stop()

# --- Rename kolom agar konsisten ---
df = df.rename(columns={
    'judul': 'title',
    'link': 'url',
    'pubDate': 'date'
})

# --- Parsing tanggal & domain ---
df['parsed_date'] = pd.to_datetime(df['date'], errors='coerce')
df['domain'] = df['url'].apply(lambda x: urlparse(x).netloc if pd.notnull(x) else "Unknown")

# --- Statistik Umum ---
st.markdown("### 🧾 Statistik Umum")
col1, col2 = st.columns(2)
col1.metric("📝 Total Artikel", len(df))
col2.metric("🌐 Jumlah Domain", df['domain'].nunique())

# --- Daftar Artikel ---
with st.expander("📋 Lihat Daftar Artikel"):
    show_columns = [col for col in ['title', 'date', 'url'] if col in df.columns]
    st.dataframe(df[show_columns], use_container_width=True)

# --- Grafik Artikel per Tanggal ---
st.markdown("### 📈 Artikel per Tanggal")
if df['parsed_date'].notnull().any():
    chart_data = df.groupby(df['parsed_date'].dt.date).size().reset_index(name='jumlah_artikel')
    fig, ax = plt.subplots()
    ax.plot(chart_data['parsed_date'], chart_data['jumlah_artikel'], marker='o', linestyle='-')
    ax.set_xlabel('Tanggal')
    ax.set_ylabel('Jumlah Artikel')
    ax.set_title('Jumlah Artikel per Tanggal')
    ax.grid(True)
    st.pyplot(fig)
else:
    st.info("Tidak ada data tanggal valid untuk divisualisasikan.")

# --- Grafik Jumlah Artikel per Domain ---
st.markdown("### 🌐 Jumlah Artikel per Domain")
st.bar_chart(df['domain'].value_counts())

# --- Word Cloud Judul ---
st.markdown("### ☁ Word Cloud Judul Artikel")
if df['title'].notnull().any():
    title_text = " ".join(df['title'].dropna())
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(title_text)
    fig_wc, ax_wc = plt.subplots()
    ax_wc.imshow(wordcloud, interpolation='bilinear')
    ax_wc.axis('off')
    st.pyplot(fig_wc)
else:
    st.info("Tidak ada judul artikel untuk dibuat Word Cloud.")

# --- Detail Artikel Berdasarkan ID ---
st.markdown("### 🔍 Detail Artikel")
with st.expander("Cari Artikel berdasarkan ID MongoDB"):
    article_id_input = st.text_input("Masukkan ID Artikel (24 digit heksadesimal):")
    if article_id_input:
        try:
            obj_id = ObjectId(article_id_input)
            article = collection.find_one({"_id": obj_id})
            if article:
                st.success("✅ Artikel ditemukan:")
                st.markdown(f"📰 Judul:** {article.get('judul', 'Tidak tersedia')}")
                st.markdown(f"📅 Tanggal Publikasi:** {article.get('pubDate', 'Tidak tersedia')}")
                st.markdown(f"📄 Deskripsi:** {article.get('description', 'Tidak tersedia')}")
                st.markdown(f"🔗 URL:** [{article.get('link', '')}]({article.get('link', '')})")
            else:
                st.warning("Artikel tidak ditemukan.")
        except Exception:
            st.error("ID tidak valid. Pastikan kamu memasukkan ID MongoDB yang benar.")