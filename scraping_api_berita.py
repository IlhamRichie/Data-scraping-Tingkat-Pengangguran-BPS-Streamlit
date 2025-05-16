import requests
import schedule
import time
from pymongo import MongoClient
from datetime import datetime

# Koneksi ke MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['FLUENT']
collection = db['berita']

# Daftar semua endpoint kategori edukasi
endpoints = [
    "https://api-berita-indonesia.vercel.app/tribun/bisnis/",
    "https://api-berita-indonesia.vercel.app/tempo/bisnis/",
    "https://api-berita-indonesia.vercel.app/republika/daerah/",
    "https://api-berita-indonesia.vercel.app/okezone/economy/",
    "https://api-berita-indonesia.vercel.app/kumparan/terbaru/",
    "https://api-berita-indonesia.vercel.app/cnn/ekonomi/",
    "https://api-berita-indonesia.vercel.app/antara/ekonomi/",
    "https://api-berita-indonesia.vercel.app/tempo/event/",
]
# Fungsi untuk mengambil data dari endpoint
def scrap_dan_simpan():
    print(f"[{datetime.now()}] Mulai scraping...")

    # Hapus data lama
    deleted = collection.delete_many({})
    print(f"{deleted.deleted_count} data lama dihapus.")

    for endpoint in endpoints:
        try:
            res = requests.get(endpoint)
            res.raise_for_status()
            data = res.json()

            for article in data['data']['posts']:
                berita = {
                    "judul": article['title'],
                    "link": article['link'],
                    "thumbnail": article.get('thumbnail', ''),
                    "description": article.get('description', ''),
                    "pubDate": article.get('pubDate', ''),
                    "source": endpoint.split('/')[3],
                    "tanggal_scrap": datetime.now()
                }

                collection.insert_one(berita)
                print(f"Disimpan: {article['title']}")

        except Exception as e:
            print(f"Gagal ambil dari {endpoint}: {e}")

    print("Scraping selesai.\n")

# Jadwalkan scraping tiap 2 menit
schedule.every(15).minutes.do(scrap_dan_simpan)

print("Scheduler aktif. Tekan 'Stop' untuk menghentikan.")

# Jalankan terus menerus
while True:
    schedule.run_pending()
    time.sleep(1)