import requests
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime

# Load .env
load_dotenv()
BPS_API_KEY = os.getenv('BPS_API_KEY')
MONGO_URI = os.getenv('MONGO_URI')

if not BPS_API_KEY or not MONGO_URI:
    print("❌ BPS_API_KEY dan MONGO_URI harus diatur di .env")
    exit()

client = MongoClient(MONGO_URI)
db = client['bps_db']
collection = db['pekerja_terdaftar']

url = f"https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/id/25/tahun/2024/id_tabel/TE9UUDFUV3Bpa3ovMHJJVGtuUHZVdz09/wilayah/0000000/key/{BPS_API_KEY}"

response = requests.get(url)
if response.status_code != 200:
    print("❌ Gagal ambil data dari API BPS:", response.status_code)
    print(response.text)
    exit()

json_data = response.json()
try:
    provinsi_data = json_data['data'][1]['data']  # array of records per provinsi
except (KeyError, IndexError):
    print("❌ Struktur data tidak sesuai.")
    exit()

timestamp = datetime.now()
collection.insert_one({
    "timestamp": timestamp,
    "data": provinsi_data
})
print("✅ Data berhasil disimpan ke MongoDB.")
