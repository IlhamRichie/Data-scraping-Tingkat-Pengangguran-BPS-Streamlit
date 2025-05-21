import requests
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime
import json # Untuk pretty print jika ada error

# Load .env
load_dotenv()
BPS_API_KEY = os.getenv('BPS_API_KEY')
MONGO_URI = os.getenv('MONGO_URI')

if not BPS_API_KEY:
    print("❌ BPS_API_KEY harus diatur di .env")
    exit()

if not MONGO_URI:
    print("❌ MONGO_URI harus diatur di .env")
    exit()

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) # Tambahkan timeout
    # Ping server untuk memastikan koneksi berhasil sebelum melanjutkan
    client.admin.command('ping')
    print("✅ Berhasil terhubung ke MongoDB.")
    db = client['bps_db']
    collection = db['data']
except Exception as e:
    print(f"❌ Gagal terhubung ke MongoDB: {e}")
    exit()


# URL API BPS (pastikan ID Tabel dan parameter lain sudah benar)
# id_tabel 'TE9UUDFUV3Bpa3ovMHJJVGtuUHZVdz09' adalah contoh, ganti jika perlu
url = f"https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/id/25/tahun/2024/id_tabel/TE9UUDFUV3Bpa3ovMHJJVGtuUHZVdz09/wilayah/0000000/key/{BPS_API_KEY}"

print(f"ℹ️ Mengambil data dari: {url}")

try:
    response = requests.get(url, timeout=30) # Tambahkan timeout untuk request
    response.raise_for_status() # Akan raise HTTPError untuk status 4xx/5xx
    
    print(f"✅ Respons API BPS diterima (Status: {response.status_code})")
    json_data = response.json()

    # Validasi struktur data dasar
    if not isinstance(json_data, dict) or 'data' not in json_data or not isinstance(json_data['data'], list) or len(json_data['data']) < 2:
        print("❌ Struktur JSON utama dari API BPS tidak sesuai harapan.")
        print("Respons diterima:")
        try:
            print(json.dumps(json_data, indent=2, ensure_ascii=False))
        except:
            print(json_data)
        exit()

    # Akses data provinsi dengan lebih aman
    # Elemen pertama (index 0) adalah metadata pagination
    # Elemen kedua (index 1) berisi metadata tabel dan data aktual per provinsi
    data_container = json_data['data'][1] 
    if 'data' not in data_container or not isinstance(data_container['data'], list):
        print("❌ Field 'data' (yang berisi list provinsi) tidak ditemukan atau bukan list dalam json_data['data'][1].")
        print("Isi json_data['data'][1]:")
        try:
            print(json.dumps(data_container, indent=2, ensure_ascii=False))
        except:
            print(data_container)
        exit()
        
    provinsi_data_list = data_container['data'] # Ini adalah list of records per provinsi

    if not provinsi_data_list:
        print("⚠️ Tidak ada data provinsi yang ditemukan dalam respons API.")
        # Anda bisa memilih untuk exit() atau menyimpan dokumen kosong jika diinginkan
        # exit() 

    timestamp = datetime.now()
    
    # Struktur dokumen yang akan disimpan
    document_to_insert = {
        "timestamp": timestamp,
        "api_url": url, # Simpan URL untuk referensi
        "metadata_tabel": { # Simpan metadata tabel jika berguna
            "judul_tabel": data_container.get("judul_tabel"),
            "lingkup": data_container.get("lingkup"),
            "tahun_data": data_container.get("tahun_data"),
            "sumber": data_container.get("sumber"),
            "catatan": data_container.get("catatan")
        },
        "data_provinsi": provinsi_data_list # Ganti nama field agar lebih jelas
    }
    
    # Hapus data lama sebelum memasukkan yang baru (opsional, jika ingin hanya 1 dokumen terbaru)
    # result_delete = collection.delete_many({})
    # print(f"ℹ️ Menghapus {result_delete.deleted_count} dokumen lama.")

    collection.insert_one(document_to_insert)
    print(f"✅ Data berhasil disimpan ke MongoDB (collection: {collection.name}). {len(provinsi_data_list)} entri provinsi.")

except requests.exceptions.Timeout:
    print(f"❌ Timeout saat mencoba menghubungi API BPS: {url}")
except requests.exceptions.HTTPError as errh:
    print(f"❌ HTTP Error dari API BPS: {errh}")
    try:
        print("Detail Respons Error:")
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    except:
        print(response.text)
except requests.exceptions.RequestException as err:
    print(f"❌ Error Request lain ke API BPS: {err}")
except json.JSONDecodeError:
    print("❌ Gagal mem-parse JSON dari respons API BPS.")
    print("Respons mentah:")
    print(response.text)
except (KeyError, IndexError) as e:
    print(f"❌ Error: Struktur data JSON dari API BPS tidak sesuai dengan yang diharapkan. Kunci atau index tidak ditemukan: {e}")
    print("Respons diterima (jika ada):")
    try:
        print(json.dumps(json_data, indent=2, ensure_ascii=False)) # Cetak json_data jika sudah terdefinisi
    except NameError:
        print("json_data belum terdefinisi.")
    except Exception as print_exc:
        print(f"Tidak bisa mencetak detail JSON: {print_exc}")
except Exception as e:
    print(f"❌ Terjadi error yang tidak diketahui: {e}")
finally:
    if 'client' in locals() and client: # Pastikan client terdefinisi
        client.close()
        print("ℹ️ Koneksi MongoDB ditutup.")