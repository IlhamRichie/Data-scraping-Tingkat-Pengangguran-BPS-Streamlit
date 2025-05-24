import requests
from pymongo import MongoClient, errors as pymongo_errors
from dotenv import load_dotenv
import os
from datetime import datetime, timezone
import json
import time
import logging
from typing import Dict, List, Optional, Any, Tuple # Pastikan baris ini ada

# --- Konfigurasi Logging ---
logging.basicConfig(
    level=logging.INFO, # Ubah ke logging.DEBUG jika ingin melihat log yang lebih detail saat troubleshooting
    format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    handlers=[
        logging.StreamHandler(), # Output ke konsol
        # logging.FileHandler("scraper.log") # Opsional: Output ke file log
    ]
)

# --- Load Environment Variables ---
load_dotenv()
BPS_API_KEY = os.getenv("BPS_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")

# Nama database dan koleksi bisa dikonfigurasi via .env atau default
DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "bps_db")
# Menggunakan nama koleksi yang lebih spesifik berdasarkan id_tabel jika diinginkan
# Ini adalah ID Tabel dari URL terakhir yang Anda berikan:
# https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/id/25/tahun/2024/id_tabel/TE9UUDFUV3Bpa3ovMHJJVGtuUHZVdz09/wilayah/0000000/key/rahasia
TARGET_BPS_ID_TABEL = "TE9UUDFUV3Bpa3ovMHJJVGtuUHZVdz09"
TARGET_BPS_TAHUN = "2024"
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", f"data_bps_{TARGET_BPS_ID_TABEL.replace('=', '').replace('/', '')}_{TARGET_BPS_TAHUN}")


# --- Konfigurasi API BPS (berdasarkan URL terakhir yang Anda berikan) ---
BPS_MODEL_ID = os.getenv("BPS_MODEL_ID", "simdasi")
BPS_DOMAIN_ID = os.getenv("BPS_DOMAIN_ID", "0000") # '0000' untuk nasional
BPS_DATA_SOURCE_ID = os.getenv("BPS_DATA_SOURCE_ID", "25") # Dari /id/25/ di URL
BPS_WILAYAH = os.getenv("BPS_WILAYAH", "0000000")

# --- Konstanta ---
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 10
REQUEST_TIMEOUT_SECONDS = 45
MONGO_TIMEOUT_MS = 10000

def validate_env_vars() -> bool:
    """Memvalidasi apakah environment variables yang dibutuhkan sudah ada."""
    required_vars = {"BPS_API_KEY": BPS_API_KEY, "MONGO_URI": MONGO_URI}
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        for var in missing_vars:
            logging.error(f"❌ Environment variable '{var}' harus diatur di file .env atau sistem.")
        return False
    return True

def connect_to_mongodb() -> tuple[Optional[MongoClient], Optional[Any]]:
    """Membangun koneksi ke MongoDB."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS, connectTimeoutMS=MONGO_TIMEOUT_MS)
        client.admin.command("ping") # Memastikan koneksi berhasil
        logging.info(f"✅ Berhasil terhubung ke MongoDB Atlas (DB: {DATABASE_NAME}, Collection: {COLLECTION_NAME}).")
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        return client, collection
    except pymongo_errors.ConnectionFailure as e:
        logging.error(f"❌ Gagal terhubung ke MongoDB (ConnectionFailure): {e}")
    except pymongo_errors.ConfigurationError as e:
        logging.error(f"❌ Gagal terhubung ke MongoDB (ConfigurationError): Pastikan MONGO_URI benar. {e}")
    except Exception as e:
        logging.error(f"❌ Gagal terhubung ke MongoDB (Unknown Error): {e}")
    return None, None

def fetch_bps_data(api_url: str) -> Optional[dict]:
    """Mengambil data dari API BPS dengan retry mechanism."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(api_url, timeout=REQUEST_TIMEOUT_SECONDS)
            logging.info(f"Mencoba mengambil data dari API BPS, percobaan {attempt + 1}/{MAX_RETRIES}. URL: {api_url}")
            response.raise_for_status() # Akan raise HTTPError untuk status 4xx/5xx
            logging.info(f"✅ Respons API BPS diterima (Status: {response.status_code})")
            return response.json()
        except requests.exceptions.Timeout:
            logging.warning(f"⏳ Timeout saat menghubungi API BPS (percobaan {attempt + 1}/{MAX_RETRIES})")
        except requests.exceptions.HTTPError as errh:
            logging.error(f"❌ HTTP Error {errh.response.status_code} dari API BPS (percobaan {attempt + 1}/{MAX_RETRIES}): {errh}")
            try:
                error_detail = errh.response.json()
                logging.error(f"Detail Respons Error API: {json.dumps(error_detail, indent=2, ensure_ascii=False)}")
                if isinstance(error_detail, dict) and "message" in error_detail:
                    logging.error(f"Pesan dari BPS API: {error_detail['message']}")
            except json.JSONDecodeError:
                logging.error(f"Detail Respons Error API (raw): {errh.response.text}")

            if errh.response.status_code < 500 and errh.response.status_code != 429: # Jangan retry untuk client error (kecuali 429)
                logging.info("Error dari sisi klien (4xx), tidak melakukan retry.")
                break # Keluar dari loop retry
        except requests.exceptions.RequestException as err:
            logging.warning(f"❌ Error Request lain ke API BPS (percobaan {attempt + 1}/{MAX_RETRIES}): {err}")
        except json.JSONDecodeError:
            logging.error(f"❌ Gagal mem-parse JSON dari respons API BPS (percobaan {attempt + 1}/{MAX_RETRIES}).")
            if 'response' in locals() and hasattr(response, 'text'): # Cek response ada dan punya .text
                logging.warning(f"Respons mentah yang diterima: {response.text[:500]}...") # Log sebagian respons mentah
            return None # Tidak perlu retry jika JSON tidak valid

        if attempt < MAX_RETRIES - 1:
            logging.info(f"Menunggu {RETRY_DELAY_SECONDS} detik sebelum mencoba lagi...")
            time.sleep(RETRY_DELAY_SECONDS)
        else:
            logging.error(f"❌ Gagal mengambil data dari API BPS setelah {MAX_RETRIES} percobaan.")
    return None

def process_and_store_data(collection: Any, json_data: dict, api_url: str, id_tabel: str, tahun_data_req: str) -> bool:
    """Memproses data JSON dari BPS dan menyimpannya ke MongoDB."""
    try:
        # Validasi Awal: json_data harus dictionary dan memiliki field 'data' berupa list
        if not isinstance(json_data, dict) or "data" not in json_data:
            logging.error("❌ Struktur JSON utama dari API BPS tidak sesuai: 'json_data' bukan dict atau tidak ada field 'data'.")
            logging.warning(f"Tipe json_data: {type(json_data)}")
            logging.warning(f"Isi json_data (awal 1000 karakter): {str(json_data)[:1000]}")
            return False

        data_field = json_data.get("data")
        if not isinstance(data_field, list) or len(data_field) < 2: # BPS API biasanya punya 2 elemen di list 'data' utama
            logging.error("❌ Field 'data' dalam JSON API BPS bukan list atau tidak memiliki cukup elemen (kurang dari 2).")
            logging.warning(f"Tipe json_data['data']: {type(data_field)}")
            if isinstance(data_field, list):
                logging.warning(f"Panjang json_data['data']: {len(data_field)}")
            logging.warning(f"Isi json_data['data'] (awal 1000 karakter): {str(data_field)[:1000]}")
            logging.warning(f"Full json_data (awal 1000 karakter): {str(json_data)[:1000]}")
            return False

        pagination_info = data_field[0] # Metadata paginasi
        data_container = data_field[1]  # Metadata tabel dan data utama

        # Validasi Lanjutan: data_container (json_data['data'][1]) harus dictionary
        if not isinstance(data_container, dict):
            logging.error(f"❌ Konten data API BPS (json_data['data'][1]) diharapkan dictionary, tapi ditemukan: {type(data_container)}.")
            logging.warning(f"Isi json_data['data'][0] (pagination?): {json.dumps(pagination_info, indent=2, ensure_ascii=False, default=str)}")
            logging.warning(f"Isi json_data['data'][1] (data_container?): {json.dumps(data_container, indent=2, ensure_ascii=False, default=str)}")
            logging.warning(f"Full json_data (awal 1000 karakter): {str(json_data)[:1000]}")
            return False

        # Validasi Lanjutan: 'data' di dalam data_container harus list (ini adalah list provinsi)
        provinsi_data_list_container = data_container.get("data")
        if not isinstance(provinsi_data_list_container, list):
            logging.error(f"❌ Field 'data' (yang berisi list provinsi) dalam json_data['data'][1] tidak ditemukan atau bukan list. Ditemukan: {type(provinsi_data_list_container)}")
            logging.warning(f"Isi json_data['data'][1] (data_container): {json.dumps(data_container, indent=2, ensure_ascii=False, default=str)}")
            logging.warning(f"Full json_data (awal 1000 karakter): {str(json_data)[:1000]}")
            return False
        
        provinsi_data_list = provinsi_data_list_container

        if not provinsi_data_list:
            logging.warning("⚠️ Tidak ada entri data provinsi (json_data['data'][1]['data'] kosong) dalam respons API, namun metadata mungkin tetap diproses.")

        timestamp_utc = datetime.now(timezone.utc)

        # Ambil metadata tabel dari data_container
        metadata_tabel_scraped = {
            key: data_container.get(key) for key in [
                "judul_tabel", "lingkup", "tahun_data", "sumber", "catatan", "kolom", # 'kolom' berisi definisi variabel
                "nama_variabel", "nama_variabel_turunan_baris", "nama_variabel_turunan_kolom"
            ] if data_container.get(key) is not None
        }
        # Tambah info paginasi jika ada dan berupa dict
        if isinstance(pagination_info, dict):
            metadata_tabel_scraped["total_records_in_query"] = pagination_info.get("count")
            metadata_tabel_scraped["current_page"] = pagination_info.get("page")
        
        # Pastikan tahun_data ada, jika tidak ambil dari tahun request
        actual_tahun_data = metadata_tabel_scraped.get("tahun_data", tahun_data_req)


        document_to_insert = {
            "timestamp_scraped_utc": timestamp_utc,
            "api_url_requested": api_url,
            "bps_id_tabel": id_tabel, # ID Tabel yang di-scrape
            "bps_tahun_data_request": tahun_data_req, # Tahun yang di-request
            "bps_tahun_data_actual": actual_tahun_data, # Tahun dari metadata tabel jika ada
            "bps_model_id_used": BPS_MODEL_ID,
            "bps_domain_id_used": BPS_DOMAIN_ID,
            "bps_data_source_id_used": BPS_DATA_SOURCE_ID,
            "metadata_tabel_scraped": metadata_tabel_scraped, # Termasuk definisi 'kolom'
            "data_provinsi": provinsi_data_list,
            "schema_version": "1.2" # Update versi skema jika ada perubahan signifikan
        }

        # Menggunakan Upsert: Update jika ada berdasarkan ID Tabel & Tahun request, Insert jika belum ada.
        query_filter = {
            "bps_id_tabel": id_tabel,
            "bps_tahun_data_request": tahun_data_req
        }
        update_result = collection.update_one(query_filter, {"$set": document_to_insert}, upsert=True)

        if update_result.upserted_id:
            logging.info(f"✅ Data baru berhasil di-insert (upsert) ke MongoDB dengan ID: {update_result.upserted_id}.")
        elif update_result.modified_count > 0:
            logging.info(f"✅ Data yang ada berhasil di-update di MongoDB (filter: {query_filter}).")
        else:
            logging.info(f"ℹ️ Tidak ada perubahan data di MongoDB (filter: {query_filter}). Data mungkin sama atau list provinsi kosong.")
        return True

    except (KeyError, IndexError, TypeError) as e:
        logging.error(f"❌ Error parsing (KeyError/IndexError/TypeError) struktur data JSON BPS atau data tidak valid: {e}")
        if 'json_data' in locals() and json_data is not None:
            try:
                logging.warning(f"Data JSON saat error (awal 1000 karakter): {str(json_data)[:1000]}")
            except Exception as dump_exc:
                logging.warning(f"Tidak bisa mencetak json_data mentah: {dump_exc}")
        else:
            logging.warning("json_data tidak tersedia atau None saat error parsing.")
        return False
    except pymongo_errors.PyMongoError as e:
        logging.error(f"❌ Error MongoDB saat menyimpan data: {e}")
        return False
    except Exception as e:
        logging.error(f"❌ Terjadi error yang tidak diketahui saat memproses/menyimpan data: {e}", exc_info=True)
        return False

def main():
    """Fungsi utama untuk menjalankan scraper."""
    logging.info(f"🚀 Memulai scraper data BPS untuk ID Tabel: {TARGET_BPS_ID_TABEL}, Tahun: {TARGET_BPS_TAHUN}...")

    if not validate_env_vars():
        return

    mongo_client, collection = connect_to_mongodb()
    
    # Perbaikan dari error NotImplementedError
    if mongo_client is None or collection is None:
        logging.error("❌ Gagal mendapatkan koneksi atau koleksi MongoDB. Scraper berhenti.")
        return

    # Bentuk URL API BPS
    # Format: /datasource/{model_id}/domain/{domain_id}/id/{id_sumberdata}/tahun/{tahun}/id_tabel/{id_tabel}/wilayah/{id_wilayah}
    api_url = f"https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/{BPS_MODEL_ID}/domain/{BPS_DOMAIN_ID}/id/{BPS_DATA_SOURCE_ID}/tahun/{TARGET_BPS_TAHUN}/id_tabel/{TARGET_BPS_ID_TABEL}/wilayah/{BPS_WILAYAH}/key/{BPS_API_KEY}"
    logging.info(f"ℹ️ URL API BPS yang akan diakses: {api_url}")

    json_data = fetch_bps_data(api_url)

    if json_data:
        if process_and_store_data(collection, json_data, api_url, TARGET_BPS_ID_TABEL, TARGET_BPS_TAHUN):
            logging.info("🎉 Scraper berhasil menyelesaikan tugas.")
        else:
            logging.error("❌ Scraper gagal memproses atau menyimpan data setelah data API diterima.")
    else:
        logging.error("❌ Scraper gagal mengambil data dari API BPS setelah semua percobaan.")

    if mongo_client:
        mongo_client.close()
        logging.info("ℹ️ Koneksi MongoDB ditutup.")

if __name__ == "__main__":
    main()