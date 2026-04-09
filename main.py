import json
import os
import sys
import pandas as pd
import time
import csv
import random
import requests
from google_drive import upload_to_drive
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import modul internal
import config
from scraper import FasihScraper
from login import auto_discovery_login

JSON_FILE = "surveys.json"

def load_surveys():
    if not os.path.exists(JSON_FILE):
        return {}
    with open(JSON_FILE, "r") as f:
        return json.load(f)

def save_surveys(data):
    with open(JSON_FILE, "w") as f:
        json.dump(data, f, indent=4)

def tambah_survey_manual():
    print("\n--- ➕ TAMBAH SURVEI BARU ---")
    nama = input("Masukkan Nama Survei (contoh: REGSOSEK): ").upper()
    p_id = input("Masukkan Period ID: ")
    u_id = input("Masukkan UUID: ")
    
    surveys = load_surveys()
    surveys[nama] = {
        "period_id": p_id,
        "uuid": u_id
    }
    save_surveys(surveys)
    print(f"✅ Survei {nama} berhasil ditambahkan!")

def process_survey(survey_key, settings, auto_upload=False):
    """Logika utama scraping dengan opsi upload otomatis"""
    start_time = time.time()
    
    # 1. Inisialisasi Scraper Bersih
    scraper = FasihScraper()
    scraper.session.cookies.clear() 
    print(f"\n--- 🧹 Session dibersihkan. Memproses: {survey_key} ---")

    # 2. Discovery & Auth
    try:
        discovery = auto_discovery_login(config.BASE_API_URL, settings['uuid'])
        if not discovery or not discovery.get("metadata"):
            print(f"🛑 Gagal Auth. Pastikan VPN aktif dan UUID benar.")
            return

        scraper.session.headers.update(discovery["headers"])
        scraper.metadata = discovery["metadata"]
    except Exception as e:
        print(f"🛑 Error Discovery: {e}")
        return

    # 3. Penelusuran Wilayah
    print(f"📂 Menelusuri wilayah untuk Kab ID: {config.SELECTED_KAB_ID}...")
    try:
        kec_list = scraper.get_sub_regions("level3", config.SELECTED_KAB_ID)
        if not kec_list:
            print("⚠️ Daftar kecamatan kosong. Periksa SELECTED_KAB_ID di .env")
            return

        all_desas = []
        for kec in kec_list:
            desa_list = scraper.get_sub_regions("level4", kec['id'])
            for d in desa_list:
                all_desas.append({
                    "id": d['id'], "name": d['name'], 
                    "kec_id": kec['id'], "kec_name": kec['name']
                })
            time.sleep(random.uniform(0.1, 0.3))
    except Exception as e:
        print(f"❌ Error saat menelusuri wilayah: {e}")
        return

    # 4. Scraping
    print(f"🚀 Scraping {len(all_desas)} desa dengan 3 threads...")
    final_results = []
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(
                scraper.fetch_all_data_per_desa, 
                settings['period_id'], d['id'], d['name'], d['kec_id']
            ): d for d in all_desas
        }
        
        for future in as_completed(futures):
            d_info = futures[future]
            try:
                res = future.result() 
                if res:
                    for row in res:
                        row['kecamatan_asal'] = d_info['kec_name']
                        row['desa_asal'] = d_info['name']
                    final_results.extend(res)
                    print(f"✔️ Selesai: {d_info['name']}")
                else:
                    print(f"⚪ Kosong: {d_info['name']}")
            except Exception as e:
                print(f"⚠️ Gagal di {d_info['name']}: {e}")

    # 5. Export & Optional Upload
    if final_results:
        if not os.path.exists("data"):
            os.makedirs("data")
            
        df = pd.DataFrame(final_results)
        mapping = settings.get("columns")
        total_baris = len(df)
        print(f"📊 Total data berhasil ditarik: {total_baris} baris.")
        
        if mapping:
            existing = [c for c in mapping.keys() if c in df.columns]
            df = df[existing].rename(columns=mapping)
            print(f"✅ Kolom difilter menjadi: {len(df.columns)} kolom.")
        
        nama_output = f"{config.tgl_str}_{survey_key}.csv"
        path_output = os.path.join("data", nama_output)
        
        # Simpan CSV Lokal (Delimiter ; agar sesuai Apps Script Anda)
        df.to_csv(path_output, index=False, sep=';', quoting=csv.QUOTE_ALL, encoding='utf-8')
        
        # --- LOGIKA UPLOAD GDRIVE ---
        if auto_upload:
            print(f"📤 Mengirim ke Drive...")
            upload_sukses = upload_to_drive(path_output)
            if upload_sukses:
                print("✨ Berhasil upload ke Google Drive.")
            else:
                print("⚠️ Gagal mengirim ke Drive.")
        else:
            print(f"📁 File disimpan di lokal: {path_output}")
        # ------------------------------
        
        durasi = time.time() - start_time
        print(f"🏁 SELESAI dalam {durasi:.2f}s")
    else:
        print(f"❌ Tidak ada data yang ditarik untuk {survey_key}.")

def main():
    """Menu Interaktif CLI"""
    while True:
        surveys = load_surveys()
        available_surveys = list(surveys.keys())
        
        print("\n" + "="*40)
        print("      FASIH SCRAPER - CLI MENU")
        print("="*40)
        for i, survey_name in enumerate(available_surveys, 1):
            print(f" [{i}] {survey_name}")
        print(f" [{len(available_surveys) + 1}] ➕ Tambah Survei")
        print(" [0] Keluar")
        print("="*40)

        try:
            pilihan = int(input(f"Pilih (0-{len(available_surveys) + 1}): "))
        except (ValueError, EOFError, KeyboardInterrupt):
            print("\n👋 Keluar...")
            break

        if pilihan == 0:
            break
        elif pilihan == len(available_surveys) + 1:
            tambah_survey_manual()
        elif 1 <= pilihan <= len(available_surveys):
            key = available_surveys[pilihan - 1]
            
            # Konfirmasi Upload via CLI
            tanya_upload = input(f"❓ Upload hasil {key} ke Google Drive? (y/n): ").lower()
            mau_upload = tanya_upload == 'y'
            
            process_survey(key, surveys[key], auto_upload=mau_upload)
        else:
            print("⚠️ Pilihan tidak valid.")

def main_automatic(survey_key, auto_upload=False):
    """Menjalankan scraping otomatis via Argumen"""
    surveys = load_surveys()
    if survey_key in surveys:
        process_survey(survey_key, surveys[survey_key], auto_upload)
    else:
        print(f"❌ Error: Nama survei '{survey_key}' tidak ditemukan.")

if __name__ == "__main__":
    # 1. Bersihkan semua argumen: ubah ke UPPERCASE dan hapus karakter "--"
    # Agar --PBI, --pbi, atau PBI semuanya dibaca sebagai PBI
    clean_args = [a.upper().replace("--", "") for a in sys.argv]
    
    # 2. Cari apakah ada flag UPLOAD di dalam argumen
    should_upload = "UPLOAD" in clean_args

    # 3. Cari argumen yang merupakan nama survei (yang bukan nama file dan bukan UPLOAD)
    # sys.argv[0] biasanya adalah 'main.py'
    potential_surveys = [a for a in clean_args if a not in ["UPLOAD", os.path.basename(__file__).upper()]]

    if potential_surveys:
        target_survey = potential_surveys[0]
        # Jalankan otomatis jika ada argumen survei
        main_automatic(target_survey, should_upload)
    else:
        # Jalankan menu interaktif jika tidak ada argumen
        main()

# if __name__ == "__main__":
#     # Upload gdrive saja:
#     upload_to_drive("data/20260409_1246_PBI.csv")