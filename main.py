import json
import os
import sys
import pandas as pd
import config
import time
import csv
import random
from scraper import FasihScraper
from login import auto_discovery_login 
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_survey(survey_key, settings, scraper_instance):
    nama_output = f"{config.tgl_str}_{survey_key}.csv"
    print(f"\n--- 📋 MEMPROSES SURVEI: {survey_key} ---")

    # --- TAHAP 1: DISCOVERY & AUTH ---
    try:
        discovery = auto_discovery_login(config.BASE_API_URL, settings['uuid'])
    except Exception as e:
        print(f"🛑 Gagal menghubungi server: {e}")
        print("💡 Pastikan VPN Anda sudah aktif dan login SSO masih berlaku.")
        return

    # Update session headers dan metadata per survei
    scraper_instance.session.headers.update(discovery["headers"])
    scraper_instance.metadata = discovery["metadata"]

    # --- TAHAP 2: PENELUSURAN WILAYAH ---
    print(f"📂 Menelusuri wilayah untuk Kabupaten: {config.SELECTED_KAB_ID}...")
    kec_list = scraper_instance.get_sub_regions("level3", config.SELECTED_KAB_ID)
    
    if not kec_list:
        print("⚠️ Daftar kecamatan kosong.")
        return

    all_desas = []
    for kec in kec_list:
        desa_list = scraper_instance.get_sub_regions("level4", kec['id'])
        for d in desa_list:
            all_desas.append({
                "id": d['id'], "name": d['name'], 
                "kec_id": kec['id'], "kec_name": kec['name']
            })
        time.sleep(random.uniform(0.1, 0.3))

    # --- TAHAP 3: MULTI-THREADED SCRAPING ---
    print(f"🚀 Scraping {len(all_desas)} desa...")
    final_results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                scraper_instance.fetch_all_data_per_desa, 
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
            except Exception as e:
                print(f"⚠️ Error di {d_info['name']}: {e}")

    # --- TAHAP 4: FILTER KOLOM & EXPORT ---
    if final_results:
        df = pd.DataFrame(final_results)
        
        # Ambil pemetaan kolom dari config
        mapping_kolom = settings.get("columns")
        
        if mapping_kolom:
            # Filter hanya kolom yang ada di config dan rename
            # Menggunakan list comprehension agar tidak error jika kolom tidak ada di API
            existing_cols = [c for c in mapping_kolom.keys() if c in df.columns]
            df = df[existing_cols].rename(columns=mapping_kolom)
            print(f"✅ Kolom difilter menjadi: {len(df.columns)} kolom.")
        
        # Simpan Output
        df.to_csv(nama_output, index=False, sep=';', quoting=csv.QUOTE_ALL, encoding='utf-8')
        
        durasi = time.time() - start_time
        print(f"🏁 {survey_key} SELESAI: {len(df)} data dalam {durasi:.2f}s -> {nama_output}")
    else:
        print(f"❌ Tidak ada data untuk {survey_key}.")


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

def main():
    while True: # Tambahkan loop agar setelah tambah data bisa langsung pilih
        surveys = load_surveys()
        available_surveys = list(surveys.keys())
        
        print("\n" + "="*40)
        print("      FASIH SCRAPER - MENU")
        print("="*40)
        for i, survey_name in enumerate(available_surveys, 1):
            print(f" [{i}] {survey_name}")
        
        print(f" [{len(available_surveys) + 1}] ➕ Tambah Survei Baru")
        print(" [0] Keluar")
        print("="*40)

        try:
            user_input = input(f"Pilih nomor (0-{len(available_surveys) + 1}): ")
            pilihan = int(user_input)
        except ValueError:
            print("❌ Masukkan angka!")
            continue

        if pilihan == 0:
            sys.exit()
        
        elif pilihan == len(available_surveys) + 1:
            tambah_survey_manual()
            
        elif 1 <= pilihan <= len(available_surveys):
            selected_key = available_surveys[pilihan - 1]
            settings = surveys[selected_key]
            
            scraper = FasihScraper()
            process_survey(selected_key, settings, scraper)
            break # Selesai proses, keluar loop atau biarkan jika ingin lanjut
        else:
            print("⚠️ Pilihan tidak valid.")

if __name__ == "__main__":
    main()