import json
import os
import sys
import pandas as pd
import time
import csv
from tqdm import tqdm
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
    """
    Scraping Level 4 dengan Progress Bar, Force Fetch Metadata,
    dan Full Traversal yang diperbaiki untuk parameter API.
    """
    start_time = time.time()
    scraper = FasihScraper()
    scraper.session.cookies.clear() 

    try:
        # 1. AUTH & DISCOVERY
        discovery = auto_discovery_login(config.BASE_API_URL, settings['uuid'])
        if not discovery or not discovery.get("metadata"):
            print(f"🛑 Gagal Auth untuk {survey_key}.")
            return

        scraper.session.headers.update(discovery["headers"])
        group_id = discovery["metadata"].get('groupId')
        
        # 2. FORCE FETCH METADATA (Menjamin array 'level' tersedia)
        url_meta = f"{config.BASE_API_URL}/region/api/v1/region-metadata?id={group_id}"
        res_meta = scraper.session.get(url_meta)
        if res_meta.status_code != 200:
            print(f"❌ Gagal mendapatkan metadata (Status: {res_meta.status_code})")
            return
            
        meta_data = res_meta.json().get('data', {})
        levels = meta_data.get('level', [])
        if not levels:
            print("❌ Struktur level wilayah kosong.")
            return

        # 3. PENENTUAN TARGET (Index 3 = Level 4)
        target_level_idx = 3 if len(levels) >= 4 else len(levels) - 1
        lvl_target_info = levels[target_level_idx]

        # --- TAHAP PENELUSURAN WILAYAH (Fixed Logic) ---
        print(f"🔍 Menelusuri jalur wilayah {config.TARGET_KAB_CODE}...", end=" ", flush=True)
        
        prov_code = str(config.TARGET_KAB_CODE)[:2] 
        # Cari UUID Kabupaten (Level 2)
        res_kab = scraper.session.get(
            f"{config.BASE_API_URL}/region/api/v1/region/level2", 
            params={"groupId": group_id, "level1FullCode": prov_code}
        )
        kab_list = res_kab.json().get('data', [])
        target_kab = next((k for k in kab_list if str(k.get('fullCode')) == str(config.TARGET_KAB_CODE)), None)
        
        if not target_kab:
            print(f"\n❌ Kode {config.TARGET_KAB_CODE} tidak ditemukan di Master Wilayah.")
            return

        current_list = [{"id": target_kab['id'], "name": target_kab['name'], "metadata": {}}]

        # Traversal Berjenjang
        for i in range(2, target_level_idx + 1):
            lvl_info = levels[i]
            lvl_api_name = f"level{lvl_info['id']}"
            next_list = []
            
            # Tentukan nama parameter ID bapak (level2Id, level3Id, dst)
            parent_id_key = f"level{levels[i-1]['id']}Id"
            
            for parent in current_list:
                params = {
                    "groupId": group_id,
                    parent_id_key: parent['id']
                }
                # API Level 3 sering mewajibkan level1FullCode (Provinsi)
                if lvl_info['id'] == 3:
                    params["level1FullCode"] = prov_code

                try:
                    res_raw = scraper.session.get(f"{config.BASE_API_URL}/region/api/v1/region/{lvl_api_name}", params=params)
                    res_data = res_raw.json().get('data', [])
                    
                    if isinstance(res_data, list):
                        for item in res_data:
                            meta = parent['metadata'].copy()
                            label_prev = f"lvl{levels[i-1]['id']}_{levels[i-1]['name']}"
                            meta[label_prev] = parent['name']
                            
                            next_list.append({
                                "id": item['id'], 
                                "name": item['name'], 
                                "metadata": meta, 
                                "parent_id": parent['id']
                            })
                except:
                    continue
            
            current_list = next_list
            if not current_list:
                print(f"\n⚠️ Terputus di {lvl_info['name']}. Periksa koneksi/akses.")
                break

        all_units = current_list
        print(f"DONE. ({len(all_units)} {lvl_target_info['name']} terkunci)")

    except Exception as e:
        print(f"\n❌ Gagal inisialisasi: {e}")
        return

    # --- TAHAP SCRAPING (Progress Bar) ---
    if not all_units:
        print("⚠️ Tidak ada unit wilayah untuk diproses.")
        return

    final_results = []
    failed_units = []
    print(f"🚀 Memulai Scraping {survey_key}:")
    
    with tqdm(total=len(all_units), desc="📊 Progress", unit="unit", colour="green") as pbar:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(
                    scraper.fetch_all_data_per_desa, 
                    settings['period_id'], u['id'], u['name'], u['parent_id']
                ): u for u in all_units
            }
            
            for future in as_completed(futures):
                u_info = futures[future]
                try:
                    res = future.result() 
                    if res:
                        for row in res:
                            for m_key, m_val in u_info['metadata'].items():
                                row[m_key] = m_val
                            label_self = f"lvl{lvl_target_info['id']}_{lvl_target_info['name']}"
                            row[label_self] = u_info['name']
                            row['target_unit_id'] = u_info['id']
                        final_results.extend(res)
                except Exception as e:
                    failed_units.append(f"{u_info['name']} ({str(e)})")
                pbar.update(1)

    # --- TAHAP AKHIR & EXPORT (Disesuaikan dengan surveys.json) ---
    if final_results:
        if not os.path.exists("data"): os.makedirs("data")
        df = pd.DataFrame(final_results)
        
        # 1. Hapus duplikat kolom teknis
        df = df.loc[:, ~df.columns.duplicated()]

        # 2. PENYESUAIAN NAMA KOLOM WILAYAH (Mapping Manual agar cocok dengan JSON)
        # Kita deteksi kolom lvl3 dan lvl4 secara dinamis, lalu beri nama sesuai kemauan JSON Anda
        col_kec = next((c for c in df.columns if c.startswith('lvl3')), None)
        col_desa = next((c for c in df.columns if c.startswith('lvl4')), None)

        if col_kec: df['kecamatan_asal'] = df[col_kec]
        if col_desa: df['desa_asal'] = df[col_desa]

        # 3. LOGIKA MAPPING & FILTERING KETAT (Strict Mode)
        mapping = settings.get("columns")
        
        if mapping:
            # Cari kolom mana saja yang benar-benar ada di DataFrame (termasuk kecamatan_asal yang baru dibuat)
            existing_source_cols = [c for c in mapping.keys() if c in df.columns]
            
            # Filter: Hapus semua kolom lvl_..., target_unit_id, dll. 
            # Hanya ambil yang terdaftar di JSON saja.
            df = df[existing_source_cols]
            
            # Rename ke nama cantik (Kecamatan, Desa, Nama_Kepala_Keluarga, dll)
            df = df.rename(columns=mapping)
            
            print(f"✅ Kolom berhasil dibersihkan & di-rename sesuai format PBI.")

        # 4. SIMPAN KE CSV
        nama_output = f"{config.tgl_str}_{survey_key}.csv"
        path_output = os.path.join("data", nama_output)
        
        # Semicolon (;) penting agar langsung rapi di Excel tanpa 'Text to Columns'
        df.to_csv(path_output, index=False, sep=';', quoting=csv.QUOTE_ALL, encoding='utf-8')
        
        print(f"\n💾 File lokal berhasil dibuat: {path_output}")

        # 5. DRIVE UPLOAD
        if auto_upload:
            print(f"📤 Mengirim ke Google Drive...", end=" ", flush=True)
            try:
                from google_drive import upload_to_drive
                if upload_to_drive(path_output):
                    print("✅ TERKIRIM!")
                else:
                    print("❌ GAGAL")
            except Exception as e:
                print(f"❌ ERROR: {e}")
            
        print(f"🏁 SELESAI! Total data: {len(df)} baris.")
    else:
        print(f"\n❌ Tidak ada data yang berhasil ditarik untuk {survey_key}.")

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