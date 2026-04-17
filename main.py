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
        
        # 2. FETCH METADATA WILAYAH
        url_meta = f"{config.BASE_API_URL}/region/api/v1/region-metadata?id={group_id}"
        res_meta = scraper.session.get(url_meta)
        levels = res_meta.json().get('data', {}).get('level', [])
        
        if not levels:
            print("❌ Struktur level wilayah kosong.")
            return

        # Target utama tetap Level 4 (Desa)
        target_level_idx = 3 if len(levels) >= 4 else len(levels) - 1
        lvl_target_info = levels[target_level_idx]

        # 3. TRAVERSAL (Mencari UUID Wilayah)
        print(f"🔍 Menelusuri jalur wilayah Kab: {config.TARGET_KAB_CODE}...", end=" ", flush=True)
        prov_code = str(config.TARGET_KAB_CODE)[:2] 
        
        res_kab = scraper.session.get(
            f"{config.BASE_API_URL}/region/api/v1/region/level2", 
            params={"groupId": group_id, "level1FullCode": prov_code}
        )
        kab_list = res_kab.json().get('data', [])
        target_kab = next((k for k in kab_list if str(k.get('fullCode')) == str(config.TARGET_KAB_CODE)), None)
        
        if not target_kab:
            print(f"\n❌ Kode {config.TARGET_KAB_CODE} tidak ditemukan.")
            return

        initial_metadata = {
            "region1Id": discovery["metadata"].get('region1Id'),
            "region2Id": target_kab['id']
        }
        initial_names = {f"lvl2_{levels[1]['name']}": target_kab['name']}

        current_list = [{
            "id": target_kab['id'], 
            "name": target_kab['name'], 
            "hierarchy": initial_metadata, 
            "meta_names": initial_names
        }]

        # Loop penelusuran sampai Level 4 (Desa)
        for i in range(2, target_level_idx + 1):
            lvl_info = levels[i]
            next_list = []
            parent_id_key = f"level{levels[i-1]['id']}Id"
            
            for parent in current_list:
                params = {"groupId": group_id, parent_id_key: parent['id']}
                if lvl_info['id'] == 3: params["level1FullCode"] = prov_code

                try:
                    res_raw = scraper.session.get(f"{config.BASE_API_URL}/region/api/v1/region/level{lvl_info['id']}", params=params)
                    res_data = res_raw.json().get('data', [])
                    
                    if isinstance(res_data, list):
                        for item in res_data:
                            new_hier = parent['hierarchy'].copy()
                            new_hier[f"region{lvl_info['id']}Id"] = item['id']
                            
                            new_names = parent['meta_names'].copy()
                            new_names[f"lvl{lvl_info['id']}_{lvl_info['name']}"] = item['name']
                            
                            next_list.append({
                                "id": item['id'], 
                                "name": item['name'], 
                                "hierarchy": new_hier,
                                "meta_names": new_names
                            })
                except: continue
            current_list = next_list
        
        # --- LOGIKA BARU: INTIP SLS (LEVEL 5) ---
        print(f"DONE. Mengunci {len(current_list)} Desa. Memetakan SLS...", end=" ", flush=True)
        for unit in current_list:
            # Cek apakah ada Level 5 di bawah desa ini
            try:
                res_sls = scraper.session.get(f"{config.BASE_API_URL}/region/api/v1/region/level5", 
                                             params={"groupId": group_id, "level4Id": unit['id']})
                sls_data = res_sls.json().get('data', [])
                
                unit['sls_list'] = [{
                    "id": s['id'], 
                    "name": s['name'],
                    "hierarchy": {**unit['hierarchy'], "region5Id": s['id']}
                } for s in sls_data] if isinstance(sls_data, list) else []
            except:
                unit['sls_list'] = []

        all_units = current_list
        print("OK.")

        # 4. TAHAP SCRAPING (Multi-Thread)
        final_results = []
        with tqdm(total=len(all_units), desc="📊 Progress", unit="unit", colour="green") as pbar:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(
                        scraper.fetch_all_data_per_unit, 
                        settings['period_id'], u['id'], u['name'], u['hierarchy'], u['sls_list'] # <-- Kirim SLS List
                    ): u for u in all_units
                }
                
                for future in as_completed(futures):
                    u_info = futures[future]
                    try:
                        res = future.result() 
                        if res:
                            for row in res:
                                for m_key, m_val in u_info['meta_names'].items():
                                    row[m_key] = m_val
                                # Label Desa
                                label_desa = f"lvl{lvl_target_info['id']}_{lvl_target_info['name']}"
                                row[label_desa] = u_info['name']
                            final_results.extend(res)
                    except Exception as e:
                        print(f"\n⚠️ Gagal di {u_info['name']}: {e}")
                    pbar.update(1)

        # 5. EXPORT & MAPPING
        if final_results:
            df = pd.DataFrame(final_results)
            df = df.loc[:, ~df.columns.duplicated()]

            # Deteksi kecamatan & desa asal
            col_kec = next((c for c in df.columns if c.lower().startswith('lvl3')), None)
            col_desa = next((c for c in df.columns if c.lower().startswith('lvl4')), None)
            if col_kec: df['kecamatan_asal'] = df[col_kec]
            if col_desa: df['desa_asal'] = df[col_desa]

            mapping = settings.get("columns")
            if mapping:
                target_cols = [c for c in mapping.keys() if c in df.columns]
                df = df[target_cols].rename(columns=mapping)

            if not os.path.exists("data"): os.makedirs("data")
            path_out = os.path.join("data", f"{config.tgl_str}_{survey_key}.csv")
            df.to_csv(path_out, index=False, sep=';', quoting=csv.QUOTE_ALL, encoding='utf-8')
            print(f"🏁 SELESAI! Total: {len(df)} baris.")

    except Exception as e:
        print(f"\n❌ Error: {e}")

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