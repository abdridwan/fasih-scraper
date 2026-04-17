import os
import time
import csv
import pandas as pd
from PyQt6.QtCore import QThread, pyqtSignal
from concurrent.futures import ThreadPoolExecutor, as_completed

import config
from scraper import FasihScraper
from login import auto_discovery_login
from google_drive import upload_to_drive

class ScraperWorker(QThread):
    finished = pyqtSignal(str)
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)

    def __init__(self, survey_key, settings, auto_upload=False):
        super().__init__()
        self.survey_key = survey_key
        self.settings = settings
        self.auto_upload = auto_upload
        self._is_running = True

    def stop(self):
        self._is_running = False
        self.log_signal.emit("\n🛑 [SISTEM] Mengirim sinyal penghentian proses...")

    def run(self):
        try:
            start_time = time.time()
            scraper = FasihScraper()
            scraper.session.cookies.clear()
            self.log_signal.emit("🧹 [1/5] Session dibersihkan. Memulai autentikasi...")
            self.progress_signal.emit(5)

            # 1. LOGIN & DISCOVERY
            discovery = auto_discovery_login(config.BASE_API_URL, self.settings['uuid'])
            if not discovery or not discovery.get("metadata"):
                self.finished.emit("❌ Autentikasi Gagal. Pastikan VPN aktif & kredensial benar.")
                return

            scraper.session.headers.update(discovery["headers"])
            scraper.metadata = discovery["metadata"]
            group_id = discovery["metadata"].get('groupId')
            
            # Ambil struktur level secara dinamis (Level 1, 2, 3, 4, dst)
            url_meta = f"{config.BASE_API_URL}/region/api/v1/region-metadata?id={group_id}"
            levels = scraper.session.get(url_meta).json().get('data', {}).get('level', [])
            
            if not levels:
                self.finished.emit("❌ Gagal mendapatkan hierarki survei.")
                return

            # Tentukan target level (biasanya 4, tapi bisa menyesuaikan otomatis jika kurang)
            target_level_idx = 3 if len(levels) >= 4 else len(levels) - 1
            target_lvl_num = levels[target_level_idx]['id']
            self.log_signal.emit(f"✅ [2/5] Login sukses. Menargetkan Level {target_lvl_num} ({levels[target_level_idx]['name']}).")
            self.progress_signal.emit(15)

            if not self._is_running: return

            # 2. TRAVERSAL WILAYAH (Bikin Hierarki Dinamis)
            self.log_signal.emit(f"📂 [3/5] Menelusuri wilayah Kab ID: {config.TARGET_KAB_CODE}...")
            
            # Setup Level 2 (Kabupaten) sbg titik awal
            prov_code = str(config.TARGET_KAB_CODE)[:2]
            res_kab = scraper.session.get(f"{config.BASE_API_URL}/region/api/v1/region/level2", 
                                          params={"groupId": group_id, "level1FullCode": prov_code}).json()
            
            # Cari Kabupaten berdasarkan TARGET_KAB_CODE
            target_kab = next((k for k in res_kab.get('data', []) if str(k['fullCode']) == str(config.TARGET_KAB_CODE)), None)

            # 🚨 PERBAIKAN: TAMBAHKAN PENGECEKAN INI 🚨
            if not target_kab:
                self.finished.emit(f"❌ Kode Kab '{config.TARGET_KAB_CODE}' tidak ditemukan di server. Pastikan kodenya benar.")
                return

            # Inisialisasi unit aktif dengan membawa hierarki
            lvl2_name_label = levels[1]['name'] if len(levels) > 1 else "Kabupaten"
            active_units = [{
                "id": target_kab['id'],
                "name": target_kab['name'],
                # INI KUNCI PAGINASI: Kumpulkan region1Id, region2Id dst...
                "hierarchy": {
                    "region1Id": scraper.metadata.get('region1Id'),
                    "region2Id": target_kab['id']
                },
                "meta_names": {f"lvl2_{lvl2_name_label}": target_kab['name']}
            }]

            # Loop dari Level 3 sampai Target Level
            for i in range(2, target_level_idx + 1):
                lvl_info = levels[i]
                current_lvl_num = lvl_info['id']
                parent_lvl_num = levels[i-1]['id']
                next_gen = []

                for parent in active_units:
                    if not self._is_running: break
                    params = {"groupId": group_id, f"level{parent_lvl_num}Id": parent['id']}
                    if current_lvl_num == 3: params["level1FullCode"] = prov_code
                    
                    try:
                        res_sub = scraper.session.get(f"{config.BASE_API_URL}/region/api/v1/region/level{current_lvl_num}", params=params).json()
                        for item in res_sub.get('data', []):
                            # Copy dan update hierarki untuk anak-anaknya
                            new_hier = parent['hierarchy'].copy()
                            new_hier[f"region{current_lvl_num}Id"] = item['id']
                            
                            new_meta = parent['meta_names'].copy()
                            new_meta[f"lvl{current_lvl_num}_{lvl_info['name']}"] = item['name']

                            next_gen.append({
                                "id": item['id'],
                                "name": item['name'],
                                "hierarchy": new_hier,
                                "meta_names": new_meta
                            })
                    except: continue
                
                active_units = next_gen

            total_target = len(active_units)
            self.log_signal.emit(f"✅ Terkunci {total_target} wilayah target.")
            self.progress_signal.emit(30)

            if not self._is_running: return

            # 3. PROSES SCRAPING MULTI-THREAD
            self.log_signal.emit(f"🚀 [4/5] Memulai scraping {total_target} unit (3 Threads)...")
            final_results = []
            completed_count = 0

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {
                    executor.submit(
                        scraper.fetch_all_data_per_unit, 
                        self.settings['period_id'], u['id'], u['name'], u['hierarchy']
                    ): u for u in active_units
                }

                for future in as_completed(futures):
                    if not self._is_running: break
                    u_info = futures[future]
                    try:
                        res = future.result()
                        if res:
                            for row in res:
                                # Injeksi nama-nama wilayah parent secara dinamis
                                for m_key, m_val in u_info['meta_names'].items():
                                    row[m_key] = m_val
                            
                            final_results.extend(res)
                            self.log_signal.emit(f"✔️ Selesai: {u_info['name']} ({len(res)} data)")
                        else:
                            self.log_signal.emit(f"⚪ Kosong: {u_info['name']}")
                    except Exception as e:
                        self.log_signal.emit(f"⚠️ Gagal di {u_info['name']}: {str(e)}")
                    
                    completed_count += 1
                    progress_pct = 30 + int((completed_count / total_target) * 60)
                    self.progress_signal.emit(progress_pct)

            # 4. EXPORT DATA
            if final_results and self._is_running:
                self.log_signal.emit("📊 [5/5] Menyusun data & mengekspor ke CSV...")
                df = pd.DataFrame(final_results)
                
                if 'id' in df.columns:
                    df = df.drop_duplicates(subset=['id'])

                # Ambil konfigurasi Mapping dari JSON PBI
                mapping = self.settings.get("columns")
                
                if mapping:
                    # --- LOGIKA PENYELAMAT KOLOM KECAMATAN & DESA ---
                    
                    # 1. Deteksi otomatis nama kolom wilayah dari metadata
                    # Biasanya API region-metadata memberikan nama lvl3_Kecamatan atau lvl4_Desa
                    for col in df.columns:
                        if "lvl3_" in col.lower() and "kecamatan_asal" not in df.columns:
                            df["kecamatan_asal"] = df[col]
                        if "lvl4_" in col.lower() and "desa_asal" not in df.columns:
                            df["desa_asal"] = df[col]

                    # 2. Paksa semua kolom di mapping tersedia
                    # Jika tetap tidak ada setelah deteksi, buat kolom kosong agar tidak error
                    for key in mapping.keys():
                        if key not in df.columns:
                            df[key] = "" 

                    # 3. Urutkan kolom sesuai urutan di JSON (ID -> Kode -> Status -> Kec -> Desa dst)
                    ordered_keys = list(mapping.keys())
                    df = df[ordered_keys]

                    # 4. Rename ke Nama Kolom Final
                    df = df.rename(columns=mapping)

                    # 5. Sesuai catatan: Jika kolom benar-benar kosong (tidak terdefinisi), hapus.
                    if "Kecamatan" in df.columns and (df["Kecamatan"] == "").all():
                        df = df.drop(columns=["Kecamatan"])
                    if "Desa" in df.columns and (df["Desa"] == "").all():
                        df = df.drop(columns=["Desa"])
                
                # Simpan ke CSV
                if not os.path.exists("data"): os.makedirs("data")
                path_out = os.path.join("data", f"{config.tgl_str}_{self.survey_key}.csv")
                
                # Gunakan semicolon (;) agar langsung rapi saat dibuka di Excel Indonesia
                df.to_csv(path_out, index=False, sep=';', quoting=csv.QUOTE_ALL, encoding='utf-8')
                
                self.progress_signal.emit(100)
                durasi = time.time() - start_time
                summary_msg = f"🏁 Selesai! Total: {len(df)} baris.\nKolom: {', '.join(df.columns)}"

                if self.auto_upload:
                    self.log_signal.emit("📤 Mengunggah hasil ke Google Drive...")
                    if upload_to_drive(path_out):
                        summary_msg += "\n✨ Berhasil upload ke Google Drive."
                    else:
                        summary_msg += "\n⚠️ Gagal upload (Cek koneksi/token)."

                self.finished.emit(summary_msg)
            else:
                self.finished.emit("⚪ Tidak ada data yang berhasil ditarik.")

        except Exception as e:
            self.finished.emit(f"❌ Kesalahan Fatal: {str(e)}")