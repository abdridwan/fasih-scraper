import os
import time
import csv
import pandas as pd
from PyQt6.QtCore import QThread, pyqtSignal
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import internal
import config
from scraper import FasihScraper
from login import auto_discovery_login
from google_drive import upload_to_drive

class ScraperWorker(QThread):
    finished = pyqtSignal(str)
    log_signal = pyqtSignal(str)

    def __init__(self, survey_key, settings, auto_upload=False):
        super().__init__()
        self.survey_key = survey_key
        self.settings = settings
        self.auto_upload = auto_upload
        self._is_running = True  # Flag kontrol untuk tombol STOP

    def stop(self):
        """Dipanggil dari main_window saat tombol STOP ditekan"""
        self._is_running = False
        self.log_signal.emit("\n🛑 [SISTEM] Mengirim sinyal penghentian proses...")

    def run(self):
        try:
            start_time = time.time()
            
            # 1. INISIALISASI SCRAPER (Adopsi logika main.py agar tidak lemot)
            scraper = FasihScraper()
            scraper.session.cookies.clear() # Bersihkan session sisa sebelumnya
            self.log_signal.emit("🧹 [1/5] Session dibersihkan. Memulai autentikasi...")

            # 2. LOGIN & DISCOVERY
            # Menggunakan login.py yang sudah dioptimasi (bypass dashboard)
            discovery = auto_discovery_login(config.BASE_API_URL, self.settings['uuid'])
            
            if not discovery or not discovery.get("metadata"):
                self.finished.emit("❌ Autentikasi Gagal. Pastikan VPN aktif & kredensial benar.")
                return

            # Masukkan hasil discovery ke session scraper
            scraper.session.headers.update(discovery["headers"])
            scraper.metadata = discovery["metadata"]
            self.log_signal.emit("✅ [2/5] Login berhasil. Metadata wilayah didapatkan.")

            if not self._is_running: return

            # 3. PENELUSURAN WILAYAH (Level Kecamatan & Desa)
            self.log_signal.emit(f"📂 [3/5] Menelusuri wilayah Kab ID: {config.SELECTED_KAB_ID}...")
            kec_list = scraper.get_sub_regions("level3", config.SELECTED_KAB_ID)
            
            if not kec_list:
                self.finished.emit("❌ Gagal mendapatkan daftar kecamatan. Cek koneksi server.")
                return

            all_desas = []
            for kec in kec_list:
                if not self._is_running: break
                
                desa_list = scraper.get_sub_regions("level4", kec['id'])
                for d in desa_list:
                    all_desas.append({
                        "id": d['id'], 
                        "name": d['name'], 
                        "kec_id": kec['id'], 
                        "kec_name": kec['name']
                    })
                # Delay kecil agar tidak dianggap spam oleh server
                time.sleep(0.1)

            if not self._is_running: 
                self.finished.emit("⚠️ Proses dihentikan saat penelusuran wilayah.")
                return

            # 4. PROSES SCRAPING (Multi-threading)
            total_desa = len(all_desas)
            self.log_signal.emit(f"🚀 [4/5] Memulai scraping {total_desa} desa (3 Threads)...")
            final_results = []
            
            with ThreadPoolExecutor(max_workers=3) as executor:
                # Mapping tugas ke setiap desa
                futures = {
                    executor.submit(
                        scraper.fetch_all_data_per_desa, 
                        self.settings['period_id'], d['id'], d['name'], d['kec_id']
                    ): d for d in all_desas
                }

                for future in as_completed(futures):
                    # Jika user klik STOP, hentikan pengambilan data desa berikutnya
                    if not self._is_running:
                        self.log_signal.emit("⚠️ [SISTEM] Menghentikan thread yang tersisa...")
                        break
                    
                    d_info = futures[future]
                    try:
                        res = future.result()
                        if res:
                            for row in res:
                                row['kecamatan_asal'] = d_info['kec_name']
                                row['desa_asal'] = d_info['name']
                            final_results.extend(res)
                            self.log_signal.emit(f"✔️ Selesai: {d_info['name']}")
                        else:
                            self.log_signal.emit(f"⚪ Kosong: {d_info['name']}")
                    except Exception as e:
                        self.log_signal.emit(f"⚠️ Gagal di {d_info['name']}: {str(e)}")

            # 5. EXPORT DATA & OPTIONAL UPLOAD
            if final_results:
                self.log_signal.emit("📊 [5/5] Menyusun data dan mengekspor ke CSV...")
                
                if not os.path.exists("data"): os.makedirs("data")
                
                df = pd.DataFrame(final_results)
                
                # Filter Kolom jika mapping didefinisikan di JSON
                mapping = self.settings.get("columns")
                if mapping:
                    existing = [c for c in mapping.keys() if c in df.columns]
                    df = df[existing].rename(columns=mapping)

                path_out = os.path.join("data", f"{config.tgl_str}_{self.survey_key}.csv")
                df.to_csv(path_out, index=False, sep=';', quoting=csv.QUOTE_ALL, encoding='utf-8')
                
                durasi = time.time() - start_time
                summary_msg = f"🏁 Selesai dalam {durasi:.2f}s! Total: {len(df)} baris."

                # Logika Upload GDrive (Hanya jika sukses & tidak di-stop paksa)
                if self.auto_upload and self._is_running:
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