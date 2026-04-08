import sys, json, os, time, csv, random
import pandas as pd
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QComboBox, QPushButton, QTextEdit, 
                             QLabel, QMessageBox, QLineEdit, QDialog, QFormLayout)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
from PyQt6.QtGui import QFont
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import modul internal
import config
from scraper import FasihScraper
from login import auto_discovery_login

JSON_FILE = "surveys.json"

class SurveyEditorDialog(QDialog):
    """Dialog untuk Tambah dan Edit Konfigurasi Survei"""
    def __init__(self, parent=None, survey_key=None, data=None):
        super().__init__(parent)
        self.setWindowTitle("Editor Survei" if survey_key else "Tambah Survei")
        self.setFixedWidth(500)
        self.old_key = survey_key
        
        layout = QFormLayout(self)
        self.input_nama = QLineEdit(survey_key if survey_key else "")
        self.input_period = QLineEdit(data.get('period_id', "") if data else "")
        self.input_uuid = QLineEdit(data.get('uuid', "") if data else "")
        
        # --- BAGIAN COLUMN MAPPING ---
        self.input_columns = QTextEdit()
        self.input_columns.setFixedHeight(150)
        
        # Menambahkan Placeholder Text (Pesan Bantuan saat Kosong)
        self.input_columns.setPlaceholderText(
            "Biarkan KOSONG (Hapus semua teks) jika ingin mengambil semua kolom data dari server.\n\n"
            "Atau gunakan format JSON:\n"
            "{\n  \"kolom_api\": \"Nama_Alias\"\n}"
        )
        
        # Set data jika ada, jika tidak biarkan kosong agar placeholder muncul
        if data and data.get('columns'):
            self.input_columns.setPlainText(json.dumps(data.get('columns'), indent=4))
        
        # Tambahkan Label bantuan di atas input_columns
        help_label = QLabel("Mapping Kolom (Kosongkan jika ingin scraping semua kolom):")
        help_label.setStyleSheet("color: #2980b9; font-size: 10px; font-style: italic;")

        layout.addRow("Nama Survei:", self.input_nama)
        layout.addRow("Period ID:", self.input_period)
        layout.addRow("UUID:", self.input_uuid)
        layout.addRow(help_label) # Baris tambahan untuk instruksi
        layout.addRow(self.input_columns)
        
        self.btn_save = QPushButton("Simpan Konfigurasi")
        self.btn_save.setFixedHeight(35)
        self.btn_save.clicked.connect(self.validate_and_accept)
        layout.addRow(self.btn_save)

    def validate_and_accept(self):
        try:
            # Jika tidak kosong, validasi apakah JSON benar
            text = self.input_columns.toPlainText().strip()
            if text:
                json.loads(text)
                
            if not self.input_nama.text().strip(): 
                raise ValueError("Nama survei wajib diisi")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Validasi Gagal", 
                                 "Pastikan format JSON benar atau kosongkan sama sekali.\nError: " + str(e))

    def get_data(self):
        # Ambil teks, jika kosong kembalikan dictionary kosong {}
        text = self.input_columns.toPlainText().strip()
        columns_data = json.loads(text) if text else {}
        
        return {
            "nama": self.input_nama.text().upper().strip(),
            "config": {
                "period_id": self.input_period.text().strip(),
                "uuid": self.input_uuid.text().strip(),
                "columns": columns_data
            }
        }

class ScraperWorker(QThread):
    finished = pyqtSignal(str)
    log_signal = pyqtSignal(str)

    def __init__(self, survey_key, settings):
        super().__init__()
        self.survey_key = survey_key
        self.settings = settings

    def run(self):
        try:
            scraper = FasihScraper()
            scraper.session.cookies.clear() 
            self.log_signal.emit("Membersihkan session dan memulai autentikasi...")

            discovery = auto_discovery_login(config.BASE_API_URL, self.settings['uuid'])
            if not discovery or not discovery.get("metadata"):
                self.finished.emit("Autentikasi gagal. Periksa koneksi VPN.")
                return

            scraper.session.headers.update(discovery["headers"])
            scraper.metadata = discovery["metadata"]

            self.log_signal.emit(f"Menelusuri wilayah Kabupaten ID: {config.SELECTED_KAB_ID}...")
            kec_list = scraper.get_sub_regions("level3", config.SELECTED_KAB_ID)
            
            if not kec_list:
                self.finished.emit("Daftar kecamatan tidak ditemukan.")
                return

            all_desas = []
            for kec in kec_list:
                desa_list = scraper.get_sub_regions("level4", kec['id'])
                for d in desa_list:
                    all_desas.append({
                        "id": d['id'], "name": d['name'], 
                        "kec_id": kec['id'], "kec_name": kec['name']
                    })
                time.sleep(0.1)

            self.log_signal.emit(f"Memulai scraping {len(all_desas)} desa (3 threads)...")
            final_results = []
            
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {
                    executor.submit(
                        scraper.fetch_all_data_per_desa, 
                        self.settings['period_id'], d['id'], d['name'], d['kec_id']
                    ): d for d in all_desas
                }
                for future in as_completed(futures):
                    d_info = futures[future]
                    try:
                        res = future.result()
                        if res:
                            for r in res:
                                r['kecamatan_asal'] = d_info['kec_name']
                                r['desa_asal'] = d_info['name']
                            final_results.extend(res)
                            self.log_signal.emit(f"Selesai: {d_info['name']}")
                    except Exception as e:
                        self.log_signal.emit(f"Gagal di {d_info['name']}: {str(e)}")

            if final_results:
                if not os.path.exists("data"): os.makedirs("data")
                df = pd.DataFrame(final_results)
                mapping = self.settings.get("columns")
                if mapping:
                    existing = [c for c in mapping.keys() if c in df.columns]
                    df = df[existing].rename(columns=mapping)
                
                path_out = os.path.join("data", f"{config.tgl_str}_{self.survey_key}.csv")
                df.to_csv(path_out, index=False, sep=';', quoting=csv.QUOTE_ALL, encoding='utf-8')
                self.finished.emit(f"Proses selesai. File: {path_out}")
            else:
                self.finished.emit("Data tidak ditemukan.")
        except Exception as e:
            self.finished.emit(f"Kesalahan fatal: {str(e)}")

class FasihGui(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.load_survey_list()

    def initUI(self):
        self.setWindowTitle("Fasih Scraper - BPS Kabupaten Sampang")
        self.setGeometry(100, 100, 800, 650)
        widget = QWidget()
        self.setCentralWidget(widget)
        layout = QVBoxLayout(widget)

        # Title
        title = QLabel("Fasih Scraper")
        title.setFont(QFont("Segoe UI", 16, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title.setStyleSheet("margin-bottom: 10px; color: #2c3e50;")
        layout.addWidget(title)

        # Group 1: Survey Selection & Management
        row1 = QHBoxLayout()
        self.combo_survey = QComboBox()
        self.combo_survey.setFixedHeight(30)
        row1.addWidget(QLabel("Daftar Survei:"), 0)
        row1.addWidget(self.combo_survey, 1)
        
        # Tombol Management (Ukuran Konsisten)
        self.btn_add = QPushButton("Tambah")
        self.btn_edit = QPushButton("Edit")
        self.btn_delete = QPushButton("Hapus")
        self.btn_reload = QPushButton("Refresh")
        
        for btn in [self.btn_add, self.btn_edit, self.btn_delete, self.btn_reload]:
            btn.setFixedWidth(80)
            btn.setFixedHeight(30)
            row1.addWidget(btn)

        self.btn_add.clicked.connect(lambda: self.open_editor(False))
        self.btn_edit.clicked.connect(lambda: self.open_editor(True))
        self.btn_delete.clicked.connect(self.delete_survey)
        self.btn_reload.clicked.connect(self.load_survey_list)
        
        layout.addLayout(row1)

        # Group 2: Action Button
        self.btn_start = QPushButton("Mulai Scraping")
        self.btn_start.setFixedHeight(45)
        self.btn_start.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_start.setStyleSheet("""
            QPushButton {
                background-color: #27ae60; 
                color: white; 
                font-weight: bold; 
                border-radius: 4px;
                font-size: 14px;
            }
            QPushButton:hover { background-color: #2ecc71; }
            QPushButton:disabled { background-color: #bdc3c7; }
        """)
        self.btn_start.clicked.connect(self.start_scraping)
        layout.addWidget(self.btn_start)

        # Console Log
        self.log_console = QTextEdit()
        self.log_console.setReadOnly(True)
        self.log_console.setStyleSheet("""
            background-color: #2c3e50; 
            color: #ecf0f1; 
            font-family: 'Consolas', 'Courier New'; 
            font-size: 12px;
            border: 1px solid #34495e;
            padding: 5px;
        """)
        layout.addWidget(self.log_console)

        # Footer
        footer = QLabel("Pranata Komputer - BPS Kabupaten Sampang (abdullah.ridwan@bps.go.id)")
        footer.setAlignment(Qt.AlignmentFlag.AlignCenter)
        footer.setStyleSheet("color: #7f8c8d; font-size: 11px; margin-top: 5px;")
        layout.addWidget(footer)

    def load_survey_list(self):
        self.combo_survey.clear()
        if os.path.exists(JSON_FILE):
            try:
                with open(JSON_FILE, "r") as f:
                    self.combo_survey.addItems(sorted(json.load(f).keys()))
            except: pass

    def delete_survey(self):
        key = self.combo_survey.currentText()
        if not key: return
        
        confirm = QMessageBox.warning(self, "Hapus Konfigurasi", 
                                     f"PERINGATAN: Konfigurasi '{key}' akan dihapus permanen.\n\n"
                                     "Apakah Anda benar-benar yakin?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No) # Set default ke NO
        
        if confirm == QMessageBox.StandardButton.Yes:
            with open(JSON_FILE, "r") as f: surveys = json.load(f)
            if key in surveys:
                del surveys[key]
                with open(JSON_FILE, "w") as f: json.dump(surveys, f, indent=4)
                self.load_survey_list()
                self.log_console.append(f"Konfigurasi '{key}' telah dihapus.")

    def open_editor(self, is_edit):
        old_key = self.combo_survey.currentText() if is_edit else None
        
        # Konfirmasi sebelum Edit
        if is_edit:
            if not old_key: return
            confirm = QMessageBox.question(self, "Konfirmasi Edit", 
                                         f"Anda akan mengubah konfigurasi '{old_key}'. Lanjutkan?",
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if confirm == QMessageBox.StandardButton.No: return
            
        data = {}
        if is_edit and old_key:
            with open(JSON_FILE, "r") as f: data = json.load(f).get(old_key, {})
        
        dialog = SurveyEditorDialog(self, old_key, data)
        if dialog.exec():
            res = dialog.get_data()
            new_key = res['nama']
            
            with open(JSON_FILE, "r") as f: surveys = json.load(f)
            if is_edit and old_key in surveys and old_key != new_key:
                del surveys[old_key]
                
            surveys[new_key] = res['config']
            with open(JSON_FILE, "w") as f: json.dump(surveys, f, indent=4)
            
            self.load_survey_list()
            self.combo_survey.setCurrentText(new_key)
            self.log_console.append(f"Berhasil menyimpan survei: {new_key}")

    def start_scraping(self):
        key = self.combo_survey.currentText()
        if not key: return
        
        # Dialog Konfirmasi Scraping
        confirm = QMessageBox.question(self, "Konfirmasi Scraping", 
                                     f"Mulai ambil data untuk survei '{key}'?\n\n"
                                     "Pastikan:\n"
                                     "1. Koneksi VPN BPS sudah aktif.\n"
                                     "2. Hasil scraping dapat diakses di folder 'data'.",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if confirm == QMessageBox.StandardButton.No:
            return
        
        with open(JSON_FILE, "r") as f: settings = json.load(f).get(key)
        
        self.btn_start.setEnabled(False)
        self.log_console.clear()
        self.log_console.append(f"Menjalankan proses: {key}...")
        
        self.worker = ScraperWorker(key, settings)
        self.worker.log_signal.connect(self.log_console.append)
        self.worker.finished.connect(self.on_finished)
        self.worker.start()

    def on_finished(self, result):
        self.log_console.append(result)
        self.btn_start.setEnabled(True)
        QMessageBox.information(self, "Informasi", result)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion") 
    win = FasihGui()
    win.show()
    sys.exit(app.exec())