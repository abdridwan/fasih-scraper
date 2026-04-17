import json
import os
import qtawesome as qta
from PyQt6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                             QComboBox, QPushButton, QTextEdit, QLabel, 
                             QMessageBox, QStackedWidget, QFrame, QCheckBox,
                             QFormLayout, QLineEdit, QProgressBar) 
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QFont
import config

# Import relative
from .dialogs import SurveyEditorDialog
from .workers import ScraperWorker

# Pastikan variabel ini didefinisikan dengan benar di level modul
JSON_FILE = "surveys.json"

class FasihGui(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        # Jalankan ini setelah UI siap
        self.load_survey_list()

    def initUI(self):
        self.setWindowTitle("Fasih Scraper - BPS Kabupaten Sampang")
        self.setMinimumSize(900, 650)
        
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QHBoxLayout(main_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # --- SIDEBAR ---
        self.sidebar = QFrame()
        self.sidebar.setFixedWidth(180)
        self.sidebar.setStyleSheet("""
            QFrame { background-color: #2c3e50; border: none; }
            QPushButton { 
                color: #ecf0f1; text-align: left; padding: 12px 15px; 
                border: none; font-size: 13px; background-color: transparent;
            }
            QPushButton:hover { background-color: #34495e; }
        """)
        sidebar_layout = QVBoxLayout(self.sidebar)
        sidebar_layout.setContentsMargins(0, 10, 0, 10)

        # 1. Definisi Tombol Sidebar
        self.btn_menu_scraping = QPushButton(" Menu Utama")
        self.btn_menu_scraping.setIcon(qta.icon('fa5s.tasks', color='white'))
        
        self.btn_menu_login = QPushButton(" Akun SSO")
        self.btn_menu_login.setIcon(qta.icon('fa5s.user-lock', color='white'))

        self.btn_menu_config = QPushButton(" Konfigurasi")
        self.btn_menu_config.setIcon(qta.icon('fa5s.cog', color='white'))
        
        # 2. Masukkan ke Layout Sidebar
        sidebar_layout.addWidget(self.btn_menu_scraping)
        sidebar_layout.addWidget(self.btn_menu_login)
        sidebar_layout.addWidget(self.btn_menu_config)
        
        sidebar_layout.addStretch()
        main_layout.addWidget(self.sidebar)

        # --- AREA KONTEN (Stacked Widget) ---
        self.pages = QStackedWidget()
        main_layout.addWidget(self.pages)

        # 3. Inisialisasi Halaman (PENTING: Buat dulu object-nya baru di addWidget)
        self.page_scraping_widget = self.create_page_scraping()
        self.page_login_widget = self.create_page_login()
        self.page_config_widget = self.create_page_config() # Pastikan fungsi ini sudah ada di class Anda
        
        # Tambahkan ke Stacked Widget berdasarkan index
        self.pages.addWidget(self.page_scraping_widget) # Index 0
        self.pages.addWidget(self.page_login_widget)    # Index 1
        self.pages.addWidget(self.page_config_widget)   # Index 2

        # 4. Hubungkan Navigasi
        self.btn_menu_scraping.clicked.connect(lambda: self.switch_page(0))
        self.btn_menu_login.clicked.connect(lambda: self.switch_page(1))
        self.btn_menu_config.clicked.connect(lambda: self.switch_page(2))
        
        # Set halaman default saat dibuka
        self.switch_page(0)

    def switch_page(self, index):
        self.pages.setCurrentIndex(index)
        
        # Style untuk tombol aktif dan normal
        style_active = "background-color: #27ae60; color: white; font-weight: bold;"
        style_normal = "background-color: transparent; color: #ecf0f1; font-weight: normal;"
        
        # Reset semua, lalu set yang aktif
        self.btn_menu_scraping.setStyleSheet(style_active if index == 0 else style_normal)
        self.btn_menu_login.setStyleSheet(style_active if index == 1 else style_normal)
        self.btn_menu_config.setStyleSheet(style_active if index == 2 else style_normal)

    def create_page_scraping(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        
        # --- ROW 1: Pemilihan Survei & Action Buttons ---
        row1 = QHBoxLayout()
        self.combo_survey = QComboBox()
        self.combo_survey.setFixedHeight(30)
        row1.addWidget(QLabel("Survei:"), 0)
        row1.addWidget(self.combo_survey, 1)
        
        self.btn_add = QPushButton(qta.icon('fa5s.plus'), "")
        self.btn_edit = QPushButton(qta.icon('fa5s.edit'), "")
        self.btn_delete = QPushButton(qta.icon('fa5s.trash-alt'), "")
        self.btn_reload = QPushButton(qta.icon('fa5s.sync-alt'), "")

        for btn in [self.btn_add, self.btn_edit, self.btn_delete, self.btn_reload]:
            btn.setFixedSize(35, 30)
            row1.addWidget(btn)

        self.btn_add.clicked.connect(lambda: self.open_editor(False))
        self.btn_edit.clicked.connect(lambda: self.open_editor(True))
        self.btn_delete.clicked.connect(self.delete_survey)
        self.btn_reload.clicked.connect(lambda: [self.load_survey_list(), self.reset_progress_bar()])
        layout.addLayout(row1)

        # --- ROW 2: Opsi Google Drive ---
        self.check_drive = QCheckBox("Kirim ke Google Drive")
        layout.addWidget(self.check_drive)

        # --- ROW 3: Progress Bar (Kotak, Tetap Muncul, Angka Dinamis) ---
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(25)
        self.progress_bar.setValue(0)
        # Kondisi awal: Sembunyikan angka jika masih 0%
        self.progress_bar.setTextVisible(False) 
        self.progress_bar.setFormat(" %p% Selesai ")
        
        # Style Kotak (border-radius: 0px)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 2px solid #dcdde1;
                border-radius: 0px; 
                background-color: #f5f6fa;
                text-align: center;
                font-size: 13px;
                font-weight: bold;
                color: #2c3e50;
            }
            QProgressBar::chunk {
                background-color: #27ae60;
                border-radius: 0px;
                margin: 0px;
            }
        """)
        layout.addWidget(self.progress_bar)

        # --- ROW 4: Tombol Utama (Start/Stop) ---
        row_btn = QHBoxLayout()
        self.btn_start = QPushButton(qta.icon('fa5s.play', color='white'), " Mulai")
        self.btn_start.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold;")
        self.btn_start.setFixedHeight(40)
        
        self.btn_stop = QPushButton(qta.icon('fa5s.stop', color='white'), " Stop")
        self.btn_stop.setStyleSheet("background-color: #c0392b; color: white; font-weight: bold;")
        self.btn_stop.setFixedHeight(40)
        self.btn_stop.setEnabled(False)
        
        row_btn.addWidget(self.btn_start, 2)
        row_btn.addWidget(self.btn_stop, 1)
        layout.addLayout(row_btn)

        # --- ROW 5: Console Log ---
        self.log_console = QTextEdit()
        self.log_console.setReadOnly(True)
        self.log_console.setStyleSheet("background-color: #1e293b; color: #38bdf8; font-family: 'Consolas';")
        layout.addWidget(self.log_console)

        # Event Connections
        self.btn_start.clicked.connect(self.start_scraping)
        self.btn_stop.clicked.connect(self.stop_scraping)

        return page

    def create_page_login(self):
        page = QWidget()
        # Membuat layout utama dengan padding yang nyaman di mata
        layout = QVBoxLayout(page)
        layout.setContentsMargins(50, 50, 50, 50)
        layout.setSpacing(20)

        # --- HEADER HALAMAN ---
        header_layout = QHBoxLayout()
        icon_user = QLabel()
        icon_user.setPixmap(qta.icon('fa5s.user-shield', color='#2c3e50').pixmap(QSize(40, 40)))
        
        title_text = QVBoxLayout()
        title_label = QLabel("Pengaturan Akun")
        title_label.setStyleSheet("font-size: 20px; font-weight: bold; color: #2c3e50;")
        subtitle_label = QLabel("Kelola kredensial SSO BPS untuk autentikasi otomatis.")
        subtitle_label.setStyleSheet("color: #7f8c8d; font-size: 12px;")
        title_text.addWidget(title_label)
        title_text.addWidget(subtitle_label)
        
        header_layout.addWidget(icon_user)
        header_layout.addLayout(title_text)
        header_layout.addStretch()
        layout.addLayout(header_layout)

        # --- FORM INPUT ---
        # Membuat semacam "Card" manual dengan QFrame
        card = QFrame()
        card.setStyleSheet("""
            QFrame { 
                background-color: white; 
                border: 1px solid #dcdde1; 
                border-radius: 10px; 
            }
            QLabel { border: none; color: #2f3640; font-weight: bold; }
            QLineEdit { 
                border: 1px solid #dcdde1; 
                border-radius: 5px; 
                padding: 8px; 
                background: #f5f6fa;
            }
            QLineEdit:focus { border: 1px solid #27ae60; background: white; }
        """)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(30, 30, 30, 30)

        import config
        form = QFormLayout()
        form.setSpacing(15)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)

        self.input_username = QLineEdit(getattr(config, 'USERNAME', ''))
        self.input_username.setPlaceholderText("Masukkan NIP/Username SSO")
        
        self.input_password = QLineEdit(getattr(config, 'PASSWORD', ''))
        self.input_password.setPlaceholderText("Masukkan Password SSO")
        self.input_password.setEchoMode(QLineEdit.EchoMode.Password)

        form.addRow("Username SSO", self.input_username)
        form.addRow("Password SSO", self.input_password)
        card_layout.addLayout(form)
        
        layout.addWidget(card)

        # --- TOMBOL AKSI ---
        self.btn_save_credentials = QPushButton(" Simpan Perubahan")
        self.btn_save_credentials.setIcon(qta.icon('fa5s.save', color='white'))
        self.btn_save_credentials.setFixedHeight(45)
        self.btn_save_credentials.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_save_credentials.setStyleSheet("""
            QPushButton { 
                background-color: #2c3e50; 
                color: white; 
                font-weight: bold; 
                border-radius: 5px; 
                font-size: 14px;
            }
            QPushButton:hover { background-color: #34495e; }
            QPushButton:pressed { background-color: #1e272e; }
        """)
        
        # Hubungkan ke fungsi simpan
        self.btn_save_credentials.clicked.connect(self.save_credentials)
        layout.addWidget(self.btn_save_credentials)

        # --- INFO TAMBAHAN ---
        note = QLabel(
            "Note: Kredensial disimpan secara lokal di file .env.\n"
            "Pastikan Anda terhubung dengan VPN BPS saat menjalankan scraper."
        )
        note.setStyleSheet("color: #95a5a6; font-size: 11px; font-style: italic;")
        note.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(note)

        layout.addStretch() # Mendorong semua konten ke atas
        return page
    
    def create_page_config(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(50, 50, 50, 50)

        # Header
        header = QLabel("Konfigurasi Sistem (.env)")
        header.setStyleSheet("font-size: 20px; font-weight: bold; color: #2c3e50;")
        layout.addWidget(header)

        # Card Form
        card = QFrame()
        card.setStyleSheet("background-color: white; border: 1px solid #dcdde1; border-radius: 10px;")
        card_layout = QFormLayout(card)
        card_layout.setContentsMargins(20, 20, 20, 20)
        card_layout.setSpacing(15)

        # Input Fields
        self.input_kab_code = QLineEdit(str(getattr(config, 'TARGET_KAB_CODE', '3527')))
        self.input_api_url = QLineEdit(getattr(config, 'BASE_API_URL', ''))
        self.input_drive_folder = QLineEdit(getattr(config, 'GD_FOLDER_ID', ''))

        card_layout.addRow("Kode Kabupaten (TARGET_KAB_CODE)", self.input_kab_code)
        card_layout.addRow("Base API URL", self.input_api_url)
        card_layout.addRow("Google Drive Folder ID", self.input_drive_folder)

        layout.addWidget(card)

        # Tombol Simpan
        btn_save = QPushButton(" Simpan Konfigurasi")
        btn_save.setIcon(qta.icon('fa5s.save', color='white'))
        btn_save.setFixedHeight(45)
        btn_save.setStyleSheet("background-color: #2c3e50; color: white; font-weight: bold; border-radius: 5px;")
        btn_save.clicked.connect(self.save_env_config)
        layout.addWidget(btn_save)

        layout.addStretch()
        return page

    def load_survey_list(self):
        self.combo_survey.clear()
        if os.path.exists(JSON_FILE):
            try:
                with open(JSON_FILE, "r") as f:
                    data = json.load(f)
                    if data:
                        self.combo_survey.addItems(sorted(data.keys()))
            except Exception as e:
                print(f"Error loading JSON: {e}")

    def open_editor(self, is_edit):
        key = self.combo_survey.currentText() if is_edit else None
        data = {}
        if is_edit and key:
            try:
                with open(JSON_FILE, "r") as f:
                    data = json.load(f).get(key, {})
            except: pass

        dialog = SurveyEditorDialog(self, key, data)
        if dialog.exec():
            res = dialog.get_data()
            surveys = {}
            if os.path.exists(JSON_FILE):
                try:
                    with open(JSON_FILE, "r") as f:
                        surveys = json.load(f)
                except: surveys = {}

            if is_edit and key in surveys:
                if key != res['nama']:
                    del surveys[key]
            
            surveys[res['nama']] = res['config']
            with open(JSON_FILE, "w") as f:
                json.dump(surveys, f, indent=4)
            
            self.load_survey_list()
            self.combo_survey.setCurrentText(res['nama'])

    def start_scraping(self):
        key = self.combo_survey.currentText()
        if not key:
            QMessageBox.warning(self, "Peringatan", "Pilih survei terlebih dahulu!")
            return
            
        try:
            with open(JSON_FILE, "r") as f:
                settings = json.load(f).get(key)
        except:
            return
        
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.log_console.clear()
        self.progress_bar.setValue(0) # Reset progress

        self.worker = ScraperWorker(key, settings, auto_upload=self.check_drive.isChecked())
        self.worker.log_signal.connect(self.log_console.append)
        
        # HUBUNGKAN SINYAL PROGRESS
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        
        self.worker.finished.connect(self.on_finished)
        self.worker.start()

    def stop_scraping(self):
        if hasattr(self, 'worker') and self.worker.isRunning():
            self.worker.stop()
            self.log_console.append("\n🛑 Menghentikan proses...")
            self.btn_stop.setEnabled(False)

    def on_finished(self, result):
        self.log_console.append(f"\n{result}")
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        QMessageBox.information(self, "Selesai", result)

    def delete_survey(self):
        key = self.combo_survey.currentText()
        if not key: return
        
        ans = QMessageBox.question(self, "Hapus", f"Hapus konfigurasi {key}?", 
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ans == QMessageBox.StandardButton.Yes:
            if os.path.exists(JSON_FILE):
                with open(JSON_FILE, "r") as f:
                    surveys = json.load(f)
                if key in surveys:
                    del surveys[key]
                    with open(JSON_FILE, "w") as f:
                        json.dump(surveys, f, indent=4)
                    self.load_survey_list()
    
    def save_credentials(self):
        """Fungsi untuk menyimpan username & password ke .env tanpa menimpa data lain"""
        username = self.input_username.text().strip()
        password = self.input_password.text().strip()

        if not username or not password:
            QMessageBox.warning(self, "Validasi Gagal", "Username dan Password wajib diisi!")
            return

        try:
            from dotenv import set_key
            env_path = ".env"
            
            # Buat file jika belum ada
            if not os.path.exists(env_path):
                with open(env_path, "w") as f: pass

            # Menggunakan set_key agar hanya mengubah field yang dimaksud
            set_key(env_path, "SSO_USERNAME", username)
            set_key(env_path, "SSO_PASSWORD", password)

            # Update variabel runtime agar config.py segera tahu perubahannya
            import config
            config.USERNAME = username
            config.PASSWORD = password

            QMessageBox.information(self, "Sukses", "Kredensial berhasil diperbarui!")
            
            # Log ke console dashboard jika perlu
            if hasattr(self, 'log_console'):
                self.log_console.append(f"System: Akun diperbarui ({username})")

        except Exception as e:
            QMessageBox.critical(self, "Kesalahan Sistem", f"Gagal menyimpan file .env: {str(e)}")
    
    def save_env_config(self):
        try:
            from dotenv import set_key
            env_path = ".env"
            
            # Ambil data dari input GUI
            kab_code = self.input_kab_code.text().strip()
            api_url = self.input_api_url.text().strip()
            folder_id = self.input_drive_folder.text().strip()

            # Simpan ke file .env
            set_key(env_path, "TARGET_KAB_CODE", kab_code)
            set_key(env_path, "BASE_API_URL", api_url)
            set_key(env_path, "GD_FOLDER_ID", folder_id)

            # Update variabel runtime di config.py
            import config
            config.TARGET_KAB_CODE = kab_code
            config.BASE_API_URL = api_url
            config.GD_FOLDER_ID = folder_id

            QMessageBox.information(self, "Sukses", "Konfigurasi .env berhasil diperbarui!")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Gagal menyimpan: {str(e)}")

    def reset_progress_bar(self):
        """Fungsi untuk mengembalikan progress bar ke kondisi awal"""
        if hasattr(self, 'progress_bar'):
            self.progress_bar.setValue(0)
            self.progress_bar.setTextVisible(False)
            # Log console juga dibersihkan agar fresh
            self.log_console.clear() 
            self.log_console.append("🔄 System: Konfigurasi di-reload dan progres di-reset.")