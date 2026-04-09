import json
from PyQt6.QtWidgets import QDialog, QFormLayout, QLineEdit, QTextEdit, QLabel, QPushButton, QMessageBox

class SurveyEditorDialog(QDialog):
    def __init__(self, parent=None, survey_key=None, data=None):
        super().__init__(parent)
        self.setWindowTitle("Editor Survei" if survey_key else "Tambah Survei")
        self.setFixedWidth(500)
        self.old_key = survey_key
        
        layout = QFormLayout(self)
        self.input_nama = QLineEdit(survey_key if survey_key else "")
        self.input_period = QLineEdit(data.get('period_id', "") if data else "")
        self.input_uuid = QLineEdit(data.get('uuid', "") if data else "")
        
        self.input_columns = QTextEdit()
        self.input_columns.setFixedHeight(150)
        self.input_columns.setPlaceholderText(
            "Kosongkan jika ingin mengambil semua kolom.\n\n"
            "Atau format JSON:\n"
            "{\n  \"kolom_api\": \"Nama_CSV\"\n}"
        )
        
        if data and data.get('columns'):
            self.input_columns.setPlainText(json.dumps(data.get('columns'), indent=4))
        
        help_label = QLabel("Mapping Kolom (JSON Format):")
        help_label.setStyleSheet("color: #2980b9; font-size: 10px; font-style: italic;")

        layout.addRow("Nama Survei:", self.input_nama)
        layout.addRow("Period ID:", self.input_period)
        layout.addRow("UUID:", self.input_uuid)
        layout.addRow(help_label)
        layout.addRow(self.input_columns)
        
        self.btn_save = QPushButton("Simpan Konfigurasi")
        self.btn_save.setFixedHeight(35)
        self.btn_save.clicked.connect(self.validate_and_accept)
        layout.addRow(self.btn_save)

    def validate_and_accept(self):
        try:
            text = self.input_columns.toPlainText().strip()
            if text: json.loads(text)
            if not self.input_nama.text().strip(): raise ValueError("Nama survei wajib diisi")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Validasi Gagal", f"Error: {str(e)}")

    def get_data(self):
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