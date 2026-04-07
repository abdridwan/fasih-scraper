import os
import datetime
from dotenv import load_dotenv

load_dotenv()
tgl_str = datetime.datetime.now().strftime("%Y%m%d_%H%M")

# Kredensial SSO BPS
USERNAME = os.getenv("SSO_USERNAME", "username")
PASSWORD = os.getenv("SSO_PASSWORD", "password")

# URL Dasar
BASE_API_URL = "https://fasih-sm.bps.go.id"

# Pengaturan Batasan
MAX_LIMIT_PER_DESA = 1000 # Limit internal per request desa
PAGE_SIZE = 100 # Standar Datatable BPS

# Target Wilayah (Statis di tingkat Kabupaten)
# Ganti dengan ID Kabupaten/Kota target Anda
SELECTED_KAB_ID = os.getenv("SELECTED_KAB_ID", "c2349819-44e6-433b-bf14-f674b4778f88")

SURVEY_SETTINGS = {
    "PBI_JKN": {
        "period_id": "39136966-8f3c-4a0c-915b-0f65eb223475", # Sesuai payload Anda
        "uuid": "8712a6fc-a996-4a8f-ad6f-56a278c19288",
        "output": f"{tgl_str}_PBI_JKN.csv"
    }
}
