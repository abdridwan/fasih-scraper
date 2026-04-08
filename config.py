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
