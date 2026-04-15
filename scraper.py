import requests
import time
import logging
import random
import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FasihScraper:
    def __init__(self):
        self.base_url = "https://fasih-sm.bps.go.id"
        self.session = requests.Session()
        self.metadata = {} # Akan diisi dari Playwright (groupId, reg1, reg2)

    def get_sub_regions(self, level_name, parent_id):
        """
        level_name: 'level3', 'level4', dst.
        parent_id: ID dari level satu tingkat di atasnya
        """
        url = f"{config.BASE_API_URL}/region/api/v1/region/{level_name}"
        
        # Ambil angka level dari string 'level4' -> 4
        current_level_num = int(''.join(filter(str.isdigit, level_name)))
        parent_level_num = current_level_num - 1
        
        # Bangun parameter: level3Id, level4Id, dsb.
        params = {
            "groupId": self.metadata.get('groupId'),
            f"level{parent_level_num}Id": parent_id
        }
        
        # Khusus untuk level 3, BPS seringkali minta level1FullCode
        if level_name == "level3":
            params["level1FullCode"] = config.TARGET_KAB_CODE[:2]

        try:
            res = self.session.get(url, params=params, timeout=20)
            if res.status_code == 200:
                return res.json().get('data', [])
            return []
        except:
            return []

    def fetch_all_data_per_desa(self, period_id, desa_id, desa_name, kec_id):
        """Worker dengan perbaikan logika agar tidak ada ID yang hilang/duplikat"""
        all_rows = []
        start = 0
        # 1. PERBAIKAN: Set page_size ke 1000. 
        # Karena max data per desa Anda < 1000, maka setiap desa cukup 1x HIT.
        # Ini menghapus resiko 'drifting' atau duplikasi antar halaman.
        page_size = 1000 
        endpoint = f"{self.base_url}/analytic/api/v2/assignment/datatable-all-user-survey-periode"

        while True:
            payload = {
                "draw": (start // page_size) + 1,
                "columns": self._generate_columns(), # Pastikan kolom terdefinisi
                "start": start,
                "length": page_size,
                # 2. PERBAIKAN: Tambahkan Order yang stabil (berdasarkan ID)
                "order": [{"column": 0, "dir": "asc"}], 
                "search": {"value": "", "regex": False},
                "assignmentExtraParam": {
                    "region1Id": self.metadata.get('region1Id'),
                    "region2Id": self.metadata.get('region2Id'),
                    "region3Id": kec_id,
                    "region4Id": desa_id,
                    "surveyPeriodId": period_id,
                    "regionId": desa_id,
                    "filterTargetType": "TARGET_ONLY"
                }
            }
            
            try:
                # Gunakan session yang sama, namun pastikan timeout cukup lama
                res = self.session.post(endpoint, json=payload, timeout=60)
                if res.status_code != 200: 
                    # Jika gagal, coba sekali lagi (Simple Retry)
                    time.sleep(2)
                    res = self.session.post(endpoint, json=payload, timeout=60)
                    if res.status_code != 200: break
                
                res_json = res.json()
                total_hit = res_json.get("totalHit", 0)

                if total_hit == 0:
                    return None
                
                data = res_json.get("searchData", [])
                if not data: break
                
                all_rows.extend(data)

                # Jika sudah mendapatkan semua data sesuai total_hit, berhenti
                if len(all_rows) >= total_hit or len(data) < page_size:
                    break
                
                start += page_size
                # Jeda sedikit antar desa agar tidak dianggap DDoS oleh F5
                time.sleep(random.uniform(0.7, 1.5)) 
                
            except Exception as e:
                logger.error(f"❌ Error di desa {desa_name}: {e}")
                break
        
        return all_rows

    def _generate_columns(self):
        """Helper untuk mendefinisikan kolom agar sorting 'column 0' bekerja"""
        cols = [{"data": "id", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}}]
        for i in range(1, 11):
            cols.append({"data": f"data{i}", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}})
        return cols