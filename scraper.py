import requests
import time
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FasihScraper:
    def __init__(self):
        self.base_url = "https://fasih-sm.bps.go.id"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/analytic/monitoring/survey-periode",
            "Connection": "keep-alive"
        })

    def _robust_pull(self, endpoint, payload, unit_name, query_label):
        max_retries = 4
        for attempt in range(max_retries):
            try:
                res = self.session.post(endpoint, json=payload, timeout=60)
                if res.status_code == 200:
                    return res.json().get("searchData", [])
                time.sleep(random.uniform(2.0, 4.0) * (attempt + 1))
            except Exception:
                time.sleep(random.uniform(2.0, 4.0) * (attempt + 1))
        return []

    def _generate_columns(self, target_search=""):
        cols = [{"data": "id", "name": "", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}}]
        for i in range(1, 15):
            # Suntikkan search ke data3 jika sedang mode filtering
            val = target_search if i == 3 else ""
            cols.append({"data": f"data{i}", "name": "", "searchable": True, "orderable": True, "search": {"value": val, "regex": False}})
        return cols

    def fetch_all_data_per_unit(self, period_id, unit_id, unit_name, region_context, sls_list=[]):
        all_rows = []
        seen_ids = set()
        endpoint = f"{self.base_url}/analytic/api/v2/assignment/datatable-all-user-survey-periode"

        # Parameter Utama (Desa)
        extra_param = {
            "surveyPeriodId": period_id,
            "regionId": unit_id, # ID Desa
            "filterTargetType": "TARGET_ONLY",
            **region_context
        }

        # --- LANGKAH 1: TEMBAK DESA DULU ---
        payload = {
            "draw": 1, "columns": self._generate_columns(),
            "start": 0, "length": 1000,
            "order": [{"column": 0, "dir": "asc"}],
            "search": {"value": "", "regex": False},
            "assignmentExtraParam": extra_param
        }
        
        initial_data = self._robust_pull(endpoint, payload, unit_name, "Check Desa")

        # --- LANGKAH 2: CEK KONDISI ---
        if len(initial_data) >= 1000 and sls_list:
            # JIKA DATA DESA MENTOK & PUNYA ANAK SLS
            logger.warning(f"⚠️ {unit_name}: Data > 1000. Turun ke level SLS untuk tarik utuh...")
            
            for sls in sls_list:
                # Timpa parameter desa dengan ID SLS
                sls_param = extra_param.copy()
                sls_param.update(sls['hierarchy']) # Masukkan region5Id
                
                sls_payload = payload.copy()
                sls_payload["assignmentExtraParam"] = sls_param
                
                # Tarik data per SLS
                sls_data = self._robust_pull(endpoint, sls_payload, unit_name, f"SLS {sls['name']}")
                
                for r in sls_data:
                    if r['id'] not in seen_ids:
                        seen_ids.add(r['id'])
                        r['nama_wilayah_target'] = unit_name
                        all_rows.append(r)
                
                time.sleep(random.uniform(0.5, 1.0)) # Jeda manusiawi
        else:
            # JIKA AMAN ATAU TIDAK PUNYA SLS
            for r in initial_data:
                if r['id'] not in seen_ids:
                    seen_ids.add(r['id'])
                    r['nama_wilayah_target'] = unit_name
                    all_rows.append(r)

        return all_rows