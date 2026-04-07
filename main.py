import pandas as pd
import config
import time
import csv
import random
import os
from scraper import FasihScraper
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright

def auto_discovery_login(base_url, survey_uuid):
    print("🌐 Membuka browser untuk Identifikasi & Auth...")
    auth_results = {"headers": {}, "metadata": None}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context()
        page = context.new_page()

        # --- FUNGSI KRUSIAL: MENANGKAP BEARER TOKEN ---
        def intercept_auth(request):
            if "/api/" in request.url:
                headers = request.headers
                if "authorization" in headers and "bearer" in headers["authorization"].lower():
                    # Simpan token ke auth_results
                    auth_results["headers"]["Authorization"] = headers["authorization"]

        page.on("request", intercept_auth)

        try:
            print("🔗 Menghubungi pangkalan utama...")
            page.goto(f"{base_url.rstrip('/')}/", timeout=60000)
            
            # Login SSO
            page.wait_for_selector('a[href*="/oauth2/authorization/ics"]', timeout=20000)
            page.click('a[href*="/oauth2/authorization/ics"]')
            page.fill('input[name="username"]', config.USERNAME)
            page.fill('input[name="password"]', config.PASSWORD)
            page.click('input#kc-login')
            page.wait_for_load_state("networkidle")

            # Navigasi ke koleksi untuk memicu metadata & token API
            print(f"📡 Mengidentifikasi metadata wilayah...")
            collect_url = f"{base_url.rstrip('/')}/survey-collection/collect/{survey_uuid}"
            
            with page.expect_response(lambda res: "region-metadata" in res.url and res.status == 200) as response_info:
                page.goto(collect_url, wait_until="networkidle")
                json_res = response_info.value.json()
                inner_data = json_res.get("data", {})
                
                if inner_data.get("id"):
                    auth_results["metadata"] = {
                        "groupId": inner_data.get("id"),
                        "region1Id": inner_data.get("region1Id"),
                        "region2Id": inner_data.get("region2Id")
                    }
                    print(f"✅ Metadata & GroupID Berhasil Didapat.")

            # --- SINKRONISASI COOKIE & HEADER LAIN ---
            all_cookies = context.cookies()
            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in all_cookies])
            
            # Update headers tanpa menghapus Authorization yang ditangkap interceptor
            auth_results["headers"].update({
                "cookie": cookie_str,
                "User-Agent": page.evaluate("navigator.userAgent"),
                "Referer": collect_url,
                "Origin": base_url.rstrip('/'),
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest"
            })

            # Ambil XSRF-TOKEN untuk POST
            for c in all_cookies:
                if c['name'].lower() == 'xsrf-token':
                    auth_results["headers"]["X-XSRF-TOKEN"] = c['value']
            
            # Beri jeda 3 detik agar interceptor sempat menangkap request API wilayah
            page.wait_for_timeout(3000)
            print(f"🍪 Cookie & Token disinkronkan.")

        except Exception as e:
            print(f"❌ Kesalahan: {e}")
        finally:
            browser.close()
            
    return auth_results

def main():
    SURVEY_KEY = "PBI_JKN"
    settings = config.SURVEY_SETTINGS.get(SURVEY_KEY)
    if not settings:
        print(f"❌ Konfigurasi untuk {SURVEY_KEY} tidak ditemukan.")
        return

    scraper = FasihScraper()

    # --- TAHAP 1: DISCOVERY & AUTH ---
    discovery = auto_discovery_login(config.BASE_API_URL, settings['uuid'])
    
    if not discovery.get("metadata") or not discovery["metadata"].get("groupId"):
        print("🛑 Identifikasi Gagal. Program tidak bisa melanjutkan tanpa GroupID.")
        return

    # Suntikkan Headers dan Metadata ke Scraper
    scraper.session.headers.update(discovery["headers"])
    scraper.metadata = discovery["metadata"]

    # --- TAHAP 2: PENELUSURAN WILAYAH ---
    print(f"\n📂 Menelusuri wilayah untuk Kabupaten: {config.SELECTED_KAB_ID}...")
    kec_list = scraper.get_sub_regions("level3", config.SELECTED_KAB_ID)
    
    if not kec_list:
        print("⚠️ Daftar kecamatan kosong. Masalah: Cookie F5 atau Token Auth ditolak.")
        return

    all_desas = []
    for kec in kec_list:
        desa_list = scraper.get_sub_regions("level4", kec['id'])
        for d in desa_list:
            all_desas.append({
                "id": d['id'], "name": d['name'], 
                "kec_id": kec['id'], "kec_name": kec['name']
            })
        print(f"  🏘️ {kec['name']}: {len(desa_list)} desa ditemukan.")
        time.sleep(random.uniform(0.2, 0.5))

    # --- TAHAP 3: SCRAPING DATA ---
    print(f"\n🚀 Memulai Multi-threaded Scraping ({len(all_desas)} desa)...")
    final_results = []
    target_total_hit = 0 # Variabel baru untuk audit
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                scraper.fetch_all_data_per_desa, 
                settings['period_id'], d['id'], d['name'], d['kec_id']
            ): d for d in all_desas
        }

        for future in as_completed(futures):
            d_info = futures[future]
            try:
                # Modifikasi fungsi fetch_all_data_per_desa Anda agar return (data, hit_count)
                # Jika tidak ingin ubah fungsi, kita asumsikan hit_count dari log tadi
                res = future.result() 
                
                if res:
                    # Tambahkan info wilayah
                    for row in res:
                        row['kecamatan_asal'] = d_info['kec_name']
                        row['desa_asal'] = d_info['name']
                    
                    final_results.extend(res)
                    print(f"✔️ {d_info['name']}: {len(res)} data berhasil ditambahkan.")
            except Exception as e:
                print(f"⚠️ Kesalahan fatal di desa {d_info['name']}: {e}")

    # --- TAHAP 4: EXPORT RAW & LOG DUPLIKAT ---
    if final_results:
        df = pd.DataFrame(final_results)
        
        # 1. Identifikasi kolom ID unik (Primary Key)
        id_col = next((c for c in ['id', 'assignment_id', 'codeIdentity'] if c in df.columns), None)
        
        if id_col:
            # 2. EKSTRAKSI BARIS DUPLIKAT KE FILE TERPISAH
            # keep=False: ambil semua baris yang kembar (misal ada 2 ID sama, ambil keduanya)
            df_duplikat = df[df.duplicated(subset=[id_col], keep=False)]
            
            if not df_duplikat.empty:
                # Urutkan agar data yang sama muncul berjajar (atas-bawah)
                df_duplikat = df_duplikat.sort_values(by=id_col)
                
                # Penamaan file log: 20260405_LOG_DUPLIKAT_PBI_JKN.csv
                nama_log = f"LOG_DUPLIKAT_{settings['output']}"
                
                df_duplikat.to_csv(nama_log, index=False, sep=';', quoting=csv.QUOTE_ALL, encoding='utf-8')
                print(f"⚠️ Terdeteksi {len(df_duplikat)} baris data duplikat.")
                print(f"📁 Detail duplikat telah disimpan ke: {nama_log}")
            else:
                print("✅ Tidak ditemukan data duplikat (Data Bersih).")

        # 3. SIMPAN FILE UTAMA (DATA MENTAH / RAW)
        # Sesuai permintaan Anda: Tetap simpan semua tanpa menghapus duplikat
        nama_output = settings['output']
        print(f"💾 Menyimpan total {len(df)} data mentah ke {nama_output}...")
        
        df.to_csv(
            nama_output, 
            index=False, 
            sep=';', 
            quoting=csv.QUOTE_ALL, 
            encoding='utf-8'
        )
        
        durasi = time.time() - start_time
        print(f"🏁 PROSES SELESAI dalam {durasi:.2f} detik.")
    else:
        print("❌ Tidak ada data yang berhasil dikumpulkan.")

if __name__ == "__main__":
    main()