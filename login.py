import config
import time
from playwright.sync_api import sync_playwright

def auto_discovery_login(base_url, survey_uuid, max_retries=3):
    # Struktur hasil tanpa Authorization/Bearer
    auth_results = {"headers": {}, "metadata": None}
    TIMEOUT_MS = 90000 

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(TIMEOUT_MS)

        for attempt in range(1, max_retries + 1):
            try:
                # 1. LOGIN SSO
                print(f"🌐 Percobaan {attempt}: Menuju halaman login...")
                page.goto(f"{base_url.rstrip('/')}/", wait_until="commit")
                
                btn_selector = 'a[href*="/oauth2/authorization/ics"]'
                page.wait_for_selector(btn_selector, state="visible")
                page.click(btn_selector)

                # Isi Kredensial SSO
                page.wait_for_selector('input[name="username"]')
                page.fill('input[name="username"]', config.USERNAME)
                page.fill('input[name="password"]', config.PASSWORD)
                
                print("🔑 Mengirim data login...")
                with page.expect_navigation(timeout=TIMEOUT_MS):
                    page.click('input#kc-login')

                # 2. REDIRECT KE ROOT (Pastikan daftar survei muncul)
                # Menunggu tabel dengan ID Pencacahan agar session cookie benar-benar terdaftar
                print("⏳ Menunggu dashboard utama stabil...")
                page.wait_for_url(f"**{base_url.rstrip('/')}/**")
                page.wait_for_selector("table#Pencacahan", state="visible", timeout=30000)
                page.wait_for_load_state("networkidle")

                # 3. REDIRECT KE COLLECTION DATA
                # Kita arahkan ke URL collect untuk memicu API region-metadata
                collect_url = f"{base_url.rstrip('/')}/survey-collection/collect/{survey_uuid}"
                print(f"📡 Navigasi ke koleksi: {survey_uuid}")
                
                with page.expect_response(lambda res: "region-metadata" in res.url and res.status == 200) as response_info:
                    page.goto(collect_url, wait_until="networkidle")
                
                # 4. EKSTRAKSI METADATA & HEADERS (Tanpa Bearer)
                json_res = response_info.value.json()
                inner_data = json_res.get("data", {})
                
                if inner_data.get("id"):
                    auth_results["metadata"] = {
                        "groupId": inner_data.get("id"),
                        "region1Id": inner_data.get("region1Id"),
                        "region2Id": inner_data.get("region2Id")
                    }
                    print(f"✅ Metadata wilayah didapatkan.")

                # Ambil Cookies & Sinkronisasi Header
                all_cookies = context.cookies()
                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in all_cookies])
                
                auth_results["headers"].update({
                    "cookie": cookie_str,
                    "User-Agent": page.evaluate("navigator.userAgent"),
                    "Referer": collect_url,
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept": "application/json, text/plain, */*"
                })
                
                # Ekstrak XSRF-TOKEN dari cookie (Sangat penting untuk request POST/GET di Fasih)
                for c in all_cookies:
                    if c['name'].lower() == 'xsrf-token':
                        auth_results["headers"]["X-XSRF-TOKEN"] = c['value']

                print(f"🍪 Auth Selesai (Session & Metadata Secured)!")
                browser.close()
                return auth_results

            except Exception as e:
                print(f"⚠️ Gagal pada percobaan {attempt}: {e}")
                if attempt < max_retries:
                    print("🔄 Mengulang proses...")
                    time.sleep(3)
                else:
                    browser.close()
                    return auth_results

    return auth_results