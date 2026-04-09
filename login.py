import config
import time
from playwright.sync_api import sync_playwright

def auto_discovery_login(base_url, survey_uuid, max_retries=3):
    auth_results = {"headers": {}, "metadata": None}
    
    # Timeout ditingkatkan ke 60-90 detik untuk koneksi tidak stabil
    TIMEOUT_MS = 90000 

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(TIMEOUT_MS) # Set default timeout global

        def intercept_auth(request):
            if "/api/" in request.url:
                headers = request.headers
                if "authorization" in headers and "bearer" in headers["authorization"].lower():
                    auth_results["headers"]["Authorization"] = headers["authorization"]

        page.on("request", intercept_auth)

        for attempt in range(1, max_retries + 1):
            try:
                print(f"🌐 Percobaan {attempt}/{max_retries}: Menghubungi halaman utama...")
                
                # Gunakan wait_until="commit" agar tidak terlalu lama menunggu semua iklan/asset luar
                page.goto(f"{base_url.rstrip('/')}/", wait_until="commit", timeout=TIMEOUT_MS)
                
                # Tunggu tombol login muncul (Jika down, ini akan timeout)
                print("⏳ Menunggu tombol SSO...")
                btn_selector = 'a[href*="/oauth2/authorization/ics"]'
                page.wait_for_selector(btn_selector, state="visible", timeout=30000)
                page.click(btn_selector)

                # Isi Kredensial
                print("🔑 Mengisi kredensial SSO...")
                page.wait_for_selector('input[name="username"]', timeout=20000)
                page.fill('input[name="username"]', config.USERNAME)
                page.fill('input[name="password"]', config.PASSWORD)
                
                # Klik Login & Tunggu Navigasi Berhasil
                with page.expect_navigation(timeout=TIMEOUT_MS):
                    page.click('input#kc-login')

                print(f"📡 Mengidentifikasi metadata wilayah...")
                collect_url = f"{base_url.rstrip('/')}/survey-collection/collect/{survey_uuid}"
                
                # Navigasi ke koleksi dengan timeout lebih longgar
                with page.expect_response(lambda res: "region-metadata" in res.url and res.status == 200, timeout=TIMEOUT_MS) as response_info:
                    page.goto(collect_url, wait_until="networkidle")
                    
                json_res = response_info.value.json()
                inner_data = json_res.get("data", {})
                
                if inner_data.get("id"):
                    auth_results["metadata"] = {
                        "groupId": inner_data.get("id"),
                        "region1Id": inner_data.get("region1Id"),
                        "region2Id": inner_data.get("region2Id")
                    }
                    print(f"✅ Metadata Berhasil Didapat.")

                # Ambil Cookie & XSRF
                all_cookies = context.cookies()
                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in all_cookies])
                auth_results["headers"].update({
                    "cookie": cookie_str,
                    "User-Agent": page.evaluate("navigator.userAgent"),
                    "Referer": collect_url
                })
                
                for c in all_cookies:
                    if c['name'].lower() == 'xsrf-token':
                        auth_results["headers"]["X-XSRF-TOKEN"] = c['value']

                print(f"🍪 Auth Selesai!")
                browser.close()
                return auth_results # Keluar dari loop jika sukses

            except Exception as e:
                print(f"⚠️ Percobaan {attempt} gagal: {e}")
                if attempt < max_retries:
                    print("🔄 Reloading & Mencoba lagi dalam 5 detik...")
                    time.sleep(5)
                else:
                    print("❌ Semua percobaan gagal.")
                    browser.close()
                    return auth_results

    return auth_results