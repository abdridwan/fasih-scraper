import config
from playwright.sync_api import sync_playwright

def auto_discovery_login(base_url, survey_uuid):
    print("🌐 Membuka browser untuk Identifikasi & Auth...")
    auth_results = {"headers": {}, "metadata": None}

    with sync_playwright() as p:
        # headless=True jika ingin berjalan di background tanpa muncul jendela browser
        browser = p.chromium.launch(headless=False) 
        context = browser.new_context()
        page = context.new_page()

        # --- FUNGSI INTERCEPTOR: MENANGKAP BEARER TOKEN ---
        def intercept_auth(request):
            if "/api/" in request.url:
                headers = request.headers
                if "authorization" in headers and "bearer" in headers["authorization"].lower():
                    auth_results["headers"]["Authorization"] = headers["authorization"]

        page.on("request", intercept_auth)

        try:
            print("🔗 Menghubungi pangkalan utama...")
            page.goto(f"{base_url.rstrip('/')}/", timeout=60000)
            
            # Proses Login SSO
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
            
            auth_results["headers"].update({
                "cookie": cookie_str,
                "User-Agent": page.evaluate("navigator.userAgent"),
                "Referer": collect_url,
                "Origin": base_url.rstrip('/'),
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest"
            })

            # Ambil XSRF-TOKEN untuk POST jika diperlukan
            for c in all_cookies:
                if c['name'].lower() == 'xsrf-token':
                    auth_results["headers"]["X-XSRF-TOKEN"] = c['value']
            
            page.wait_for_timeout(5000)
            print(f"🍪 Cookie & Token disinkronkan.")

        except Exception as e:
            print(f"❌ Kesalahan saat login: {e}")
        finally:
            browser.close()
            
    return auth_results