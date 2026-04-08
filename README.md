# 🚀 Fasih Scraper (BPS Kab. Sampang)

Alat otomatisasi berbasis Python untuk scraping data **Fasih BPS** secara multi-threaded dengan fitur upload otomatis ke Google Drive.

-----

## 🛠️ Persiapan & Instalasi

1.  **Clone & Instal:**

    ```bash
    git clone https://github.com/abdridwan/fasih-scraper.git
    cd fasih-scraper
    python -m pip install -r requirements.txt
    ```

2.  **Konfigurasi Environment:**
    Salin `.env.example` menjadi `.env` dan isi kredensial SSO BPS Anda:

    ```env
    SSO_USERNAME=username_anda
    SSO_PASSWORD=password_anda
    SELECTED_KAB_ID=kode_kab_bps
    ```

-----

## 🚀 Cara Penggunaan

### 1\. Mode CLI (Otomasi & Task Scheduler)

Sangat disarankan untuk penggunaan rutin atau terjadwal.

  * **Interaktif (Menu):** `python main.py`
  * **Langsung (Otomatis):** `python main.py [NAMA_SURVEI]`
  * **Kirim ke Cloud:** Tambahkan `--upload` di akhir perintah.

### 2\. Mode GUI (Tampilan Visual)

Cocok untuk manajemen survei yang lebih mudah tanpa terminal.

```bash
python gui_main.py
```

-----

## ☁️ Integrasi Google Drive (Opsional)

1.  Dapatkan file `client_secrets.json` (Tipe: **Desktop App**) melalui [Google Cloud Console](https://console.cloud.google.com/).
2.  Simpan file di folder utama proyek.
3.  Tambahkan konfigurasi di `.env`:
    ```env
    GD_CLIENT_SECRETS_FILE=client_secrets.json
    GD_FOLDER_ID=id_folder_tujuan
    ```
4.  Jalankan skrip dengan `--upload`. Browser akan terbuka **sekali saja** untuk login. Token akses akan disimpan permanen di `token.pickle`.

-----

## ⚙️ Kustomisasi Survei

Gunakan menu **Tambah/Edit** pada **GUI** atau edit file `surveys.json` secara manual untuk mengatur `period_id`, `uuid`, dan mapping kolom API.

-----

> **Author:** [abdridwan](https://github.com/abdridwan)  
> **Last Update:** April 2026