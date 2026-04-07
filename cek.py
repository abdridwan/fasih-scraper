import pandas as pd

# 1. Tentukan nama file
csv_file = '20260405_0732_PBI_JKN.csv'
xlsx_file = 'Data Prelist Full 3527.xlsx'
output_file = 'Hasil_Identifikasi_ID_Hilang.xlsx'

try:
    # 2. Baca file CSV
    # Menggunakan sep=None dan engine='python' agar pandas otomatis mendeteksi pemisah (koma atau titik koma)
    df_csv = pd.read_csv(csv_file, sep=None, engine='python')
    
    # 3. Baca file Excel (Sheet1)
    df_xlsx = pd.read_excel(xlsx_file, sheet_name='Sheet1')

    # Pastikan kolom 'id' ada di kedua file
    if 'id' in df_csv.columns and 'id' in df_xlsx.columns:
        
        # 4. Ambil semua ID unik dari CSV untuk mempercepat pencarian
        ids_di_csv = set(df_csv['id'].astype(str).unique())

        # 5. Filter baris di Excel yang 'id'-nya TIDAK ada di dalam set ids_di_csv
        # .astype(str) digunakan untuk memastikan perbandingan tipe data konsisten
        df_diff = df_xlsx[~df_xlsx['id'].astype(str).isin(ids_di_csv)]

        # 6. Simpan hasil ke file Excel baru
        if not df_diff.empty:
            df_diff.to_excel(output_file, index=False)
            print(f"Berhasil! Ditemukan {len(df_diff)} ID yang tidak ada di CSV.")
            print(f"File telah disimpan sebagai: {output_file}")
        else:
            print("Semua ID di file Excel ternyata sudah ada di file CSV.")
            
    else:
        print("Error: Kolom 'id' tidak ditemukan di salah satu file.")

except Exception as e:
    print(f"Terjadi kesalahan: {e}")