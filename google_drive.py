import os
import pickle
from dotenv import dotenv_values
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Scope untuk akses file Drive yang dibuat oleh aplikasi ini
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def get_gdrive_service():
    path_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    env_vars = dotenv_values(path_env)
    client_secret_file = env_vars.get('GD_CLIENT_SECRETS_FILE', 'client_secrets.json')
    
    creds = None
    # token.pickle menyimpan token akses setelah login pertama kali
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
            
    # Jika tidak ada token valid, lakukan proses login
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Simpan token untuk penggunaan berikutnya
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds, cache_discovery=False)

def upload_to_drive(file_path):
    env_vars = dotenv_values(".env")
    folder_id = env_vars.get('GD_FOLDER_ID')

    try:
        service = get_gdrive_service()
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(file_path, mimetype='text/csv')
        
        print(f"⏳ Mengunggah sebagai User ke Drive...")
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()

        print(f"✅ Berhasil! File ID: {file.get('id')}")
        return True
    except Exception as e:
        print(f"❌ Error Upload: {e}")
        return False