# gdrive_handler.py
import io
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import os
import pickle
import config

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def get_gdrive_service():
    """Authenticate and return Google Drive service"""
    creds = None
    
    if os.path.exists(config.TOKEN_FILE):
        with open(config.TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                config.CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open(config.TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)
    
    return build('drive', 'v3', credentials=creds)

def download_csv_from_gdrive(service, file_id):
    """Download CSV file from Google Drive"""
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
    
    fh.seek(0)
    return pd.read_csv(fh)

def list_files_in_folder(service, folder_id):
    """List all files in a Google Drive folder"""
    results = service.files().list(
        q=f"'{folder_id}' in parents",
        fields="files(id, name, mimeType)"
    ).execute()
    
    return results.get('files', [])

def get_transactions_file_id(service):
    """Get the file ID for transactions.csv"""
    files = list_files_in_folder(service, config.GDRIVE_FOLDER_ID)
    
    for file in files:
        if file['name'].lower() == 'transactions.csv':
            return file['id']
    
    return None