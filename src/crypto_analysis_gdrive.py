import pandas as pd
import requests
import schedule
import time
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import os
import json

# Google Drive Setup
SCOPES = ['https://www.googleapis.com/auth/drive.file']
SERVICE_ACCOUNT_FILE = 'credentials.json'
# Update this with your new folder ID from step 2
FOLDER_ID = '15fCqiIeeo62SRHd8eLPLhtmHF6XJPuX_'  
FILE_ID = None

def setup_google_drive():
    """Set up Google Drive API service"""
    try:
        # Load credentials
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials)
        return service
    except Exception as e:
        print(f"Error setting up Google Drive: {e}")
        raise

def fetch_crypto_data():
    """Fetch top 50 cryptocurrencies data from CoinGecko API"""
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": False
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        df = pd.DataFrame(data)
        df = df[[
            'name', 'symbol', 'current_price', 'market_cap',
            'total_volume', 'price_change_percentage_24h'
        ]].rename(columns={
            'current_price': 'price_usd',
            'total_volume': 'volume_24h',
            'price_change_percentage_24h': 'price_change_24h'
        })
        
        return df
    
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def analyze_data(df):
    """Perform analysis on cryptocurrency data"""
    analysis = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'top_5_by_market_cap': df.head(5)[['name', 'market_cap']].to_dict('records'),
        'average_price': df['price_usd'].mean(),
        'highest_price_change': df.nlargest(1, 'price_change_24h')[['name', 'price_change_24h']].to_dict('records')[0],
        'lowest_price_change': df.nsmallest(1, 'price_change_24h')[['name', 'price_change_24h']].to_dict('records')[0]
    }
    return analysis

def update_excel_and_upload(df, analysis, drive_service):
    """Update Excel file and upload to Google Drive"""
    global FILE_ID
    
    try:
        # Use a timestamp in filename to avoid conflicts
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        local_file = f'crypto_data_{timestamp}.xlsx'
        
        print(f"Creating Excel file: {local_file}")
        
        # Create Excel file locally
        with pd.ExcelWriter(local_file) as writer:
            df.to_excel(writer, sheet_name='Live Data', index=False)
            
            analysis_df = pd.DataFrame([{
                'Timestamp': analysis['timestamp'],
                'Average Price (USD)': analysis['average_price'],
                'Highest 24h Change': f"{analysis['highest_price_change']['name']}: {analysis['highest_price_change']['price_change_24h']:.2f}%",
                'Lowest 24h Change': f"{analysis['lowest_price_change']['name']}: {analysis['lowest_price_change']['price_change_24h']:.2f}%"
            }])
            analysis_df.to_excel(writer, sheet_name='Analysis', index=False)
            
            top_5_df = pd.DataFrame(analysis['top_5_by_market_cap'])
            top_5_df.to_excel(writer, sheet_name='Top 5 by Market Cap', index=False)

        print("Excel file created successfully")

        # Upload to Google Drive
        file_metadata = {
            'name': 'Crypto_Analysis_Live.xlsx',
            'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'parents': [FOLDER_ID]
        }
        
        media = MediaFileUpload(local_file,
                              mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                              resumable=True)
        
        try:
            if FILE_ID:
                print(f"Updating existing file: {FILE_ID}")
                file = drive_service.files().update(
                    fileId=FILE_ID,
                    media_body=media).execute()
            else:
                print("Creating new file in Google Drive")
                file = drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id').execute()
                FILE_ID = file.get('id')
                
                # Set file permissions
                print("Setting file permissions")
                permission = {
                    'type': 'anyone',
                    'role': 'reader'
                }
                drive_service.permissions().create(
                    fileId=FILE_ID,
                    body=permission).execute()
            
            print(f"File {'updated' if FILE_ID else 'created'} successfully")
            
        except HttpError as error:
            print(f"Error uploading to Google Drive: {error}")
            raise
            
        finally:
            # Clean up local file with proper error handling
            try:
                if os.path.exists(local_file):
                    os.remove(local_file)
                    print("Local file cleaned up")
            except Exception as e:
                print(f"Warning: Could not delete local file: {e}")
        
        return FILE_ID
        
    except Exception as e:
        print(f"Error in update_excel_and_upload: {e}")
        raise

def get_sharing_link(file_id):
    """Generate sharing link for the file"""
    return f"https://drive.google.com/file/d/{file_id}/view"

def main():
    """Main function to run the crypto analyzer"""
    print("Crypto Analyzer Started")
    
    try:
        # Setup Google Drive service
        print("Setting up Google Drive service...")
        drive_service = setup_google_drive()
        print("Google Drive service setup complete")
        
        def job():
            try:
                print(f"\nUpdating data at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Fetch data
                df = fetch_crypto_data()
                if df is not None:
                    # Perform analysis
                    analysis = analyze_data(df)
                    
                    # Update Excel and upload to Google Drive
                    file_id = update_excel_and_upload(df, analysis, drive_service)
                    
                    print("Data updated successfully")
                    print(f"File link: {get_sharing_link(file_id)}")
                    
                    # Print some key insights
                    print("\nKey Insights:")
                    print(f"Average Price: ${analysis['average_price']:.2f}")
                    print(f"Highest 24h Change: {analysis['highest_price_change']['name']}: {analysis['highest_price_change']['price_change_24h']:.2f}%")
                    print(f"Lowest 24h Change: {analysis['lowest_price_change']['name']}: {analysis['lowest_price_change']['price_change_24h']:.2f}%")
            
            except Exception as e:
                print(f"Error in job: {e}")
        
        # Run job immediately
        job()
        
        # Schedule job to run every 5 minutes
        schedule.every(5).minutes.do(job)
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    except Exception as e:
        print(f"Fatal error in main: {e}")

if __name__ == "__main__":
    main()