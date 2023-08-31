from google.oauth2 import service_account
import pandas as pd
import gspread
import json
import os

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']




def get_data_from_googlesheet(spreadsheet_id,key_files = 'key.json'):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds_with_scope = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    
    # If there are no (valid) credentials available, let the user log in.

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_file_path = os.path.join(script_dir, key_files)
        with open(data_file_path, "r") as json_file:
            service_account_info = json.load(json_file)
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        creds_with_scope = credentials.with_scopes(SCOPES)

        client = gspread.authorize(creds_with_scope)
        spreadsheet = client.open_by_url(f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid=0')

        index = 0
        flag = True
        dfs = []
        while flag:
            try:
                worksheet = spreadsheet.get_worksheet(index)
                records_data = worksheet.get_all_records()
                df = pd.DataFrame.from_dict(records_data)
                dfs.append(df)
                index +=1
            except:
                flag = False

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return None
        # Call the Sheets API
        
    except Exception as err:
        print(err)

