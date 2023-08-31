import yaml
from db_util.mysql import MySQLConnector
from db_util.clickhouse import ClickHouseConnector 
import pandas as pd
import argparse
from datetime import date
from datetime import timedelta
from datetime import datetime
import os
from api_sheet_utils import get_data_from_googlesheet

def init_param():
    parser = argparse.ArgumentParser(description ='FBAds_tool')
 
    parser.add_argument('-s', '--start', dest ='start',
                        help ='target date start')
    parser.add_argument('-e', '--end', dest ='end',
                        help ='target date end')
    
    args = parser.parse_args()
    return args


script_dir = os.path.dirname(os.path.abspath(__file__))
data_file_path = os.path.join(script_dir, 'config.yaml')

def read_config_from_file(filename: str = "./config.yaml"):
    try:
        with open(filename, "r") as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        print(f"Error reading configuration: {str(e)}") 

def main():
    start = datetime.now()
    args = init_param()
    today = date.today()
    start_date = today - timedelta(days = 1) if (args.start is None ) or (args.start == '' ) else args.start
    end_date = start_date if (args.end is None ) or (args.end == '' ) else args.end
    print(f"Update data from {start_date} to {end_date}")
    start_date = str(start_date)
    end_date = str(end_date)
    target_date = end_date
    
    start_date =  datetime.strptime(start_date, '%Y-%m-%d')
    end_date =  datetime.strptime(end_date, '%Y-%m-%d')
    config = read_config_from_file(data_file_path)
    mysql = MySQLConnector(**config['mysql'])
    spreadsheet_id = config['spreadsheet_id']


    mysql.delete_data_by_condition(table=config['table']['metric'],condition_dict={"report_date":start_date})

    data_ads = get_data_from_googlesheet(spreadsheet_id=spreadsheet_id)
    data_ads['report_date']=pd.to_datetime(data_ads['report_date'],format='%d/%m/%Y')
    data_ads = data_ads[data_ads['report_date'] >= start_date]  
    data_ads = data_ads[(data_ads['report_date'] <= end_date)]
    data_ads['report_date'] = data_ads['report_date'].astype(str)
    data_ads = data_ads[data_ads['click'] != '']

    query = f"SELECT report_date,source,metric,dim1,num1 FROM metric_report WHERE  metric = 't1_n1' AND report_date between '{start_date}' and '{end_date}' ;"
    data_n1 = mysql.query_data_to_dataframe(query)
    data_n1 = data_n1[['report_date','dim1','num1']]
    data_n1.columns = ['report_date','agent_line','nru']
    data_n1['report_date'] = data_n1['report_date'].astype(str)

    result = data_ads.merge(data_n1,on=['report_date','agent_line'])
    mysql.insert_dataframe(config['table']['metric'],result,igorne=True)


    #update num recharge
    try:
        file_excel = os.path.join(script_dir,'recharge_file', f'{target_date}.xlsx')
        data_recharge = pd.read_excel(file_excel)
        data_recharge.columns = ['user','time','agent','agent_line','num_recharge']

        data_recharge['report_date']=pd.to_datetime(data_recharge['time'],format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
        data_recharge = data_recharge[['report_date','agent_line','num_recharge']]
        #get all user have >=1 recharge
        data_recharge = data_recharge[data_recharge['num_recharge'] > 0]
        data_recharge = data_recharge.groupby(['report_date','agent_line']).count().reset_index()
        mysql.update_data(dataframe=data_recharge,table_name=config['table']['metric'],update_cols=['num_recharge'],condition_cols=['report_date','agent_line'])
    except Exception as ex:
        print('An exception occurred ',ex)
    



    end = datetime.now()
    print(f"Done, time run: {end-start}")


if __name__ == "__main__":
    
    main()