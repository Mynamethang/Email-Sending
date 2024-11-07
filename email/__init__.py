import logging
import azure.functions as func
import json
import os
from .script.fun import send
import pypyodbc as pyodbc
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # dwh_server = os.environ["dwh_server"]
    # dwh_db = os.environ["dwh_db"]
    # dwh_user = os.environ["dwh_user"]
    # dwh_pass = os.environ["dwh_pass"]

    # server = dwh_server
    # database = dwh_db 
    # username = dwh_user
    # password = dwh_pass
    # cnxn = pyodbc.conn{"mail_to_list":"['thang.nguyen@tcdata.vn']","batch_id":"241029001"}ect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

    # sql="select max(batch_id) batch_id  from [ADF].[ETL_BATCH]"
    # df1 = pd.read_sql(sql, cnxn)
    mail_to_list = ['thang.nguyen@tcdata.vn', 'nguyen.le@tcdata.vn']
    #batch_id = str(df1.loc[0,'batch_id'])
    batch_id = '241029001'
    try:
        req_body = req.get_json()
    except ValueError:
        pass
    else:
        mail_to_list = eval(req_body.get('mail_to_list'))
        batch_id = str(req_body.get('batch_id'))
    
    send(mail_to_list,batch_id)
    

    return func.HttpResponse(
             "This HTTP triggered function executed successfully.",
             status_code=200
        )
