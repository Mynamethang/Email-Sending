from datetime import datetime  
from datetime import timedelta
import time
import pyodbc
import os
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


mail_from_user = os.environ["mail_from_user"]
mail_from_pass = os.environ["mail_from_pass"]
mail_pvp = os.environ["mail_pvp"]
dwh_server = os.environ["dwh_server"]
dwh_db = os.environ["dwh_db"]
dwh_user = os.environ["dwh_user"]
dwh_pass = os.environ["dwh_pass"]

def send(mail_to_list, batch_id):

    server = dwh_server
    database = dwh_db 
    username = dwh_user
    password = dwh_pass
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

    # Queries
    sql = "select row_number() over (order by min(a.created_ts)) stt, a.job_id,b.job_name,b.job_group,b.job_sub_group,format(min(a.created_ts),'yyyy-MM-dd hh:mm:ss') [start],format(max(a.created_ts),'yyyy-MM-dd hh:mm:ss') [end],round(cast(datediff(ms,'1900-01-01',max(a.created_ts)- min(a.created_ts)) as float)/1000/60,2) duration from [ADF].[ETL_LOG] a join [ADF].[ETL_JOB] b on a.job_id = b.job_id  where [BATCH_ID]="+batch_id+" and a.job_id not in (select distinct job_id from (select job_id,LOG_STATUS,row_number() over(partition by  job_id order by created_ts desc) rn from  [ADF].[ETL_LOG] where [BATCH_ID] ="+batch_id+" and LOG_STATUS in ('ERROR')) a where LOG_STATUS in ('ERROR') and rn=1) group by a.job_id,b.job_name,b.job_group,b.job_sub_group order by min(a.created_ts)"
    df1 = pd.read_sql(sql, cnxn)
    sql = "select row_number() over (order by min(a.created_ts)) stt, a.job_id,b.job_name,b.job_group,b.job_sub_group,format(min(a.created_ts),'yyyy-MM-dd hh:mm:ss') [start],format(max(a.created_ts),'yyyy-MM-dd hh:mm:ss') [end],round(cast(datediff(ms,'1900-01-01',max(a.created_ts)- min(a.created_ts)) as float)/1000/60,2) duration,c.[error_message] from [ADF].[ETL_LOG] a join [ADF].[ETL_JOB] b on a.job_id = b.job_id join (select job_id ,max([error_message]) [error_message] from [ADF].[ETL_LOG] where [BATCH_ID] ="+batch_id+" and LOG_STATUS = 'ERROR' group by job_id) c on a.job_id = c.job_id  where [BATCH_ID]="+batch_id+" and a.job_id in (select distinct job_id from (select job_id,LOG_STATUS,row_number() over(partition by  job_id order by created_ts desc) rn from  [ADF].[ETL_LOG] where [BATCH_ID] ="+batch_id+" and LOG_STATUS in ('ERROR')) a where LOG_STATUS in ('ERROR') and rn=1) group by a.job_id,b.job_name,b.job_group,b.job_sub_group,c.[error_message] order by min(a.created_ts)"
    df2 = pd.read_sql(sql, cnxn)

    sql = "SELECT round(cast(datediff(ms,'1900-01-01',[END_TS]- [START_TS]) as float)/1000/60,2) duration FROM [ADF].[ETL_BATCH] where [BATCH_ID] ="+batch_id
    total_time = str(pd.read_sql(sql, cnxn).loc[0, 'duration'])

    # Subject and body of email
    Subject = '[SUCCESS] BI PROJECT - ETL Notification ' + str((datetime.utcnow() + timedelta(hours=5)).strftime('%Y-%m-%d'))
    bodyhtml = ""
    bodyhtml += "<html><head><style>table,th,td{border:1px solid black;border-collapse: collapse;}th,td{padding: 5px;text-align: left;    }   table {margin-left: 32px; }  th {background-color:None;text-align: center;    } .fl {background-color:#D80008;    } .sc {background-color:#00aa4e;    } </style></head><body>"
    bodyhtml += "Total Time: <b>" + total_time + " </b>"


    if len(df2) != 0:
        Subject = '[FAILED] BI PROJECT - ETL Notification ' + str((datetime.utcnow() + timedelta(hours=5)).strftime('%Y-%m-%d'))
        
        bodyhtml += "<br>"
        bodyhtml += "Total job fail: <b> " + str(len(df2)) + " </b><br><br>"
        bodyhtml += '<table><tr class="fl">    <th>STT</th>   <th>JOB NAME</th><th>JOB GROUP</th><th>JOB SUB GROUP</th><th>START</th><th>ERROR_MESSAGE</th></tr>'
        for i in range(len(df2)):
            bodyhtml += '<tr><td>'+str(df2.loc[i, 'stt'])+'</td><td>'+str(df2.loc[i, 'job_name'])+'</td><td>'+str(df2.loc[i, 'job_group'])+'</td><td>'+str(df2.loc[i, 'job_sub_group'])+'</td><td>'+str(df2.loc[i, 'start'])+'</td><td>'+str(df2.loc[i, 'error_message'])+'</td></tr>'
        bodyhtml +=   '</table><br>'
        
    bodyhtml += "<br>"
    bodyhtml += "Total job success: <b> " + str(len(df1)) + " </b><br><br>"
    bodyhtml += '<table><tr class="sc">    <th>STT</th>   <th>JOB NAME</th><th>JOB GROUP</th><th>JOB SUB GROUP</th><th>START</th><th>END</th><th>DURATION</th></tr>'
    for i in range(len(df1)):
        bodyhtml += '<tr><td>'+str(df1.loc[i, 'stt'])+'</td><td>'+str(df1.loc[i, 'job_name'])+'</td><td>'+str(df1.loc[i, 'job_group'])+'</td><td>'+str(df1.loc[i, 'job_sub_group'])+'</td><td>'+str(df1.loc[i, 'start'])+'</td><td>'+str(df1.loc[i, 'end'])+'</td><td>'+str(df1.loc[i, 'duration'])+'</td></tr>'
    bodyhtml +=   '</table><br>'
    
    bodyhtml += '</body></html>'
    
    # Gmail setup
    try:
        server = smtplib.SMTP('email.pvpower.vn', 587) # Set up the SMTP server
        server.starttls()
        server.login(mail_from_user, mail_from_pass)

        for recipient in mail_to_list:
            msg = MIMEMultipart()
            msg['From'] = mail_pvp  # Specify full email address as the sender
            msg['To'] = ', '.join(mail_to_list)
            msg['Subject'] = 'Email Test'
            msg.attach(MIMEText(bodyhtml, 'html'))
            server.sendmail(mail_pvp, recipient, msg.as_string())  # Use full email address here as well

        server.quit()  
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email. Error: {e}")

