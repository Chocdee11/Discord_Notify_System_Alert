import pymssql
import pandas as pd
import requests
from datetime import datetime
from discord_webhook import DiscordWebhook, DiscordEmbed
import time
import datetime
import math

# Connection details --------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------
# ปรับเปลี่ยนแค่ part นี้เท่านั้น
# หากมีการแยกเครื่องกันระหว่างเครื่อง Database และ flow ให้ใส่ตามลำดับที่กำหนดไว้โดยให้ VM Databse ขึ้นก่อน ในส่วนของ Flow จะใส่ 'server': 'localhost' เนื่องจากมีการต่่อแบบ local 

job_name = "job_name"
server = [
    {'server': 'server', 'user':'user', 'password':'password','database':'database'} ## VM Database
    {'server': 'localhost', 'user':'user', 'password':'password','database':'database'} ## VM flow
]

webhook_url = "webhook_url"
PRD_pic="https://drive.usercontent.google.com/download?id=19FD6gB1Sno2F2xRA_kJg2E3b3obkiI5W" # สามารถเปลี่ยนแปลงรูปภาพได้ หรือจะใช้รูปตามทำกำหนดก็ได้
UAT_pic = "https://drive.usercontent.google.com/download?id=1wXqOnn1032Ah7EoHIJsg-3VztP329zEm"

PIC = UAT_pic if "uat" in job_name.lower() else PRD_pic

# ---------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------
if "_" in job_name:
    task = job_name.split("_")[0]
else:
    task = job_name

conn_db = pymssql.connect(
    server[0]['server'], server[0]['user'], server[0]['password'], server[0]['database']
)

if len(server) == 2:
    conn_fw = pymssql.connect(
        server[1]['server'], server[1]['user'], server[1]['password'], server[1]['database']
    )
else:
    conn_fw = conn_db 

cursor_db = conn_db.cursor()
cursor_fw = conn_fw.cursor()

query_fw = f""" 
    with a as (
    select 
        instance_id,
        job_id,
        step_id,
        step_name,
        run_status,
        run_date,
        run_time,
        run_duration,
        ROW_NUMBER() over(partition by job_id, step_name order by run_date desc, run_time desc) as num_lasted
    from msdb.dbo.sysjobhistory
    ),

    run_date as (
    select 
        jn.name,
        a.step_id,
        ja.run_requested_date,
        ja.next_scheduled_run_date,
    	ja.run_requested_source
    from a 
    join msdb.dbo.sysjobs as jn
        on a.job_id = jn.job_id
    left join msdb.dbo.sysjobactivity as ja 
        on a.job_id = ja.job_id
        and a.instance_id = ja.job_history_id
	where num_lasted = '1'
    and jn.enabled = '1'
    and step_id = '0'
    ) 

    select distinct
        jn.name,
        a.instance_id,
        a.step_id,
        a.step_name,
        concat(SUBSTRING(cast(run_date as varchar),1,4) ,'-', SUBSTRING(cast(run_date as varchar),5,2) ,'-', SUBSTRING(cast(run_date as varchar),7,2), ' ' , 
        concat(SUBSTRING(cast(RIGHT(concat(replicate('0',6),run_time),6) as varchar),1,2) ,':', SUBSTRING(cast(RIGHT(concat(replicate('0',6),run_time),6) as varchar),3,2) ,':', SUBSTRING(cast(RIGHT(concat(replicate('0',6),run_time),6) as varchar),5,2))) as run_datetime,
        CASE
            WHEN a.run_duration > 235959
                THEN CAST((CAST(LEFT(CAST(a.run_duration AS VARCHAR),
                        LEN(CAST(a.run_duration AS VARCHAR)) - 4) AS INT) / 24) AS VARCHAR)
                            + '.' + RIGHT('00' + CAST(CAST(LEFT(CAST(a.run_duration AS VARCHAR),
                        LEN(CAST(a.run_duration AS VARCHAR)) - 4) AS INT) % 24 AS VARCHAR), 2)
                            + ':' + STUFF(CAST(RIGHT(CAST(a.run_duration AS VARCHAR), 4) AS VARCHAR(6)), 3, 0, ':')
            ELSE STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(a.run_duration AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':')
        END AS lastDuration,
        format(rd.run_requested_date,'yyyy-MM-dd HH:mm:ss') as run_requested_date,
        format(rd.next_scheduled_run_date,'yyyy-MM-dd HH:mm:ss') as next_scheduled_run_date,
        CASE rd.run_requested_source
            WHEN 0 THEN 'Unknown'
            WHEN 1 THEN 'Schedule'
            WHEN 2 THEN 'User (manual run)'
            WHEN 3 THEN 'Alert'
            WHEN 4 THEN 'External API / Sp_Start_Job'
            WHEN 5 THEN 'Restarted (internal)'
        ELSE 'Other'
    END AS run_source
    from a 
    join msdb.dbo.sysjobs as jn
        on a.job_id = jn.job_id
	join msdb.dbo.sysjobsteps as js
		on a.step_name = js.step_name
    left join msdb.dbo.sysjobactivity as ja 
        on a.job_id = ja.job_id
        and a.instance_id = ja.job_history_id
    left join run_date as rd 
        on jn.name = rd.name
	where num_lasted = '1'
    and jn.enabled = '1'
    and jn.name = '{job_name}'
    order by name, step_id 
    """

query_extractlog = f"""
    select 
        DataSource,
        UpdateAtETL,
        CountRow,
        case 
            when datediff(HOUR,UpdateAtETL,getdate()) in (1,0,-1) then 'success'
            else 'fail'
        end as status
    FROM [{server[0]['database']}].[dbo].[ExtractLog]
    where DataSource LIKE '%\_{task}%' ESCAPE '\\'
    order by DataSource;
"""

query_runtime = f"""
select 
	*,
	case 
		when status_freq_subday_type = 'At the specified time' then 'Daily'
		else concat('Every ',freq_subday_interval,' ',status_freq_subday_type,' ','Start : ',start_at)
	end as schedul_job_detail
from (
    select 
        c.job_id,
        c.name as job_name,
        c.enabled as job_status,
        a.schedule_id,
        a.next_run_date,
        a.next_run_time,
        CASE
            WHEN a.next_run_time > 235959
                THEN CAST((CAST(LEFT(CAST(a.next_run_time AS VARCHAR),
                    LEN(CAST(a.next_run_time AS VARCHAR)) - 4) AS INT) / 24) AS VARCHAR)
                        + '.' + RIGHT('00' + CAST(CAST(LEFT(CAST(a.next_run_time AS VARCHAR),
                    LEN(CAST(a.next_run_time AS VARCHAR)) - 4) AS INT) % 24 AS VARCHAR), 2)
                        + ':' + STUFF(CAST(RIGHT(CAST(a.next_run_time AS VARCHAR), 4) AS VARCHAR(6)), 3, 0, ':')
            ELSE SUBSTRING(STUFF(STUFF(RIGHT(REPLICATE('0', 6) + CAST(a.next_run_time AS VARCHAR(6)), 6), 3, 0, ':'), 6, 0, ':'),1,5)
        END AS NextRuntime,
        b.name as schedul_name,
        b.enabled,
		case 
			when b.freq_type = '1' then 'One time only'
			when b.freq_type = '4' then 'Daily'
			when b.freq_type = '8' then 'Weekly'
			when b.freq_type = '16' then 'Monthly'
			when b.freq_type = '32' then 'Monthly, relative to freq_interval'
			when b.freq_type = '64' then 'Runs when the SQL Server Agent service starts'
			when b.freq_type = '128' then 'Runs when the computer is idle'
		End status_freq_type,
        b.freq_interval,
		case
			when b.freq_subday_type = '1' then 'At the specified time'
			when b.freq_subday_type = '2' then 'Seconds'
			when b.freq_subday_type = '4' then 'Minutes'
			when b.freq_subday_type = '8' then 'Hours'
		end status_freq_subday_type,
        b.freq_subday_interval,
		SUBSTRING(b.name,1,CHARINDEX(' ',b.name)) as start_at
    from msdb.dbo.sysjobschedules as a
    join msdb.dbo.sysschedules as b
        on a.schedule_id = b.schedule_id
    join msdb.dbo.sysjobs as c
        on a.job_id = c.job_id
    where c.enabled = '1'
    and c.name = '{job_name}'
) as b
order by next_run_time                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
"""

# get job activity data
cursor_fw.execute(query_fw)
rows_fw = cursor_fw.fetchall()
# get extract log data
cursor_db.execute(query_extractlog)
rows_db = cursor_db.fetchall()
# get runtime data
cursor_fw.execute(query_runtime)
rows_runtime = cursor_fw.fetchall()

conn_db.close()
conn_fw.close()

# ---------------------------------------------------------------------------------------------------
rt = [a[6]for a in rows_runtime]
status_runtime = [a[14] for a in rows_runtime]
etl_runtime = " • " + " • ".join(rt)

run_time = rows_fw[0][4].split()[1]
next_time = rows_fw[0][7].split()[1]
source_run = [i[8] for i in rows_fw]

# ---------------------------------------------------------------------------------------------------
# Convert the results to DataFrames
extractlog = pd.DataFrame(rows_db)
job_history = pd.DataFrame(rows_fw)

# Header DataFrame
extractlog.columns = ['DataSource', 'UpdateAtETL', 'CountRow', 'status']
job_history.columns = ['name', 'instance_id', 'step_id', 'step_name', 'run_datetime', 'lastDuration', 'run_requested_date', 'next_scheduled_run_date','run_requested_source']

# right join two DataFrames
merged = pd.merge(extractlog, job_history, how='right', left_on='DataSource', right_on='step_name')
merged_list = merged.values.tolist()

print(merged_list)
# ---------------------------------------------------------------------------------------------------
divider = "-----------------------------------------------"
title_message = f"\n📚 **Job Name :** {job_name} \n" + f"📆 **Runtime :** {run_time[0:5]} \n"  + f"⏩ **Next Runtime :** {next_time[0:5]} \n" + f"⌛ **Duration_time :** {rows_fw[0][5]} \n{divider}\nStatus : {status_runtime[0]}\nUser : {source_run[0]}\nETL Runtime : {etl_runtime}\n{divider} "

row_success = [f for f in merged_list if f[3] == 'success']
row_fail = [f for f in merged_list if f[3] == 'fail']

message_success = ""
message_fail = ""

## Message Alert status
if len(row_success) != 0:
    for i in merged_list:
        if i[0] is not None and i[7].startswith(('API_','ETL_','dm_')) :
            DataSource= i[0]
            UpdateAtETL = i[1]
            CountRow = int(i[2])
            status = "🟢" if i[3] == "success" else "🟡"
            job_name = i[4]
            step_id = i[6]
            step_name = i[7]
            start_time = i[8]
            LastRunDuration = i[9]
            run_requested_date = i[10]
            next_scheduled_run_date = i[11]

            message = ( 
                f"\n🍎**StepTable** : [{step_id}] {DataSource}\n"
                f"🔹**UpdateETL** : {UpdateAtETL} \n"
                f"🔹**CountRow** : {CountRow} row\n"
                f"🔹**Duration_time** : {LastRunDuration} \n"
                f"🔹**Status** : 🟢 \n"
            )
            
            message_success += message

    webhook = DiscordWebhook(url=webhook_url,content = divider + title_message)
    embed = DiscordEmbed(description= message_success, color='57e560')
    embed.set_timestamp()
    embed.set_thumbnail(url=PIC)
    embed.set_footer("🐕‍🦺🏠🌳")
    webhook.add_embed(embed)

    response = webhook.execute()
    print(response.status_code) 

if len(row_fail) != 0:
    for i in merged_list:
        if i[0] is not None and i[7].startswith(('API_','ETL_','dm_')) :
            DataSource= i[0]
            UpdateAtETL = i[1]
            CountRow = int(i[2])
            status = "🟢" if i[3] == "success" else "🟡"
            job_name = i[4]
            step_id = i[6]
            step_name = i[7]
            start_time = i[8]
            LastRunDuration = i[9]
            run_requested_date = i[10]
            next_scheduled_run_date = i[11]

            message = ( 
                f"\n🍎**StepTable** : [{step_id}] {DataSource}\n"
                f"🔹**UpdateETL** : {UpdateAtETL} \n"
                f"🔹**CountRow** : {CountRow} row\n"
                f"🔹**Duration_time** : {LastRunDuration} \n"
                f"🔹**Status** : 🟢 \n"
            )

            message_fail += message

    webhook = DiscordWebhook(url=webhook_url,content = divider + title_message)     
    embed = DiscordEmbed(title="Fail",description= message_fail , color='f0fe66')
    embed.set_timestamp()
    embed.set_thumbnail(url=PIC)
    embed.set_footer("🐕‍🦺🏠🌳")
    webhook.add_embed(embed)

    response = webhook.execute()
    print(response.status_code) 
