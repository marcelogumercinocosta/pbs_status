import os
import glob
import pandas as pd
import warnings
import numpy as np
warnings.filterwarnings('ignore')

allFiles = glob.glob(os.path.join("../../data_raw/","*"))

df_logs_full = pd.DataFrame()
for file in allFiles:
    df = pd.read_table(file, delimiter="\n",index_col=None, header=0, error_bad_lines=False, names=['logs'],)
    df_logs_full = pd.concat([df_logs_full, df])

df_logs = df_logs_full[df_logs_full['logs'].str.contains(';Job;', regex=False)]
df_logs = df_logs['logs'].str.split(pat=";", expand=True)
df_logs.columns = ['date','job_id','pbs_status','key','job_name','status']
df_logs = df_logs[df_logs['status'].str.contains('Resource_List|Confirming ALPS reservation|Started, pid|Terminated', regex=True)]
df_logs = df_logs.replace(r'^Resource_List.*','place', regex=True)
df_logs = df_logs.replace(r'^Confirming ALPS reservation.*','confirmed', regex=True)
df_logs = df_logs.replace(r'^Started, pid.*','started', regex=True)
df_logs = df_logs.replace(r'^Terminated.*','terminated', regex=True)
df_logs['date']=pd.to_datetime(df_logs['date'] , format='%m/%d/%Y %H:%M:%S')
df_logs_filtrados = df_logs[['job_name','status','date']]
df_logs_status = df_logs_filtrados.pivot(index='job_name', columns='status', values='date')
df_logs_status['time_allocation'] = (df_logs_status['started'] - df_logs_status['confirmed']).dt.seconds/60
df_logs_status['time_run'] = (df_logs_status['terminated'] - df_logs_status['started']).dt.seconds/60
df_logs_status['run'] = (df_logs_status['terminated'] - df_logs_status['started']).dt.seconds/60

df_id_user_name = df_logs_full[df_logs_full['logs'].str.contains(r'<.*user_name', regex=True)]
df_id_user_name = df_id_user_name.replace( r'^.*user_name="|" batch_id="|">$',";", regex=True).replace(r'^;|;$',"", regex=True)
df_id_user_name = df_id_user_name['logs'].str.split(pat=";", expand=True)
df_id_user_arc = df_logs_full[df_logs_full['logs'].str.contains(r'<.*architecture', regex=True)]
df_id_user_arc = df_id_user_arc.replace({'logs': r'^.*<ReserveParam architecture="|" width="|" depth="|" nppn="|">$' }, {'logs': ";"}, regex=True).replace(r'^;|;$',"", regex=True)
df_id_user_arc = df_id_user_arc['logs'].str.split(pat=";", expand=True)
df_id_user_arc.index = df_id_user_arc.index -1
df_id = df_id_user_name.join(df_id_user_arc, lsuffix='_user_name', rsuffix='_arc')
df_id.columns = ['user_name','job_name','architecture','width','depth','nppn']
# Altera o Nome do User para Git
nomes_antivos = df_id['user_name'].unique()
nomes_novos = [f"user{x}" for x in list(range(0,len(nomes_antivos)))]
df_id['user_name'] = df_id['user_name'].replace(nomes_antivos, nomes_novos)

df_id_user_name = df_logs_full[df_logs_full['logs'].str.contains(r'<.*user_name', regex=True)]
df_id_user_name = df_id_user_name.replace( r'^.*user_name="|" batch_id="|">$',";", regex=True).replace(r'^;|;$',"", regex=True)
df_id_user_name = df_id_user_name['logs'].str.split(pat=";", expand=True)
df_id_user_arc = df_logs_full[df_logs_full['logs'].str.contains(r'<.*architecture', regex=True)]
df_id_user_arc = df_id_user_arc.replace({'logs': r'^.*<ReserveParam architecture="|" width="|" depth="|" nppn="|">$' }, {'logs': ";"}, regex=True).replace(r'^;|;$',"", regex=True)
df_id_user_arc = df_id_user_arc['logs'].str.split(pat=";", expand=True)
df_id_user_arc.index = df_id_user_arc.index -1
df_id = df_id_user_name.join(df_id_user_arc, lsuffix='_user_name', rsuffix='_arc')
df_id.columns = ['user_name','job_name','architecture','width','depth','nppn']
# Altera o Nome do User para Git
nomes_antivos = df_id['user_name'].unique()
nomes_novos = [f"user{x}" for x in list(range(0,len(nomes_antivos)))]
df_id['user_name'] = df_id['user_name'].replace(nomes_antivos, nomes_novos)

df_final = df_id.set_index('job_name').join(df_logs_status)
df_final.to_csv('../../data/teste.csv', sep=';', encoding='utf-8')