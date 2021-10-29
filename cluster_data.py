import psycopg2 
import pandas as pd
import numpy as np
import pyodbc
import sys
from pathlib import Path
import xlsxwriter
from datetime import date

## Append the the path to the custom modules i.e. CernDBConnector
sys.path.append("C://Users/nb044705/OneDrive - Cerner Corporation/DEVELOPMENT/github")

from CapAPI_Toolkit import capacity

## Set variable to location of database.ini file
## https://vault.cerner.com/credential/read?credID=680228
CernDBConnector_INI = ("C:/Users/nb044705/OneDrive - Cerner Corporation/development/credentials/database.ini")
from CernDBConnector import config

SQLPath = ("C:/Users/NB044705/OneDrive - Cerner Corporation/development/github/VMData/")


## MSSQL DB Connection
## Obtain the db connection parameters and pass to MS SQL
## module returning a connection to the database
def connectMSSQL(db,CernDBConnector_INI):
    params = config.config(db,CernDBConnector_INI)
    conn = pyodbc.connect(**params)
    return(conn)


## Pass in the database to connect to and the sql via a file handle
## return the queried data as a pandas dataframe object
def getMSDBData(db,sql):
    conn = None
    try:

        ## Open database connection
        conn = connectMSSQL(db,CernDBConnector_INI)

        ## Open and read the file as a single buffer
        sqlFile  = open(SQLPath + 'SQL/' + sql,'r')

        df = pd.read_sql_query(sqlFile.read(),conn)

        ## close db conn and sql file
        sqlFile.close()
        #cur.close()
        return(df)

    except (Exception, pyodbc.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

##################
## vCenter data ##
##################

## get vm data from vcenter
cluster_data = getMSDBData('vcenter','cluster_data.sql')

## drop duplicate vm's if they exist
cluster_data.drop_duplicates(subset ="VMUuid",keep = False, inplace = True) 

## count vm's in each cluster
vm_count = cluster_data.groupby(['Cluster'])['VMUuid'].count().reset_index()
vm_count.rename(columns = {'VMUuid' : 'vm_count'}, inplace = True)

## group cluster and hardware models
host_count = cluster_data.groupby(['Cluster'])['FullName'].nunique().reset_index()
cluster = cluster_data.groupby(['vcenter','Cluster','EVC']).size().reset_index()
cluster_summary = pd.merge(cluster,host_count, on='Cluster', how='left')
cluster_summary.drop(0, axis=1, inplace=True)
cluster_summary.rename(columns = {'FullName' : 'host_count'}, inplace = True)
cluster_summary = pd.merge(cluster_summary,vm_count, on='Cluster', how='left')

## Grouo and sort on proc models and memory size to rempve duplicate rows
model_proc = cluster_data.groupby(['Cluster','Model','CPUModel','MemorySize']).size().reset_index()
model_proc = model_proc.sort_values(['CPUModel','MemorySize'],ascending = (False,False))
model_proc = model_proc.drop_duplicates(subset = 'Cluster')
model_proc.drop(0, axis=1, inplace=True)

## Create the cluster_audit df
clusters = cluster_data.groupby(['Cluster','Model','CPUModel','MemorySize','FullName']).size().reset_index()
clusters = clusters.groupby(['Cluster','Model','CPUModel','MemorySize'])['FullName'].count().reset_index()
clusters.rename(columns = {'FullName' : 'Count'}, inplace = True)
cluster_audit = clusters.groupby(['Cluster'])['Model'].count().reset_index()
cluster_audit.rename(columns = {'Model' : 'cluster_audit'}, inplace = True)
clusters.head()

## Merge the df's
cluster_count = pd.merge(cluster_summary,model_proc, on='Cluster', how='left')
cluster_count = pd.merge(cluster_count,cluster_audit, on='Cluster', how='left')

## split cluster name to name and extra
new = cluster_count["Cluster"].str.split("-", n = 1, expand = True)
  ## Making separate cluster name column
cluster_count["Cluster"]= new[0]
  ## Making separate extra name column 
cluster_count["extra"]= new[1]

## Remove Clusters not in scope
cluster_count = cluster_count[cluster_count['Cluster']!='Templates']
cluster_count = cluster_count[cluster_count['Cluster']!='KC2']
cluster_count = cluster_count[cluster_count['Cluster']!='Compute']
cluster_count = cluster_count[cluster_count['Cluster']!='NSX']
cluster_count = cluster_count[cluster_count['Cluster']!='Maint temp']
cluster_count = cluster_count[cluster_count['Cluster']!='Maintenance101']
cluster_count = cluster_count[cluster_count['Cluster']!='templates']
cluster_count = cluster_count[cluster_count['Cluster']!='VIO']
cluster_count = cluster_count[cluster_count['Cluster']!='Maintenance102']
cluster_count = cluster_count[cluster_count['Cluster']!='Maintenance02']
cluster_count = cluster_count[cluster_count['Cluster']!='LSSSA2']
cluster_count = cluster_count.sort_values(['Cluster'],ascending = (True))

cluster_count = cluster_count[['vcenter','Cluster','cluster_audit','extra','Model','CPUModel','MemorySize','host_count','vm_count','EVC']]


###############
## TSCO Data ##
###############

## Defined function
def rapidConnect(db,CernDBConnector_INI):
    keys = config.config(db,CernDBConnector_INI)
    return(keys)

## access fuction to obtain keys
keys = rapidConnect('TSCO',CernDBConnector_INI)


## Obtain access to specific API (https://wiki.cerner.com/display/grid/Truesight+Capacity+Optimization+API+Data+Catalog?src=breadcrumbs-parent)
def getClusterCapacity():
    response = capacity.getData(2160,'PROD',keys)
    return response


## Call the functions that access the CapAPI_Toolkit.Capacity module and create dataframe of return values
raw = getClusterCapacity()

## Create data df to work on without changing the original
data = raw

## Function to convert units 

def humanbytes_u(B,units):
   'Return the given bytes as a human friendly KB, MB, GB, or TB string'
   B = float(B)
   KB = float(1024)
   MB = float(KB ** 2) # 1,048,576
   GB = float(KB ** 3) # 1,073,741,824
   TB = float(KB ** 4) # 1,099,511,627,776

   if units == 'B':
      return '{0}'.format(B)
   elif units == 'KB':
      return '{0:.2f}'.format(B/KB)
   elif units == 'MB':
      return '{0:.2f}'.format(B/MB)
   elif units == 'GB':
      return '{0:.2f}'.format(B/GB)
   elif units == 'TB':
      return '{0:.2f}'.format(B/TB)
    
## Function to format units    
def format_units(df,col,units):
    for i in df.index:
        val = df.get_value(i,col)
        formatval = humanbytes_u(val,units)
        #print(formatval)
        df.set_value(i,col,formatval)

data = data.replace('', np.NaN)


data['MEM_CAPACITY_ALLOCABLE_C'] = data['MEM_CAPACITY_ALLOCABLE_C'].astype(str).astype(np.float64)
data['STG_CAPACITY_ALLOCABLE_C'] = data['STG_CAPACITY_ALLOCABLE_C'].astype(str).astype(np.float64)
data['VCPU_DENSITY'] = data['VCPU_DENSITY'].astype(str).astype(np.float64)


format_units(data,'MEM_CAPACITY_ALLOCABLE_C','GB')
format_units(data,'STG_CAPACITY_ALLOCABLE_C','GB')

## split cluster name to name and extra
new = data["CL_NAME"].str.split("-", n = 1, expand = True)
  ## Making separate cluster name column
data["Cluster"]= new[0]
  ## Making separate extra name column 
data["extra"]= new[1]

## Remove excess columns from the tsco data
tsco_data = data[['FUNCTIONALITY','Cluster','MEM_CAPACITY_ALLOCABLE_C','STG_CAPACITY_ALLOCABLE_C','VCPU_DENSITY','DATACENTER']]
tsco_data.rename(columns = {'DATACENTER' : 'dc'}, inplace = True)

## Merge vcenter and tsco data
cluster_counts = pd.merge(cluster_count,tsco_data, on='Cluster', how='right')

## Create a new column for the location by parsing the first two characters from the cluster name 
cluster_counts['location'] = cluster_counts['Cluster'].map(lambda x: x[0:2])

def loc(x):
    if x == 'KC':
        return "KC"
    if x == 'LS':
        return "LS"
    else:
    
        return "other"

cluster_counts['Location'] = cluster_counts['location'].apply(lambda x: loc(x))


## Rearrange columns in the cluster_count df
cluster_counts = cluster_counts[['Location','vcenter','Cluster','extra','cluster_audit','FUNCTIONALITY','Model','CPUModel','MemorySize','host_count','vm_count','STG_CAPACITY_ALLOCABLE_C','MEM_CAPACITY_ALLOCABLE_C','VCPU_DENSITY','EVC','dc']]

## Calculate the extra hosts in each cluster
cluster_counts['extra_hosts'] = cluster_counts['MEM_CAPACITY_ALLOCABLE_C']/cluster_counts['MemorySize']

## Round down the extra host qty
cluster_counts['extra_hosts'] = cluster_counts['extra_hosts'].apply(np.floor)

## Get Cluster FZ and OS Tags

cluster_tags = getMSDBData('vcenter','cluster_tags.sql')
newcluster = cluster_tags["Cluster"].str.split("-", n = 1, expand = True)
  ## Making separate cluster name column
cluster_tags["Cluster"]= newcluster[0]
  ## Making separate extra name column 
cluster_tags["extra2"]= newcluster[1]

cluster_tags.head()

cluster_data = pd.merge(cluster_counts, cluster_tags, on='Cluster', how='left')

today = date.today()
date = today.strftime("%Y-%m-%d")

cluster_data['date_update'] = date

cluster_data = cluster_data[['Location','vcenter','Cluster','extra','cluster_audit','FUNCTIONALITY','Model','CPUModel','MemorySize','host_count','vm_count','STG_CAPACITY_ALLOCABLE_C','MEM_CAPACITY_ALLOCABLE_C','VCPU_DENSITY','EVC','extra_hosts','Linux','Windows','FZ1','FZ2','date_update','dc']]


## Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('C:/Users/nb044705/Cerner Corporation/SSE IPA Capacity Management - misc/cluster_counts.xlsx', engine='xlsxwriter')

## Write each dataframe to a different worksheet
cluster_data.to_excel(writer, sheet_name='cluster_data',index=False)
cluster_count.to_excel(writer, sheet_name='cluster_audit',index=False)
data.to_excel(writer, sheet_name='tsco_data',index=False)

## Close the Pandas Excel writer and output the Excel file.
writer.save()


cluster_tags.to_csv(r'C:/Users/nb044705/OneDrive - Cerner Corporation/Desktop/cluster_tags.csv',index=False)

#raw.to_csv(r'C:/Users/nb044705/OneDrive - Cerner Corporation/Desktop/tsco_raw.csv',index=False)

print('complete')

