import psycopg2
import pandas as pd
import numpy as np
from datetime import date
import mysql.connector as mysql
import pyodbc
import csv

today = date.today()
date = today.strftime("%m.%d.%Y")

import sys
from pathlib import Path


## Append the the path to the custom modules i.e. CernDBConnector
sys.path.append("C://Users/nb044705/OneDrive - Cerner Corporation/DEVELOPMENT/github")


## Set variable to location of database.ini file
## https://vault.cerner.com/credential/read?credID=680228
CernDBConnector_INI = ("C:/Users/nb044705/OneDrive - Cerner Corporation/development/credentials/database.ini")
from CernDBConnector import config

SQLPath = ("C:/Users/NB044705/OneDrive - Cerner Corporation/development/github/VMData/")



## Obtain the db connection parameters and pass to Postgres
## module returning a connection to the database
def connectMYSQL(db,CernDBConnector_INI):
    params = config.config(db,CernDBConnector_INI)
    conn = mysql.connect(**params)
    return(conn)



## Pass in the database to connect to and the sql via a file handle
## return the queried data as a pandas dataframe object
def getMYDBData(db,sql):
    conn = None
    try:

        ## Open database connection
        conn = connectMYSQL(db,CernDBConnector_INI)

        ## Open and read the file as a single buffer
        sqlFile  = open(SQLPath + 'SQL/' + sql,'r')

        df = pd.read_sql_query(sqlFile.read(),conn)

        ## close db conn and sql file
        sqlFile.close()
        #cur.close()
        return(df)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')



## MSSQL DB Connection
## Obtain the db connection parameters and pass to MS SQL
## module returning a connection to the database

def connectMSSQL(db,CernDBConnector_INI):
        params = config.config(db,CernDBConnector_INI)
        conn = pyodbc.connect(**params)
        return(conn)
    ## Pass in the database to connect to and the sql via a file handle,## return the queried data as a pandas dataframe objectdef getMSDBData(db,sql,):
def getMSDBData(db,sql):
    conn = None
    try:        
        conn = connectMSSQL(db,CernDBConnector_INI)

        ## Open and read the file as a single buffer
        sqlFile  = open(SQLPath + 'SQL/' + sql,'r')

        df = pd.read_sql_query(sqlFile.read(),conn)

        ## close db conn and sql file
        sqlFile.close()
        #cur.close()
        return(df)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')



## Connect to the ClosedStack db and pull assets and related servers
#vm_data = getMYDBData('oci','array.sql')


host_data = getMYDBData('oci','host_array.sql')

host_data.head()

host_data['ESXiHost'] = host_data['ESXiHost'].str.lower()

## split cluster name to name and extra
new = host_data["ESXiHost"].str.split(".", n = 1, expand = True)
  ## Making separate cluster name column
host_data["ESXiHost"]= new[0]
  ## Making separate extra name column 
host_data["extra"]= new[1]

host_data = host_data[['ESXiHost','Array','Tier','AllocatedTB','UsedTB']]

cluster_data = getMSDBData('vcenter','vm_cluster.sql')

#cluster_data['Server'] = cluster_data['Server'].str.lower()

cluster_data['ESXiHost'] = cluster_data['ESXiHost'].str.lower()

## split cluster name to name and extra
new = cluster_data["ESXiHost"].str.split(".", n = 1, expand = True)
  ## Making separate cluster name column
cluster_data["ESXiHost"]= new[0]
  ## Making separate extra name column 
cluster_data["extra"]= new[1]

#cluster_data = cluster_data[['Cluster','ESXiHost']]


#cluster_array = pd.merge(cluster_data,vm_data,on='Server',how='left')


#cluster_array = cluster_array.groupby(['Cluster','ESXiHost'])['VMAllocatedTB'].sum().reset_index()



## split cluster name to name and extra
new = cluster_data["Cluster"].str.split("-", n = 1, expand = True)
  ## Making separate cluster name column
cluster_data["Cluster"]= new[0]
  ## Making separate extra name column 
cluster_data["extra"]= new[1]

#cluster_array['date_update'] = date

cluster_array = pd.merge(cluster_data,host_data,on='ESXiHost')

#cluster_array = cluster_array.groupby(['Cluster','extra','Array','Tier'])['AllocatedTB','UsedTB'].sum().reset_index()

cluster_array = cluster_array[['Cluster','extra','AllocatedTB','UsedTB','Array','Tier']]

cluster_array = cluster_array.drop_duplicates(subset=['Cluster','extra','AllocatedTB','UsedTB','Array','Tier'], keep='first')

cluster_array['date_update'] = date

cluster_array.to_csv(r'C:/Users/NB044705/OneDrive - Cerner Corporation/development/output/cluster_array.csv',index=False)


#host_data.to_csv(r'C:/Users/nb044705/OneDrive - Cerner Corporation/Desktop/host_data.csv',index=False)
## Obtain the db connection parameters and pass to Postgres
## module returning a connection to the database
def connect(db,CernDBConnector_INI):
    params = config.config(db,CernDBConnector_INI)
    conn = psycopg2.connect(**params)
    #return(conn)

    try:
        # read connection parameters
        print("in try1")
        #params = config.config(cmis_local)
        print("in try2")
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        #conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()


        #Empty table
        sql1 = """truncate vrt_cluster_array;"""
        cur.execute(sql1)

        #open file
        in_file = open('C:/Users/NB044705/OneDrive - Cerner Corporation/development/output/cluster_array.csv', mode="r")
        csvReader = csv.reader(in_file)

        SQL = """
            COPY %s FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS ','
            """
        def process_file(conn,table,file_object):
            cursor = conn.cursor()
            cursor.copy_expert(sql=SQL % table, file=file_object)
            conn.commit()
            cursor.close()


        #Load CMIS data
        process_file(conn, 'vrt_cluster_array',in_file)

    

        in_file.close()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

if __name__ == '__main__':
    connect('cmisadmin',CernDBConnector_INI)



print('complete')