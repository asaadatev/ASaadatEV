import os
import sys
import pandas as pd
import numpy as np
import datetime
from datetime import datetime as dt
import json
import pyodbc
from sqlalchemy import create_engine
import re

def con_create(RESOURCES_PATH, db='master'):
    
    ## Login ##

    login_file_path = os.path.join(RESOURCES_PATH, r'login.json')

    with open(login_file_path, 'r') as json_file:
        login_details = json.load(json_file)

    ## Create Engine Using SQLAlchemy ##
    
    connection_string = f"mssql+pyodbc://{login_details['uid']}:{login_details['pwd']}@{login_details['server']}/{db}?driver=SQL+Server"

    engine = create_engine(connection_string)
    
    return engine

def get_avail(db, save=False, ROOT_PATH=None, RESOURCES_PATH=None, AVAIL_DIR_SUBPATH=r'Availability'):
    db_con = con_create(RESOURCES_PATH=RESOURCES_PATH)

    old_avail_query = f"""
    SELECT 
	t3.[Description] [System], 
    MIN(t1.[LogTime]) [FirstLogTime],
    MAX(t1.[LogTime]) [LastLogTime],
    CAST(DATEDIFF(day, MIN(t1.[LogTime]), MAX(t1.[LogTime])) AS FLOAT) [TotalDays],
    count(DISTINCT CONVERT(DATE, t1.LogTime)) [AvailDays],
    CAST(count(DISTINCT CONVERT(DATE, t1.LogTime))/CAST(DATEDIFF(day, MIN(t1.[LogTime]), MAX(t1.[LogTime])) AS FLOAT) AS FLOAT(4)) [AvailRate],
	STUFF((
		SELECT DISTINCT '|' + zzDescription
		FROM [{db}].[dbo].fst_GEN_ParameterType temp
		WHERE temp.zzDescription = zzDescription
		FOR XML PATH (''))
		, 1, 1, '') [Parameters]
    FROM [{db}].[dbo].[fst_GEN_ParameterValue] AS t1  
    INNER JOIN [{db}].[dbo].[fst_GEN_Parameter] AS t2  
        ON t1.[ParameterId] = t2.[ParameterID]  
    INNER JOIN [{db}].[dbo].[fst_GEN_System] AS t3  
        ON t2.[SystemID] = t3.[SystemID]  
    INNER JOIN [{db}].[dbo].fst_GEN_ParameterType AS t4  
        ON t2.[SystemTypeID] = t4.[SystemTypeID]  
        AND t2.[ParameterNumber] = t4.[ParameterNumber]  
    GROUP BY t3.Description
    ORDER BY t3.Description ASC
    """

    avail_query = f"""
    WITH query AS (
    SELECT 
        t3.[Description] [System],
        MIN(t1.[LogTime]) [FirstLogTime],
        MAX(t1.[LogTime]) [LastLogTime],
        CAST(DATEDIFF(day, MIN(t1.[LogTime]), MAX(t1.[LogTime])) AS FLOAT) [TotalDays],
        count(DISTINCT CONVERT(DATE, t1.LogTime)) [AvailDays]
    FROM [scada_Production_XFAB_France].[dbo].[fst_GEN_ParameterValue] AS t1  
    INNER JOIN [scada_Production_XFAB_France].[dbo].[fst_GEN_Parameter] AS t2  
        ON t1.[ParameterId] = t2.[ParameterID]  
    INNER JOIN [scada_Production_XFAB_France].[dbo].[fst_GEN_System] AS t3  
        ON t2.[SystemID] = t3.[SystemID]  
    INNER JOIN [scada_Production_XFAB_France].[dbo].fst_GEN_ParameterType AS t4  
        ON t2.[SystemTypeID] = t4.[SystemTypeID]
        AND t2.[ParameterNumber] = t4.[ParameterNumber]
    GROUP BY t3.Description)

    SELECT *,
        CASE
            WHEN [TotalDays] > 0 THEN [AvailDays]/[TotalDays]
            ELSE 0
        END AS [AvailRate]
    FROM query
    ORDER BY [System]
    """

    customer = db[17:]

    availability = pd.read_sql(avail_query, db_con.connect())
    
    if save:
    
        AVAIL_DIR_PATH = os.path.join(ROOT_PATH, AVAIL_DIR_SUBPATH)

        while not os.path.exists(AVAIL_DIR_PATH):
            os.makedirs(AVAIL_DIR_PATH)
        
        XLSX_PATH = os.path.join(AVAIL_DIR_PATH, fr'{db}_data_availability.xlsx')

        with pd.ExcelWriter(path=XLSX_PATH, engine='xlsxwriter', datetime_format='YYYY-MM-DD HH:MM:SS') as writer:
            sheet_name = f'{customer}DataAvailability'
            availability.to_excel(writer, sheet_name=sheet_name, index=False)

        assert os.path.exists(XLSX_PATH)

    return availability
