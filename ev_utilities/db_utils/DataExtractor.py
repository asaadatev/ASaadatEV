#!/usr/bin/env python
# coding: utf-8

# In[159]:


import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime as dt
import json
from sqlalchemy import create_engine
import re


# In[195]:


def con_create(db='master'):
    
    ## Login ##

    ROOT_PATH = sys.path[0][:sys.path[0].rindex('\\')]

    RESOURCES_PATH = os.path.join(ROOT_PATH, r'resources')

    login_file_path = os.path.join(RESOURCES_PATH, r'login.json')

    with open(login_file_path, 'r') as json_file:
        login_details = json.load(json_file)

    ## Create Engine Using SQLAlchemy ##
    
    connection_string = f"mssql+pyodbc://{login_details['uid']}:{login_details['pwd']}@{login_details['server']}/{db}?driver=SQL+Server"

    engine = create_engine(connection_string)
    
    return engine


# In[196]:


def get_db(customer_account='XFAB'):
    get_dbs_query = ("select name "
                           +"from master.sys.databases "
                           +"where name like '%scada_production%'")
    customer_data_dbs = pd.read_sql_query(get_dbs_query, con_create(db='master').connect())
    customer_account_db = customer_data_dbs[customer_data_dbs.name.str.contains(customer_account)]
    customer_account_db = list(customer_account_db.name)
    if len(customer_account_db) == 1:
        return customer_account_db[0]
    elif len(customer_account_db) > 1:
        print(f'There are more than 1 customer account databases matching the customer account `{customer_account}` passed to the get_db function. Namely: \n')
        print(*customer_account_db, sep='\n')
        print(f'\nChoosing the first element; i.e. \'{customer_account_db[0]}\'.')
        print(f'\nIf this is not what you want, please recall the function and pass the name of the desired customer account from the list displayed above.')
        return customer_account_db[0]
    else:
        raise ValueError(f'There are no databases whose name contains the substring {customer_account}')



def get_system_data(customer_account, 
                    systems=None, 
                    sys_type=None,
                    save=False,
                    save_loc=None):
    
    db = get_db(customer_account=customer_account)
    
    db_con = con_create(db=db)
    
    all_systems_data = {}
    
    if sys_type != None:
        if isinstance(sys_type, (list, tuple, set)):
            str_type = re.sub(r'\]|\}',
                       ')',
                       (re.sub(r'\[|\{', 
                               '(', 
                               str(sys_type))))

            type_cond = f"and t3.SystemTypeID in {str_type}"

        else:    
            type_cond = f"and t3.SystemTypeID = {sys_type}"
    else:
        type_cond = ''
        
    if save:
        if save_loc == None:
            input_loc = input('\nPlease enter a location to save the data to parquet files.')
            if not os.path.exists(input_loc):
                print('\nNo such directory exists. Please try again.\n')
                return get_system_data(customer_account, 
                                       systems=systems,
                                       sys_type=sys_type,
                                       save=save,
                                       save_loc=save_loc)
            else:
                save_loc = input_loc
        else:
            pass
    
    if systems!=None:
    
        for idx in range(len(systems)):

            system = systems[idx]

            system_data_query= (f'SELECT t3.[Description], t4.[zzDescription], t1.[ParameterId], t1.[LogTime], t1.[Value] \
                                 FROM [{db}].[dbo].[fst_GEN_ParameterValue] AS t1 \
                                 INNER JOIN [{db}].[dbo].[fst_GEN_Parameter] AS t2 \
                                     ON t1.[ParameterId] = t2.[ParameterID] \
                                 INNER JOIN [{db}].[dbo].[fst_GEN_System] AS t3 \
                                     ON t2.[SystemID] = t3.[SystemID] \
                                 INNER JOIN [{db}].[dbo].fst_GEN_ParameterType AS t4 \
                                     ON t2.[SystemTypeID] = t4.[SystemTypeID] \
                                     AND t2.[ParameterNumber] = t4.[ParameterNumber] \
                                 WHERE t3.[Description] = \'{str(system)}\' \
                                 {type_cond}\
                                 ORDER BY t1.[LogTime]')
            
            num_remaining_systems = len(systems) - (idx+1)
            
            system_data = pd.read_sql(system_data_query, 
                                      db_con.connect())

            if system_data is not None and len(system_data) > 0:
                all_systems_data[system] = system_data
                print(f'\nRetrieved data for {system}. {num_remaining_systems} systems remain.\n')
                if save:
                    file_path = os.path.join(save_loc, f"{system}.parquet")
                    all_systems_data[system].to_parquet(file_path, compression=None)
                    
            else:
                print(f'\nThere is no data available for {system}. {num_remaining_systems} systems remain.\n')
            
            
    else:
        systems_query = (f"SELECT Description \
                         FROM [{db}].[dbo].[fst_GEN_System] AS t3 \
                         {type_cond.replace('and', 'where')}")
#         if sys_type != None:
#             str_idx=get_parameters_query.index('order by')
#             get_parameters_query = get_parameters_query[:str_idx] + f"and a.SystemTypeID = {sys_type}" + get_parameters_query[str_idx:]

        systems = pd.read_sql(systems_query,
                              db_con.connect())
        systems = systems.Description.to_list()
        
        if len(systems) > 0:
            print(f'Will attempt to retrive data for the following {len(systems)} systems:')
            print(*systems, sep='\n')
            return get_system_data(customer_account=customer_account,
                                   systems=systems,
                                   sys_type=sys_type,
                                   save=save,
                                   save_loc=save_loc)
        else:
            raise ValueError(f'No systems with the type(s) {sys_type} for {customer_account}.')
    
    return all_systems_data

