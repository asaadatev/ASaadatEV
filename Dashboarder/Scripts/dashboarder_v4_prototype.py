#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
import pandas as pd
import numpy as np
import datetime
from datetime import datetime as dt
import json
import pyodbc
import re
import types
from termcolor import colored
import types
import re
import os
from bokeh import plotting
from bokeh import layouts
from bokeh import io
from bokeh import models
from bokeh.models.tools import HoverTool
from bokeh.models.widgets.markups import Div
from math import ceil
import math
from sympy.ntheory import primefactors
import statsmodels as sm        
import copy
import warnings
from ev_utilities import DataAvailability
import time


# In[3]:


## Login ##

ROOT_PATH = sys.path[0][:sys.path[0].rindex('\\')]

RESOURCES_PATH = os.path.join(ROOT_PATH, r'Resources')

login_file_path = os.path.join(RESOURCES_PATH, r'login.json')

with open(login_file_path, 'r') as json_file:
    login_details = json.load(json_file)

## Obtain list of all databases with relevant information ##

master_db = 'master'

master_db_connection = pyodbc.connect('Driver={SQL Server};'
                                      'Server=' + login_details['server'] + ';'
                                      'Database=' + master_db + ';'
                                      'Uid=' + login_details['uid'] + ';'
                                      'Pwd=' + login_details['pwd'] + ';')

get_databases_query = ("select name \
                        from master.sys.databases \
                        where name like '%scada_production%'")

warnings.filterwarnings('ignore')
customer_data_databases = pd.read_sql_query(get_databases_query, master_db_connection)
warnings.resetwarnings()


# In[5]:


## Prompt user for the customer/database of interest ##

confirmation = 'N'
while confirmation != 'Y':
    print(customer_data_databases.iloc[1:])
    choices = customer_data_databases.iloc[1:].index.to_list()
    user_choice = input('\nUsing the the table displayed, please enter the number corresponding to the customer database of interest:\n')
    try:
        user_choice = int(user_choice)
    except:
        pass
    while user_choice not in choices:
        print(customer_data_databases.iloc[1:])
        user_choice = input('\nInvalid choice. Please choose a number associated with one of the databases listed below: \n')
        try:
            user_choice = int(user_choice)
        except:
            pass
    confirmation = input(f'You have chosen "{customer_data_databases.iloc[user_choice][0]}". Is this correct (Y/N)? ').upper()
database = customer_data_databases.iloc[user_choice][0]

MAIN_FOLDER_PATH = fr"{input('Please enter the path of the directory within which you wish the data and figures to be saved: ')}".replace('"', '').replace("'",'')
while not os.path.exists(MAIN_FOLDER_PATH):
    print('\n')
    MAIN_FOLDER_PATH = fr"{input('The entered path does not exist. Please enter a valid path to continue: ')}"
date = datetime.datetime.today().strftime('%Y-%m-%d')
customer_account_name = database[17:]
MAIN_FOLDER_PATH = os.path.join(MAIN_FOLDER_PATH, fr'{customer_account_name}')
if not os.path.exists(MAIN_FOLDER_PATH):
    os.makedirs(MAIN_FOLDER_PATH)
    assert os.path.exists(MAIN_FOLDER_PATH)
print(f'\nAll files will be placed at: \n"{MAIN_FOLDER_PATH}".')

## Retrieve Data Availability for the Customer Account ##
    
availability = DataAvailability.get_avail(db = database, 
                                          save=True, 
                                          ROOT_PATH=MAIN_FOLDER_PATH, 
                                          RESOURCES_PATH=RESOURCES_PATH)

print(f'\n\nData availability results saved within "{MAIN_FOLDER_PATH}\Availability"\n\n')

time.sleep(5)

## Get inputs function ##

def get_input(prompt, success_conditions=None, failure_messages=None, message=None, wrapping_func=None, literal_str=False):

    if wrapping_func == None:
        wrapping_func = lambda x: x
    
    if message is not None:
        print(message)
    
    if success_conditions == None:
#         print('input1')
        if literal_str==True:
#             print('input2')
            return fr"{input(prompt)}"
        else:
#             print('input3')
            return wrapping_func(input(prompt))
    else:
        k=1
#         print('input4')
        x = fr"{input(prompt)}"
        
        x = x.replace('"', '')
        
        num_conditions_to_satisfy = len(success_conditions)

        success_count = 0

        while success_count < num_conditions_to_satisfy:
#             print('input5')
        
            for idx in range(num_conditions_to_satisfy):
#                 print('input6')
                # print(f'Success condition {idx} is {success_conditions[idx](x)} because x is {x} and its type is {type(x)}')
                if isinstance(success_conditions[idx], types.FunctionType):
#                     print('input7')
                    if success_conditions[idx](x):
#                         print('input8')
                        success_count+=1
                    else:
#                         print('input9')
                        print(failure_messages)
                        if failure_messages is not None and len(failure_messages) > 1:
#                             print('input10')
                            print(failure_messages[idx].format(x, type(x), f'Condition {idx+1} failure'))
                            return get_input(prompt, 
                                            success_conditions=success_conditions, 
                                            failure_messages=failure_messages, 
                                            message=message,
                                            wrapping_func=wrapping_func)
                        elif failure_messages is not None and len(failure_messages) == 1:
#                             print('input11')
                            print(failure_messages[0].format(x))
                            return get_input(prompt, 
                                             success_conditions=success_conditions, 
                                             failure_messages=failure_messages, 
                                             message=message,
                                             wrapping_func=wrapping_func)
                
                else:
#                     print('input12')
                    if success_conditions[idx]:
                        success_count+=1
                    else:
#                         print('input13')
                        if len(failure_messages) > 1:
#                             print('input14')
                            print(failure_messages[idx].format(x, type(x), f'Condition {idx+1} failure'))
                            return get_input(prompt, 
                                            success_conditions=success_conditions, 
                                            failure_messages=failure_messages, 
                                            message=message,
                                            wrapping_func=wrapping_func)
                        else:
#                             print('input15')
                            print(failure_messages[0])
                            return get_input(prompt, 
                                             success_conditions=success_conditions, 
                                             failure_messages=failure_messages, 
                                             message=message,
                                            wrapping_func=wrapping_func)
        print(f'Thank you for choosing {wrapping_func(x)}')
        return wrapping_func(x)

## Establish connection with the chosen customer data connection ##

db_connection = pyodbc.connect('Driver={SQL Server};'
                                'Server=' + login_details['server'] + ';'
                                 'Database=' + database + ';'
                                 'Uid=' + login_details['uid'] + ';'
                                 'Pwd=' + login_details['pwd'] + ';')

## Define a function that checks whether a string can be converted to an integetr or not ##

def isintable(x):
    try:
        x = int(x)
        return True
    except:
        False

## Prompt user for whether they want to obtain system data based on system types, system names or both ##

type_or_name_or_both = get_input(prompt=f'Please choose whether you wish to proceed by \n1. System Name; \n2. System Type; \n3. Both.\nAlternatively, if you wish to retrieve data for all systems within {database}, please enter 4.\n',
                                 success_conditions=(isintable, lambda x: int(x) in (1,2,3,4)),
                                 failure_messages=('{2}: {0} is not a valid choice. Please choose from 1, 2, 3 and 4 because {0} cannot be converted into an integer.',
                                                   '{2}: {0} is not a valid choice. Please choose from 1, 2, 3 and 4 because {0} is not in (1,2,3,4).'),
                                 message=None,
                                 wrapping_func=int)

if type_or_name_or_both in (1,3):
    systems_by_file = get_input(prompt='Would you like to \n0. Enter the system names here (enter 0); or \n1. Pass a file path containing the system names (enter 1)\n',
                                success_conditions=(isintable, lambda x: int(x) in (0,1)),
                                failure_messages=('{2}: {0} is not a valid choice as it cannot be converted into an integer. Please choose from 0 or 1.',
                                                  '{2}: {0} is noT a valid choice as it is neither 0 nor 1. Please choose 0 or 1.'),
                                wrapping_func=int)

else:
    systems_by_file = 0

get_system_names_bool_args = {'by_names':bool(type_or_name_or_both==1 or type_or_name_or_both==3),
                              'by_types':bool(type_or_name_or_both==2 or type_or_name_or_both==3),
                              'systems_by_file':bool(systems_by_file==1),
                              'everything':bool(type_or_name_or_both==4)}


## Define a function for retrieving system names form a comma separated text file ##

def systems_from_csv(PATH):
    
    systems = []
    
    SYSTEMS_FILE_PATH = PATH

    with open(SYSTEMS_FILE_PATH, 'r') as systems_file:
        for line in systems_file:
            line_systems = re.split(',|, | ,| , ', line)
            systems = systems.copy() + line_systems.copy()

    for i in range(len(systems)):
        if '\'' in systems[i] or '\"' in systems[i]:
            systems[i] = systems[i].replace('\'', '')
            systems[i] = systems[i].replace('\"', '')
    
    return systems

## Prompt user for systems/system types/both and ensure at least one corresponding system exists ##

def get_system_names(db_connection, database, by_names=False, by_types=False, systems_by_file=False, everything=False):
    if by_names:
        if not systems_by_file:
            systems = get_input(prompt='Please enter the names of the systems for which you wish to retrieve data, separated by commas.\n')
            systems = re.split(',| ,|, | , ', systems)
            systems_as_string = str(systems).replace('[','(').replace(']',')')
            

            if by_types:
                system_types = get_input(prompt='Please enter the types of the systems for which you wish to retrieve data, separated by commas.\n')
                system_types = re.split(',| ,| , ', system_types)
                system_types_as_string = str(system_types).replace('[', '(').replace(']', ')')
                
                check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                            where Description in {systems_as_string} \
                                            and SystemTypeID in {system_types_as_string} \
                                            order by SystemTypeID'
            
            else:
                check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                            where Description in {systems_as_string} \
                                            order by SystemTypeID'

        else:
            systems_path = get_input(prompt='Please enter the file location for a comma separated file containing the names of the systems of interest:\n',
                                     success_conditions=[os.path.exists],
                                     failure_messages=['The path you have entered, namely {}, does not exist. Please try again.\n'])
            systems = systems_from_csv(systems_path)
            systems_as_string = str(systems).replace('[','(').replace(']',')')


            if by_types:
                system_types = get_input(prompt='Please enter the types of the systems for which you wish to retrieve data, separated by commas.\n')
                system_types = re.split(',| ,| , ', system_types)
                system_types_as_string = str(system_types).replace('[','(').replace(']',')')
                check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                                where Description in {systems_as_string} \
                                                and SystemTypeID in {system_types_as_string} \
                                                order by SystemTypeID'
            else:
                check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                            where Description in {systems_as_string} \
                                            order by SystemTypeID'
        warnings.filterwarnings('ignore')
        query_result = pd.read_sql_query(check_system_name_query, db_connection)
        warnings.resetwarnings()
        valid_systems = list(query_result['Description'].unique())
        invalid_systems = list(set(systems) - set(valid_systems))
        if len(invalid_systems) > 1:
            print('checking invalid systems')
            if len(valid_systems) < 1:
                print('yo13')
                print(f'Could not find any systems within {database} corresponding to the system names and types (if any entered) provided. Please try again.')
                return get_system_names(db_connection=db_connection, 
                                        database=database, 
                                        by_names=by_names, 
                                        by_types=by_types, 
                                        systems_by_file=systems_by_file,
                                        everything=everything)
            else:
                print(f'Could not find any systems within {database} corresponding to the entered system types (if any were entered) and following system names: {invalid_systems}.')
                print(f'Valid system names: {valid_systems}.\nWill retrieve data for these systems.')
                return query_result, valid_systems
        else:
            print(f'Found systems corresponding to all of the entered names within the {database} database; namely: \n{valid_systems}\nWill retrieve data for these systems.')
            return query_result, valid_systems

    elif by_types:
        system_types = get_input(prompt='Please enter the types of the systems for which you wish to retrieve data, separated by commas.\n')
        system_types = re.split(',| ,| , ', system_types)
        system_types_as_string = str(system_types).replace('[','(').replace(']',')')
        check_system_name_query = f'select * from {database}.dbo.fst_GEN_system \
                                    where SystemTypeID in {system_types_as_string} \
                                    order by SystemTypeID'
        
        warnings.filterwarnings('ignore')
        query_result = pd.read_sql_query(check_system_name_query, db_connection)
        warnings.resetwarnings()
        
        if len(query_result) > 0:
            type_systems = query_result[['Description', 'SystemTypeID']]
            print(f'The following systems were found to correspond with system types {system_types}:')
            print(type_systems)
            print('Will retrieve data for these systems.')
            return query_result, list(query_result.Description.unique())
        
        else:
            print('yo19')
            print(f'Could not find any systems within {database} whose system type IDs correspond with any of {system_types}. Please try again.')
            return get_system_names(db_connection=db_connection, 
                                    database=database, 
                                    by_names=by_names, 
                                    by_types=by_types, 
                                    systems_by_file=systems_by_file,
                                    everything=everything)
    elif everything:
        check_system_name_query = f'select * from {database}.dbo.fst_GEN_System \
                                    order by SystemTypeID'
        warnings.filterwarnings('ignore')
        query_result = pd.read_sql_query(check_system_name_query, db_connection)
        warnings.resetwarnings()
        print(f'{len(query_result)} systems were found within {database}, including:')
        print(query_result[['Description','SystemTypeID']])
        print('Will retrieve data for these systems.')
        return query_result, list(query_result.Description.unique())

systems_info, systems = get_system_names(db_connection=db_connection,
                                         database=database,
                                         **get_system_names_bool_args)


# In[6]:


## Prompt user for whether they want to obtain data separated by swaps, unseparated by swaps or both

separate_by_swap = False
sep_and_whole = False

separate_by_swap_or_not = get_input(prompt=f'Would you like the system data to be: \n1. Separated by swaps (enter 1); \n2. Unseparated by swaps (enter 2); \n3. Plots for the entire data as well as the data separated by swaps (enter 3).\n',
                                    success_conditions=(isintable, lambda x: int(x) in (1,2,3)),
                                    failure_messages=('{2}: {0} is not a valid choice as it cannot be converted into an integer. Please choose from 1, 2 or 3.',
                                                      '{2}: {0} is noT a valid choice as it is not in (1,2,3). Please choose 1, 2 or 3.'),
                                    wrapping_func=int)

if separate_by_swap_or_not in (1,3):
    separate_by_swap = True
    if separate_by_swap_or_not == 3:
        sep_and_whole = True
elif separate_by_swap_or_not == 2:
    pass
else:
    raise ValueError(f'Invalid value of \'{separate_by_swap_or_not}\' of type {type(separate_by_swap_or_not)} passed to `separate_by_swap_or_not`.')


# In[7]:


# Prompt user for whether they want the parameter definitions to be concealed

def conceal_or_not():
    conceal_param_defs = get_input(prompt='Would you like the parameter definitions to be concealed? (Y/N):\n',
                               success_conditions=[lambda x: isinstance(x, str), lambda x: x.upper() in ('Y', 'N')],
                               failure_messages=['The value entered is not a string. Please enter \'Y\' or \'N\'.',
                                                 'The value entered is neither \'Y\' nor \'N\'. Please enter a valid input.'])
    if conceal_param_defs.upper() == 'Y':
        conceal_param_defs = True
    elif conceal_param_defs.upper() == 'N':
        conceal_param_defs = False
    else:
        print(f'Invalid entry {conceal_param_defs} provided. Please try again')
        return conceal_or_not()
    
    return conceal_param_defs

conceal_params = conceal_or_not()


# In[8]:


# Prompt the user for whether they want all parameter data or predefined views

def plot_all_data_or_view():
    view_or_all = get_input(prompt='Would you like to see: \n1) all of the parameter data for these systems; or \n2) just predefined views?\n',
                            success_conditions=[isintable, lambda x: int(x) in (1,2)],
                            failure_messages=('{2}: {0} is not a valid choice as it cannot be converted into an integer. Please enter 1 or 2.',
                                              '{2}: {0} is not in (1,2). Please enter 1 or 2.'),
                            wrapping_func=int)
    if view_or_all == 1:
        view_or_all = 'all'
    elif view_or_all == 2:
        view_or_all = 'view'
    else:
        print(f'Invalid entry {view_or_all} provided. Please try again')
        return plot_all_data_or_view()
    
    return view_or_all

dash_view_or_all = plot_all_data_or_view()


# In[9]:


## Obtain parameter information ##

def get_parameters(systems, database, db_connection):

    systems_parameter_info = {}
        
    for idx in range(len(systems)):

        sys = systems[idx]

        get_parameters_query = (f"select DISTINCT a.SystemID, a.SystemTypeID, a.Description [SystemName], a.LastAlertLogTime, c.ParameterNumber, c.zzDescription,  c.SIUnitID\
                                from {database}..fst_GEN_System a \
                                join [{database}].[dbo].[fst_GEN_Parameter] b \
                                    on a.SystemTypeID = b.SystemTypeID \
                                join [{database}].[dbo].[fst_GEN_ParameterType] c \
                                    on b.SystemTypeID = c.SystemTypeID \
                                    and b.ParameterNumber = c.ParameterNumber \
                                where a.Description = \'{sys}\' \
                                order by a.SystemTypeID, a.Description")
        warnings.filterwarnings('ignore')
        parameter_information = pd.read_sql_query(get_parameters_query, db_connection)
        warnings.resetwarnings()
        
        systems_parameter_info[sys] = parameter_information
        
    return systems_parameter_info

## Create a parameter mapping dictionary for all systems ##

systems_parameter_info = get_parameters(systems=systems, database=database, db_connection=db_connection)

systems_parameter_info['param_mapping'] = {}
for system in systems:
    zipped = list(zip(systems_parameter_info[system]['ParameterNumber'], systems_parameter_info[system]['zzDescription']))
    systems_parameter_info['param_mapping'][system] = dict(zipped)

systems_parameter_info['param_mapping']
systems_to_check = list(systems_parameter_info['param_mapping'].keys())
for key in systems_to_check:
    if len(systems_parameter_info['param_mapping'][key]) == 0:
        print(f'Removing \'{key}\' from the list of systems for which data will be retrieved because it has no parameter informaiton.')
        systems_parameter_info['param_mapping'].pop(key)

### Partition Parameters by the Associated Mechanical Part ###

systems = list(systems_parameter_info['param_mapping'].keys())
params_dict_partitioned = {system:{} for system in systems}
for system in systems:
    # print(systems_parameter_info['param_mapping'].keys())
    # print(system)
    # print(system in list(systems_parameter_info['param_mapping'].keys()))
    params_sys = systems_parameter_info['param_mapping'][system]
    params_sys_dict = {'DryPump':{},
                       'Booster':{},
                       'ExhaustAndShaft':{},
                       'Flow':{},
                       'RunTime':{},
                       'Oil':{},
                       'Others':{}}
    for sys in params_sys.keys():
        params_sys[sys] = params_sys[sys].replace(' ', '').replace('DP', 'DryPump').replace('MB', 'Booster')
        if 'DP' in params_sys[sys] or 'Dry' in params_sys[sys]:
            params_sys_dict['DryPump'][sys]=params_sys[sys]

        elif 'Booster' in params_sys[sys] or 'MB' in params_sys[sys]:
            params_sys_dict['Booster'][sys]=params_sys[sys]

        elif 'Exhaust' in params_sys[sys] or 'Shaft' in params_sys[sys]:
            params_sys_dict['ExhaustAndShaft'][sys]=params_sys[sys]

        elif 'Time' in params_sys[sys] or 'Hours' in params_sys[sys]:
            params_sys_dict['RunTime'][sys]=params_sys[sys]

        elif 'Oil' in params_sys[sys]:
            params_sys_dict['Oil'][sys]=params_sys[sys]
            
        elif 'Flow' in params_sys[sys]:
            params_sys_dict['Flow'][sys]=params_sys[sys]

        else:
            params_sys_dict['Others'][sys]=params_sys[sys]
    # for key in params_sys_dict.keys():
    #     params_sys_dict[key].sort()
    params_dict_partitioned[system] = params_sys_dict

## Define directory paths ##

AVAIL_FOLDER_PATH = os.path.join(MAIN_FOLDER_PATH, f'Availability')
DATA_FOLDER_PATH = os.path.join(MAIN_FOLDER_PATH, 'Data')

## Retrieve data for each system to store in parquet files ##

systems_with_data = []
systems_withou_data = []
systems_param_mapping = {}
for system in systems:
    if not os.path.exists(DATA_FOLDER_PATH):
        os.mkdir(DATA_FOLDER_PATH)
    assert os.path.exists(DATA_FOLDER_PATH)
    print(f'Retrieving data for {system}.')
    system_parameters = list(systems_parameter_info['param_mapping'][f'{system}'].keys())
#     print(f'Parameters: \n{system_parameters}')

    # Get systems data:

    parameters_as_string = str(system_parameters).replace('[', '(').replace(']', ')')

    system_data_query = (f'SELECT t3.[Description], t4.[zzDescription], t1.[ParameterId], t1.[LogTime], t1.[Value] \
                           FROM [dbo].[fst_GEN_ParameterValue] AS t1 \
                           INNER JOIN [dbo].[fst_GEN_Parameter] AS t2 \
                            ON t1.[ParameterId] = t2.[ParameterID] \
                           INNER JOIN [dbo].[fst_GEN_System] AS t3 \
                            ON t2.[SystemID] = t3.[SystemID] \
                           INNER JOIN [dbo].fst_GEN_ParameterType AS t4 \
                            ON t2.[SystemTypeID] = t4.[SystemTypeID] \
                            AND t2.[ParameterNumber] = t4.[ParameterNumber] \
                           WHERE t3.[Description] = \'{str(system)}\' \
                           AND t2.[ParameterNumber] in {parameters_as_string} \
                           ORDER BY t1.[LogTime]')
    
    warnings.filterwarnings('ignore')
    system_data = pd.read_sql_query(system_data_query, con=db_connection)
    warnings.resetwarnings()
    
    system_data['ParameterInfo'] = system_data['zzDescription'] + ' RID ' + system_data['ParameterId'].astype(str)

    # Get system parameter mappings:

    all_customer_systems_query = ('SELECT [SystemID], [SystemTypeID], [Description] '
                                  'FROM [dbo].[fst_GEN_System] '
                                  'ORDER BY [Description]')
    warnings.filterwarnings('ignore')
    all_customer_systems_res = pd.read_sql_query(all_customer_systems_query, con=db_connection)
    warnings.resetwarnings()
    
    system_type_ids = all_customer_systems_res.loc[all_customer_systems_res.Description == system, 'SystemTypeID']

    if len(system_type_ids) == 0:
        print(f'{system} has not system type in the database {database}. Removing this system from the systems for which data will be retrieved.')
        del systems[system]
        continue
    elif len(system_type_ids) > 1:
        print(f'{system} has multiple system IDs - choosing {str(max(system_type_ids))}.')
    system_type_id= max(system_type_ids.values)
    
    get_parameters_info_query = (f'SELECT DISTINCT a.[ParameterNumber], [zzDescription], [SIUnitID] \
                                 FROM fst_GEN_Parameter a \
                                 INNER JOIN fst_GEN_ParameterType b \
                                     ON a.[SystemTypeID] = b.SystemTypeID \
                                     AND a.[ParameterNumber] = b.[ParameterNumber] \
                                 WHERE b.[SystemTypeId] = {str(system_type_id)} \
                                 ORDER BY a.[ParameterNumber] ASC')
    warnings.filterwarnings('ignore')
    system_param_mapping = pd.read_sql_query(get_parameters_info_query, con=db_connection)
    warnings.resetwarnings()
    
    systems_param_mapping[system] = dict(zip(system_param_mapping['ParameterNumber'].to_list(), system_param_mapping['zzDescription'].to_list()))

    if system_data is not None:
        file_path = os.path.join(DATA_FOLDER_PATH, system+'.parquet')
        system_data.to_parquet(file_path, compression=None)
        print(f'Data retrieved for {system}.')
        systems_with_data.append(system)
    
    else:
        print(f'There is no available data for {system}.')
        systems_withou_data.append(system)
    num_remaining_systems = len(systems) - (systems.index(system) + 1)
    print(f"{num_remaining_systems} systems remain to be processed.")
        
for sys in systems_param_mapping.keys():
    sys_param_nums = list(systems_param_mapping[sys].keys())
    for part in params_dict_partitioned[sys].keys():
        for param_num in params_dict_partitioned[sys][part].keys():
            if param_num in sys_param_nums:
                params_dict_partitioned[sys][part][param_num] = systems_param_mapping[sys][param_num].replace(' ', '')
            else:
                params_dict_partitioned[sys][part].pop([param_num])

systems_in_dict = list(params_dict_partitioned.keys())
for sys in systems_in_dict:
    if sys not in systems_with_data:
        params_dict_partitioned.pop(sys)


# In[24]:


# Visualisation #

DATA_FILES_DIR = DATA_FOLDER_PATH
FIG_DIR = os.path.join(MAIN_FOLDER_PATH,'Figures')
CSV_DIR = AVAIL_FOLDER_PATH
if not os.path.exists(DATA_FOLDER_PATH):
    os.mkdir(DATA_FOLDER_PATH)
if not os.path.exists(FIG_DIR):
    os.mkdir(FIG_DIR)
if not os.path.exists(CSV_DIR):
    os.mkdir(CSV_DIR)


# In[100]:


# from __future__ import print_function  # for Python2
# import sys

# local_vars = list(locals().items())
# vars_and_size = {}
# for var, obj in local_vars:
#     vars_and_size[var]=sys.getsizeof(obj)

# sorted_vals_and_size=sorted(vars_and_size.items(), key=lambda x:x[1])
# sorted_vals_and_size


# # In[101]:


# for var_and_size in sorted_vals_and_size[-10:]:
#     del locals()[var_and_size[0]]


# In[11]:


def get_data_dir_contents(data_files_dir, 
                          include_system_names_like=None):
    """
    Retrieves the names of all the files (i.e. systems) in the data_files_dir directory and places them in a list.
    """
    
    files = os.listdir(data_files_dir)
    if include_system_names_like != None:
        if type(include_system_names_like)==str:
            files = [file for file in files if include_system_names_like in file]

        elif type(include_system_names_like)==list or type(include_system_names_like)==tuple:
            files = [file for file in files if any(name_like in file for name_like in include_system_names_like)]

        else:
            raise TypeError(f"The argument `include_system_names_like` does not take assignments of type {type(include_system_names_like)}. Please pass a string or list of strings to this argument.")

    return files


# In[58]:


## Prepare the data in the parquet files for plotting ##

def plot_prep_from_parquet(data_files_dir, 
                           file_name, 
                           include_system_names_like=None):

    """
    Prepares the data that has already been written to parquet files for plotting. In particular, 
    this function separates the data for each system by swap date. It does so by partitioning the
    provisioned data by any columns whose name contains any of the following substrings:
    ('Run Hours', 'Time')
    
    Inputs:
    - data_files_dir (str): Full path or path from current working directory where the parquet files containing the system parametric data are stored.
    
    - include_system_names_like (str or list(str)): String or list of strings of the types of system names whose data we wish to  prepare. For instance, to prepare data for the systems that contain the substring 'iH1000' and 'iH2000', pass ['iH1000', 'iH2000']. DEFAULT is None and, in this case, all of the files in the provisioned directory are parsed.
    
    Outputs:
    - all_systems_data: A dictionary containing the data for the specified types of system names (if any were passed) partitioned by swap dates (if any swaps occurred).
    
    NB: If a system does not have any columns with the substring 'Run Hours' or 'Time', the system will be assumed to not be a pump and therefore skipped. Its data will not appear in the output.
    
    """
    
    system_data_dict = {}

    # Get the system data and format correctly:

    system_name = re.split(r'\.', file_name)[0]
    system_data = pd.read_parquet(os.path.join(data_files_dir, file_name))
    print(f'Preparing the {system_name} data for plotting.')

    # Convert DataFrame from long to wide format and sort by LogTime:

    system_data = system_data.pivot_table(index='LogTime',
                                          columns='ParameterInfo',
                                          values='Value').sort_values(by='LogTime')

    system_data = system_data.rename(columns={col_name:col_name.replace(' ', '') for col_name in system_data.columns})

    system_data = system_data.sort_values(by='LogTime')

    run_time_cols = list(set([col_name for col_name in system_data.columns 
                                                    if 'Time' in col_name
                                                    or 'Hour' in col_name]))

    print(f'Run Time Columns {run_time_cols}')

    try:
        assert len(run_time_cols) > 0
        run_time_col = run_time_cols[0]
        # system_data.rename({run_time_col:'RunHours'}, axis=1, inplace=True)

    except:
        # raise ValueError
        print(f"The system {system_name} does not have any parameters that include the strings `RunHours` nor `Time`. Therefore, it must not be a pump. Skipping.")
        return None, system_name

    print(f'Data for system {system_name} retrieved.')

    # Separate data by the swap number:

    run_hours = system_data[[run_time_col]][~system_data[run_time_col].isna()]
    run_hours_idx_list = list(run_hours.index)
    first_datum_idx = run_hours.index.min()
    first_datum_idx_num = run_hours_idx_list.index(first_datum_idx)
    swap_dates = [first_datum_idx]
    current_swap_num = 1

    run_hours['system_num'] = current_swap_num
    for idx_num in range(0, len(run_hours.index)-1):

        if idx_num in [0]:
            continue

        # *old condition_1* condition_1 = abs(run_hours.iloc[idx_num][run_time_col] - run_hours.iloc[idx_num + 1][run_time_col]) > 100

        # New condition_1 checks for a percentage drop (of greater than 60%) as opposed to a numerical drop for more robust detection

        if  run_hours.iloc[idx_num][run_time_col] != 0:
            condition_1 = run_hours.iloc[idx_num + 1][run_time_col] / run_hours.iloc[idx_num][run_time_col] < 0.4
        else:
            condition_1 = False

        # condition_2 ensures that the low difference in run_hours identified by condition_1 is 
        # not due to a break in incoming data

        current_dt = run_hours.index[idx_num]
        prev_dt = run_hours.index[idx_num-2]
        try:
            current_run_hrs = run_hours[run_time_col].loc[current_dt].iloc[0]
        except:
            try:
                current_run_hrs = run_hours[run_time_col].loc[current_dt]
            except:
                print('wtf')

        time_diff = ((run_hours.index[idx_num+1] - run_hours.index[idx_num])) + datetime.timedelta(hours=24)
        hrs_diff = float((time_diff/np.timedelta64(1, 'h')))
        future_dt = run_hours.index[idx_num] + time_diff
        current_dt = run_hours.index[idx_num]
        try:
            current_run_hrs = run_hours.loc[current_dt].iloc[0]
        except:
            current_run_hrs = run_hours.loc[current_dt]
        # print(run_hours.index[idx_num], run_hours.loc[run_hours.index[idx_num]]) 
        # print(run_hours.index[idx_num+1], run_hours.loc[run_hours.index[idx_num+1]])
        # print(hrs_diff) 
        # print(future_dt, run_hours.loc[future_dt[0]-datetime.timedelta(hours=2):future_dt[0]])
        local_future_run_hrs = run_hours.loc[current_dt:future_dt]
        last_future_val = local_future_run_hrs.iloc[-1]
        condition_2 = (current_run_hrs - last_future_val)[run_time_col] > hrs_diff

        try:
            if condition_1 and condition_2:
                current_swap_num += 1
                swap_dates.append(run_hours.index[idx_num+1])
        except:
            raise ValueError(f'condition_1: \n{condition_1}\n\ncondition_2: \n{condition_2}\n\ncurrent_run_hrs: \n{current_run_hrs}\n\nlast_future_val: \n{last_future_val}')

        run_hours.loc[run_hours.index[idx_num], 'system_num'] = current_swap_num

    run_hours['system_num'] = run_hours['system_num'].fillna(method='ffill').fillna(method='bfill')

    print(f'Swap dates for system {system_name} isolated.')

    system_data_dict[system_name] = {}

    system_data_dict[system_name]['swap_dates'] = swap_dates

    # Check if any swaps took place
    if len(swap_dates) == 0:
        system_data_dict[system_name]['system_1'] = system_data

    elif len(swap_dates) == 1:
        system_data_dict[system_name]['system_1'] = system_data[swap_dates[0]:]

    elif len(swap_dates) > 1:
        # Add the data, separated by swap number, into the system_data_all dictionary:
        for swap_dt_idx in range(len(swap_dates)):
            system_num = swap_dt_idx+1
            swap_dt = swap_dates[swap_dt_idx]
            if swap_dt_idx == len(swap_dates)-1:
                system_data_dict[system_name][f"system_{system_num}"] = system_data[swap_dt:]
            else:
                next_swap_dt = swap_dates[swap_dt_idx+1]
                system_data_dict[system_name][f"system_{system_num}"] = system_data[swap_dt:next_swap_dt]

        if system_data_dict[system_name]['swap_dates'][0] == system_data_dict[system_name]['swap_dates'][1]:
            del system_data_dict[system_name]['swap_dates'][0]
            system_key_nums = [key for key in system_data_dict[system_name].keys() if key!='swap_dates']
            for p_num in range(1,len(system_key_nums)):
                new_key = f'system_{p_num}'
                old_key = f'system_{p_num+1}'
                system_data_dict[system_name][new_key] = system_data_dict[system_name][old_key]
                del system_data_dict[system_name][old_key]

    print(f'Data for system {system_name} partitioned by swap date.')

#         print(f'System swap dates: {all_systems_data[system_name]["swap_dates"]}')

    print(f"Data for {system_name} prepared for plotting!")

#     if len(all_systems_data) > 0:
#         print(f"\n\nData retrieved and prepared for the following systems: ")
#         for system in all_systems_data.keys():
#             print(system)
    
#     else:
#         print('No data found for the specified systems.')
    
    return system_data_dict, system_name


# In[59]:


## Define a function to partition parameters into type of process

def order_parameters(data, cols, conceal_params=False):
    
    '''
    The function:
    1. Separates the parameters into groups 
    2. Changes the units of columns with temperature, pressure and flow from Kelvin, Pa and m^3/s to degrees Celcius, PSI and liters/minute, respectively.
    All of this is then used for columnar plotting by the function `interactive_plot_custom_data_1`.'''
    parameters_ordered = {'DryPump (DP)':[],
                          'Booster (MB)':[],
                          'ExhaustAndShaft (ES)':[],
                          'RunTime (RT)':[],
                          'Oil (OL)':[],
                          'Flow (FW)':[],
                          'Motor (MR)':[],
                          'Vibration (VR)':[],
                          'Pos (PS)':[],
                          'MagneticBearing (MC)':[],
                          'MiscellaneousTemperatures (MT)':[],
                          'Other (OT)':[]}
    
    data = data.rename(columns={col:col.replace(' ', '').replace('DP', 'DryPump').replace('MB', 'Booster') for col in data.columns})
    
    for column in data.columns:
        if ('Dry' in column or 'DP' in column) and ('Hours' not in column and 'Time' not in column):
            parameters_ordered['DryPump (DP)'].append(column)
        elif 'Booster' in column or 'MB' in column:
            parameters_ordered['Booster (MB)'].append(column)
        elif 'Exhaust' in column or 'Shaft' in column:
            parameters_ordered['ExhaustAndShaft (ES)'].append(column)
        elif 'Oil' in column:
            parameters_ordered['Oil (OL)'].append(column)
        elif 'Hour' in column or 'Time' in column:
            parameters_ordered['RunTime (RT)'].append(column)
        elif 'Flow' in column:
            parameters_ordered['Flow (FW)'].append(column)
        elif 'Motor' in column:
            parameters_ordered['Motor (MR)'].append(column)
        elif 'Vib' in column:
            parameters_ordered['Vibration (VR)'].append(column)
        elif 'Pos' in column:
            parameters_ordered['Pos (PS)'].append(column)
        elif 'Magnetic' in column:
            parameters_ordered['MagneticBearing (MC)'].append(column)
        elif 'Temperature' in column:
            parameters_ordered['MiscellaneousTemperatures (MT)'].append(column)
        else:
            parameters_ordered['Other (OT)'].append(column)
        
        if 'temp' in column.lower():
            data[column] = data[column] - 273.15
        elif 'pressure' in column.lower():
            data[column] = data[column] * 0.000145038
        elif 'flow' in column.lower():
            data[column] = data[column] * 60000
    
    parameters_ordered_concealed = {}
    
    for key in parameters_ordered.keys():
        parameters_ordered[key].sort()
        left_idx = key.index('(')+1
        right_idx = key.index(')')
        concealed_key = key[left_idx:right_idx]
        parameters_ordered_concealed[concealed_key] = parameters_ordered[key]
    
    if cols==None:
        cols = data.columns
        
    if conceal_params:
        return parameters_ordered_concealed, data
    else:
        return parameters_ordered, data


# In[115]:


## Define a function to compute sma and ewma for each parameter passed

def moving_averages(data, 
                    current_parameter, 
                    run_time_data=False,
                    ewma_span=None,
                    ewma_com=None,
                    ewma_halflife=None,
                    ewma_alpha=None,
                    ewma_min_periods=None,
                    ewma_adjust=True,
                    ewma_ignore_na=False,
                    resampling_frequency='12H',
                    rolling_period='14D'):

    if not run_time_data:
        start_date = data.index.min()
        end_date = data.index.max()
        data = data.loc[~data.index.duplicated()]
        old_data = data.copy()
        try:
            new_index = pd.date_range(start=start_date,
                                      end=end_date,
                                      freq=resampling_frequency)
        except:
            print(start_date)
            print(end_date)
            raise ValueError
        smoothed_data = data[data.notna()].reindex(new_index, method='ffill')
        smoothed_data.index.name='LogTime'
        col_data_df = pd.DataFrame(smoothed_data)

        # Simple Moving Average:
        sma_parameter = f'{current_parameter}_SimpleMovingAverage'
        sma_col_data = col_data_df[current_parameter].rolling(rolling_period).mean()
        col_data_df[sma_parameter] = sma_col_data

        # Exponentially Weighted Moving Average:
        ewma_parameter = f'{current_parameter}_ExpontentiallyWeightedMovingAverage'
        ewma_col_data = col_data_df[current_parameter].ewm(span=ewma_span,
                                                           com=ewma_com,
                                                           halflife=ewma_halflife,
                                                           alpha=ewma_alpha,
                                                           min_periods=ewma_min_periods,
                                                           adjust=ewma_adjust,
                                                           ignore_na=ewma_ignore_na).mean()
        col_data_df[ewma_parameter] = ewma_col_data
        col_data_df = col_data_df.reindex(old_data.index, 
                                          method='ffill')
        try:
            col_data_df[current_parameter] = old_data
        except:
            try:
                col_data_df[current_parameter] = old_data[current_parameter]
            except:
                pass
                raise ValueError("There's a problem.")
        
        return col_data_df, sma_parameter, ewma_parameter
    
    else:
        col_data_df = pd.DataFrame(data[data.notna()])
        return col_data_df


# In[116]:


## Define funtion to create arbitrarily deep dictionaries

from collections import defaultdict
def recursive_dict():
    return defaultdict(recursive_dict)


# In[117]:


## Create dashboards ##

def bokeh_plot_all_system_data(data,
                             save_dest,
                             system,
                             system_position,
                             customer_name,
                             cols=None,
                             save=True,
                             show_plots=False,
                             conceal_params=False):
    
    
    start_datetime = data.index.min().strftime('%d/%m/%Y %H:%M:%S')

    end_datetime = data.index.max().strftime('%d/%m/%Y %H:%M:%S')
    
    system_name = f"{system_position}: {system.capitalize().replace('_', ' ')}, Start Date-Time: {start_datetime}, End Date-Time: {end_datetime}"

    parameters_ordered, data = order_parameters(data, cols, conceal_params=conceal_params)
    
    parts_plots = {}
    count = 0
    for i in parameters_ordered.keys():
        # print(f'\npart parames before anything: \n{parameters_ordered}')
        # parameters_ordered[i] = [parameters_ordered[i][j] for j in parameters_ordered[i] if j in data.columns]
        # print(f'\npart params after checking against data columns: \n{parameters_ordered}')
        part_data = data[parameters_ordered[i]]
#         return part_data
        part_data = part_data.rename({col:col.replace(' ', '') for col in part_data.columns}, axis=1)
        parameters_ordered[i] = [param.replace(' ', '') for param in parameters_ordered[i]]
        parts_plots[i] = []
        # print(parameters_ordered)
        for j in range(len(parameters_ordered[i])):
            col_data = part_data[parameters_ordered[i][j]]
            # print(type(col_data))
            if col_data.notna().sum() < 1:
                continue
            elif col_data.notna().sum() < 15:
                radius_size=3
            else:
                radius_size=0.8
            current_parameter = parameters_ordered[i][j]
            


            col_data = col_data[col_data.notna()]
            
            if 'RT' not in i:
                col_data_df, sma_parameter, ewma_parameter = moving_averages(col_data, 
                                                                         current_parameter=current_parameter,
                                                                         run_time_data=False,
                                                                         rolling_period='14D',
                                                                         ewma_alpha=0.15,
                                                                         ewma_adjust=False)
                if conceal_params:
#                     print('concealing parameter')
                    left_raw_idx = current_parameter.index('RID')
                    left_sma_idx = sma_parameter.index('RID')
                    left_ewma_idx = ewma_parameter.index('RID')
                    raw_label = f'{customer_name}_{current_parameter[left_raw_idx:]}'
                    sma_label = f'{customer_name}_{sma_parameter[left_sma_idx:]}'
                    ewma_label = f'{customer_name}_{ewma_parameter[left_ewma_idx:]}'
#                     print(raw_label, sma_label, ewma_label, sep='\n')
                    
                else:
#                     print('not concealing parameter')
                    raw_label = current_parameter
                    sma_label = sma_parameter
                    ewma_label = ewma_parameter
#                     print(raw_label, sma_label, ewma_label, sep='\n')
                
                source = plotting.ColumnDataSource(col_data_df)

                # Create interactive hovertool
                fig_hover_tool = HoverTool(tooltips=[('LogTime', '@LogTime{%Y-%m-%d %H:%M:%S.%3N}'),
                                                    (f'{raw_label}', f'@{current_parameter}'),
                                                    (f'{sma_label}', f'@{sma_parameter}'),
                                                    (f'{ewma_label}', f'@{sma_parameter}')],
                                           formatters={'@LogTime':'datetime'},
                                           mode='mouse')    

            else:
                col_data_df = moving_averages(col_data,
                                              current_parameter=current_parameter,
                                              run_time_data=True,
                                              rolling_period='14D',
                                              ewma_alpha=0.15,
                                              ewma_adjust=False)
                source = plotting.ColumnDataSource(col_data_df)
                raw_label = current_parameter
                # Create interactive hovertool
                fig_hover_tool = HoverTool(tooltips=[('LogTime', '@LogTime{%Y-%m-%d %H:%M:%S.%3N}'),
                                                    (f'{raw_label}', f'@{current_parameter}')],
                                           formatters={'@LogTime':'datetime'},
                                           mode='mouse')
            
            if count > 0:
                fig = plotting.figure(x_axis_label='DateTime',
                                      y_axis_label=raw_label,
                                      x_range=shared_x_range,
                                      x_axis_type='datetime',
                                      title=raw_label)
                # print(f'\n\nData plotted for {system_position} {system} {i}:{j}')
            
            else:
                fig = plotting.figure(x_axis_label='DateTime',
                                      y_axis_label=raw_label,
                                      x_axis_type='datetime',
                                      title=raw_label)
                # print(f'\n\nData plotted for {system_position} {system} {i}:{j}')
            
            if count == 0:
                count += 1
                shared_x_range = fig.x_range

            fig.line(x='LogTime', 
                     y=current_parameter, 
                     source=source, 
                     color='#47ed00',
                     line_alpha=0.7,
                     muted_alpha=0.2,
                     legend_label=raw_label)
            
            fig.add_tools(fig_hover_tool)
            
            if 'RT' not in i:
                fig.line(x='LogTime',
                        y=sma_parameter,
                        source=source,
                        color='red',
                        line_alpha=1,
                        muted_alpha=0.1,
                        legend_label=sma_label)

                fig.line(x='LogTime',
                        y=ewma_parameter,
                        source=source,
                        color='blue',
                        line_alpha=1,
                        muted_alpha=0.2,
                        legend_label=ewma_label)

            fig.circle(x='LogTime',
                       y=current_parameter,
                       source=source,
                       color='green',
                       radius=radius_size)
            
            fig.title.text_font_size = '12pt'

            fig.xaxis.major_label_orientation = math.pi/4

            fig.axis.axis_label_text_font_size = '10px'

            fig.legend.title = 'Legend'

            fig.legend.title_text_font_size = '12pt'

            fig.legend.title_text_font_style = 'italic'

            fig.legend.title_text_color = 'white'

            fig.legend.location = 'top_left'

            fig.legend.border_line_alpha = 1

            fig.legend.border_line_color = 'black'

            fig.legend.background_fill_alpha = 0.7

            fig.legend.background_fill_color = 'grey'

            fig.legend.click_policy = 'hide'

            fig.legend.label_text_font_size = '12pt'

            fig.legend.label_text_font_style = 'italic'

            fig.legend.label_text_color = 'white'
            
            # fig.add_layout(fig.legend[0], 'right')

            parts_plots[i].append(fig)
    
    
    plot_title = Div(text=f"{system_name}",
                     style={'font-size':'30px', 'color':'black'}) #, style={'font-size':'300%', 
                                            #       'color':'black', 
                                            #       'text-align':'center', 
                                            #       'margin':'auto'})
    
    plot_columns = []
    for key in parts_plots.keys():
        if len(parts_plots[key]) > 0:
            part_name = Div(text=f'{key} Parameters',
                            style={'font-size':'22px', 'color':'black'})
            plot_columns.append(layouts.column(part_name, layouts.column(parts_plots[key])))
        
    system_figure_dir = os.path.join(save_dest, system_position)
    
    if not os.path.exists(system_figure_dir):
        os.makedirs(system_figure_dir)
        assert os.path.exists(system_figure_dir)
    
    if conceal_params:
        output_file_name = os.path.join(system_figure_dir, f"{system_position}_{system}_concealed_parameters.html")
    else:
        output_file_name = os.path.join(system_figure_dir, f"{system_position}_{system}.html")
    
    if save:
        io.output_file(output_file_name)
        
        plotting.save(layouts.column(plot_title,
                                     layouts.row(plot_columns)))
        
    if show_plots:
        io.show(layouts.column(plot_title,
                                     layouts.row(plot_columns)))
        
bokeh_system_data_plotters = {}
bokeh_system_data_plotters['all']=bokeh_plot_all_system_data


# In[118]:


# Define a list of numpy arrays where each array contains the rows of the desired dashboard view spec

custom_views = recursive_dict()
custom_views['view_1'] = np.array([['BoosterPower', 'DryPumpPower', 'ExhaustPressure'],
                          ['BoosterSpeed', 'DryPumpSpeed', 'DryPumpInternalTemp'],
                          ['BoosterTemp', 'DryPumpHVTemp', 'DryPumpEndCoverTemp']],
                          dtype='object').T
custom_views['view_2'] = np.array([['BoosterPower', 'DryPumpPower', 'ExhaustPressure', 'ExhaustPressure'],
                          ['BoosterSpeed', 'DryPumpSpeed', 'RunHours', 'N2Flow'],
                          ['BoosterTemp', 'DryPumpHVTemp', 'DryPumpEndCoverTemp', 'DryPumpInternalTemp'],
                          ['VI1', 'VI2', 'VI3', 'VI4']],
                          dtype='object').T
custom_views['view_3'] = np.array([['BoosterPower', 'DryPumpPower', 'ExhaustPressure', 'ExhaustPressure'],
                          ['BoosterSpeed', 'DryPumpSpeed', 'RunHours', 'N2Flow'],
                          ['BoosterTemp', 'DryPumpInternalTemp', 'DryPumpHVTemp', 'DryPumpEndCoverTemp'],
                          ['VI1', 'VI2', 'VI3', 'VI4']],
                          dtype='object').T

# In[]:

# Change parameter names to searchable regex strings

booster_len = len('Booster')
dp_len = len('DryPump')
for key in custom_views.keys():
    for parent_idx in list(range(len(custom_views[key]))):
        parent_list = custom_views[key][parent_idx]
        for idx in list(range(len(parent_list))):
            string = parent_list[idx]
            if 'Booster' in string:
                new_string = string.replace(string[booster_len:], 
                                            fr"*({string[booster_len:]})")
                new_string = new_string.replace('Booster', 
                                                '(Booster)')
            elif 'DryPump' in string:
                new_string = string.replace(string[dp_len:], 
                                            fr"*({string[dp_len:]})")
                new_string = new_string.replace('DryPump', 
                                                '(DryPump)')
            else:
                new_string = string
            
            custom_views[key][parent_idx][idx] = new_string


# In[119]:


# Define a function to return the index of an item in an arbitrarily nested array

def ndarray_idx(array, item):
    for idx, val in np.ndenumerate(array):
        if array[idx] == item:
            return idx


# In[120]:


def bokeh_plot_system_view(data,
                           system,
                           system_position,
                           customer_name,
                           save_dest,
                           views_array,
                           view_to_use='view_1',
                           cols=None,
                           save=True,
                           show_plots=False,
                           conceal_params=False):
    
    data = data.rename({col:col.replace(' ', '') for col in data.columns}, axis=1)
    
    data.index.name = 'LogTime'
    
    start_datetime = data.index.min().strftime('%d/%m/%Y %H:%M:%S')

    end_datetime = data.index.max().strftime('%d/%m/%Y %H:%M:%S')
    
    dash_title = f"{system_position}: {system.capitalize().replace('_', ' ')}, {view_to_use} Start Date-Time: {start_datetime}, End Date-Time: {end_datetime}"
    
    view = views_array[view_to_use]
    
    dashboard_dimensions = view.shape
    
    parts_plots = [[] for i in range(dashboard_dimensions[0])] # Initialise a list of n rows where n is the number of rows in `view`
    
    non_empty_plot_count = 0
    
    for param_idx, param in np.ndenumerate(view):
        
        param_row_idx=param_idx[0]
        
        # Find parameter in data
        
        if param!='RunHours':
            param_to_seek = copy.deepcopy(param)
            param_to_seek_alt = param_to_seek.replace('Booster', 'MB').replace('DryPump', 'DP')
        
        else:
            param_to_seek = 'Hour'
            param_to_seek_alt = 'RunTime'
            
        param_data = data.filter(regex=f"({param_to_seek})|({param_to_seek_alt})")
        
        # Ensure there is data to plot
        try:
            assert len(param_data.columns) > 0
            param_col = param_data.columns[0]
            assert len(param_data[param_data[param_col].notna()]) > 0
        except:
            print(f"There is no {param} data for {system}.")
            if conceal_params:
                empty_fig_title = 'unavailable_parameter'
            else:
                empty_fig_param = param.replace('(','').replace(')','').replace('*','')
                empty_fig_title = f'unavailable_parameter_{empty_fig_param}'
            
            if sum(param_idx) > 0:
            
                empty_fig = plotting.figure(x_axis_label='DateTime',
                                            y_axis_label=param,
                                            x_axis_type='datetime',
                                            title=empty_fig_title)
            else:
                empty_fig = plotting.figure(x_axis_label='DateTime',
                                            y_axis_label=param,
                                            x_axis_type='datetime',
                                            title=empty_fig_title)
            
            parts_plots[param_row_idx].append(empty_fig)
            continue
        
        try:
            param_col = param_data.columns[0]
        except:
            print(f"ran into problem with {system} {system_position}, {param_to_seek}")
            print(param_data)
                
        param_data = param_data[param_data[param_col].notna()]
        
        # Check if this is a run hours parameter
        if 'Hour' in param or 'RunTime' in param:
            # Just data will be plotted if the parameter is a run hours parameter
            print(f'The RunHours parameter is {param_col}')
            param_df = pd.DataFrame(param_data)
            source = plotting.ColumnDataSource(param_df)
            raw_label = param_col
            print(f"The names of the columns of the column data source are {source.column_names}")
            fig_hover_tool = HoverTool(tooltips=[('LogTime', '@LogTime{%Y-%m-%d %H:%M:%S.%3N}'),
                                                (f'{raw_label}', f'@{param_col}')],
                                       formatters={'@LogTime':'datetime'},
                                       mode='mouse')
        else:
            # Otherwise, compute SMA and EWMA
            try:
                param_df, sma_parameter, ewma_parameter = moving_averages(param_data,
                                                                          current_parameter=param_col,
                                                                          run_time_data=False,
                                                                          rolling_period='14D',
                                                                          ewma_alpha=0.5,
                                                                          ewma_adjust=False)
            except:
                print(system)
                print(system_position)
                print(param)
                print(param_col)
                print(param_df)
                raise ValueError
            
            # Check if concealment has been requested
            if conceal_params:
                # If so, conceal parameter names and set the result to `_label` variables
                left_raw_idx = param_col.index('RID')
                left_sma_idx = sma_parameter.index('RID')
                left_ewma_idx = ewma_parameter.index('RID')
                raw_label = f"{customer_name}_{param_col[left_raw_idx:]}"
                sma_label = f"{customer_name}_{sma_parameter[left_sma_idx:]}"
                ewma_label = f"{customer_name}_{ewma_parameter[left_ewma_idx:]}"
                
            else:
                # Otherwise, set the labels to be the same as the parameter names
                raw_label = param_col
                sma_label = sma_parameter
                ewma_label = ewma_parameter
            
            # Create a Bokeh ColumnDataSource object from the data
            source = plotting.ColumnDataSource(param_df)
            
            # Create interactive hovertool
            fig_hover_tool = HoverTool(tooltips=[('LogTime', '@LogTime{%Y-%m-%d %H:%M:%S.%3N}'),
                                                (f'{raw_label}', f'@{param_col}'),
                                                (f'{sma_label}', f'@{sma_parameter}'),
                                                (f'{ewma_label}', f'@{sma_parameter}')],
                                       formatters={'@LogTime':'datetime'},
                                       mode='mouse')

        # Initialise figure

        if non_empty_plot_count > 0:
            # If this is not the first figure, use the shared_x_range
            fig = plotting.figure(x_axis_label='DateTime',
                                  y_axis_label=raw_label,
                                  x_range=shared_x_range,
                                  x_axis_type='datetime',
                                  title=raw_label)
        else:
            # Otherwise, allow the figure to default to the x_range required to plot the data
            fig = plotting.figure(x_axis_label='DateTime',
                                  y_axis_label=raw_label,
                                  x_axis_type='datetime',
                                  title=raw_label)

        # Add objects to figure
        fig.line(x='LogTime', 
                 y=param_col,
                 source=source,
                 color='#47ed00',
                 line_alpha=0.7,
                 muted_alpha=0.2,
                 legend_label=raw_label)
        
        fig.add_tools(fig_hover_tool)
        
        if 'Hour' in param or 'RunTime' in param:
            pass
        else:
            # If this is not a run hours parameter, plot the sma and ewma
            fig.line(x='LogTime',
                     y=sma_parameter,
                     source=source,
                     color='red',
                     line_alpha=1,
                     muted_alpha=0.1,
                     legend_label=sma_label)

            fig.line(x='LogTime',
                     y=ewma_parameter,
                     source=source,
                     color='blue',
                     line_alpha=1,
                     muted_alpha=0.2,
                     legend_label=ewma_label)

        # If this is the first non-empty plot 
        if non_empty_plot_count == 0:
            shared_x_range = fig.x_range
            non_empty_plot_count+=1
            
        # Modify figure appearance
        
        fig.title.text_font_size = '12pt'

        fig.xaxis.major_label_orientation = math.pi/4

        fig.axis.axis_label_text_font_size = '10px'

        fig.legend.title = 'Legend'

        fig.legend.title_text_font_size = '12pt'

        fig.legend.title_text_font_style = 'italic'

        fig.legend.title_text_color = 'white'

        fig.legend.location = 'top_left'

        fig.legend.border_line_alpha = 1

        fig.legend.border_line_color = 'black'

        fig.legend.background_fill_alpha = 0.7

        fig.legend.background_fill_color = 'grey'

        fig.legend.click_policy = 'hide'

        fig.legend.label_text_font_size = '12pt'

        fig.legend.label_text_font_style = 'italic'

        fig.legend.label_text_color = 'white'

        # Save x_range if this is the first figure

        if sum(param_idx) == 0:
            shared_x_range = fig.x_range

        # Add plot to the correct row in the parts_plots list
            
        parts_plots[param_row_idx].append(fig)
    
    dashboard_title = Div(text=f"{dash_title}",
                          style={'font-size':'30px', 'color':'black'})
    
    dashboard_rows = []
    for dash_row_idx in range(len(parts_plots)):
        dash_row = layouts.row(parts_plots[dash_row_idx])
        dashboard_rows.append(dash_row)
    
    system_figure_dir = os.path.join(save_dest, system_position)
    
    if not os.path.exists(system_figure_dir):
        os.makedirs(system_figure_dir)
        assert os.path.exists(system_figure_dir)
    
    if conceal_params:
        output_file_name = os.path.join(system_figure_dir, f"{system_position}_{system}_{view_to_use}_concealed_params.html")
    else:
        output_file_name = os.path.join(system_figure_dir, f"{system_position}_{system}_{view_to_use}.html")
        
    
    if save:
        io.output_file(output_file_name)
        plotting.save(layouts.column(dashboard_title, layouts.column(dashboard_rows)))
        
    if show_plots:
        io.show(layouts.column(dashboard_title, layouts.column(dashboard_rows)))

bokeh_system_data_plotters['view']=bokeh_plot_system_view


# In[129]:


def interactive_plot_all_systems_data(data_files_dir,
                                      save_dest,
                                      customer_name,
                                      mark='all',
                                      cols=None,
                                      save=True,
                                      show_plots=False,
                                      separate_by_swap=True,
                                      sep_and_whole=True,
                                      conceal_params=False,
                                      views_array=None):
    
    data_files = get_data_dir_contents(data_files_dir=data_files_dir)
    
    for file_name in data_files:
        system_data_dict, system_name = plot_prep_from_parquet(data_files_dir=data_files_dir,
                                                               file_name=file_name)
        try:
            assert system_data_dict is not None
        except:
            continue # this is where I got to
    
        position = system_name # Every 'system_name' is actually the name of a position at a customer site at which particular systems are placed
        
        # Changes: all_system_data[position] now replaced with 'system_data_dict[position]'
        
        position_systems = [dict_key for dict_key in system_data_dict[position].keys() if 'swap_date' not in str(dict_key)]
        if separate_by_swap or sep_and_whole:
            for system in position_systems: # Here system means the system number at that position - e.g. system `pump_1` at position KOXDL50400
                print(f'\nWorking on the separated by swap date plot for {position} {system}.\n')
                if mark=='all':
                    bokeh_system_data_plotters[mark](data=system_data_dict[position][system],
                                                    save_dest = save_dest,
                                                    system_position = f"{position}",
                                                    system=f"{system}",
                                                    show_plots=show_plots,
                                                    save=save,
                                                    conceal_params=conceal_params,
                                                    customer_name=customer_name)
                elif mark=='view':
                    for view_to_use in list(views_array.keys()):
                        try:
                            bokeh_system_data_plotters[mark](data=system_data_dict[position][system],
                                                        save_dest = save_dest,
                                                        views_array=views_array,
                                                        view_to_use=view_to_use,
                                                        system_position = f"{position}",
                                                        system=f"{system}",
                                                        show_plots=show_plots,
                                                        save=save,
                                                        conceal_params=conceal_params,
                                                        customer_name=customer_name)
                        except:
                            print(position)
                            print(system)
                            print(system_data_dict[system])
                            raise ValueError('Something went wrong.')

                if save:
                    text = '\033[1m' + f"Dashboard for {position} {system} generated and placed within '{save_dest}.'" + '\033[0m'
                    colored_text = colored(text=text, color='blue')
                    print(colored_text)

        if (not separate_by_swap) or sep_and_whole:
            all_data_for_position = pd.concat([system_data_dict[position][sys] for sys in position_systems])
            print(f'\nWorking on the plot for all of the data on {position}.\n')

            if mark=='all':
                bokeh_system_data_plotters[mark](data=all_data_for_position,
                                                save_dest=save_dest,
                                                system_position=position,
                                                system='All_Systems',
                                                show_plots=show_plots,
                                                save=save,
                                                conceal_params=conceal_params,
                                                customer_name=customer_name)
            elif mark=='view':
                for view_to_use in list(views_array.keys()):
                        try:
                            bokeh_system_data_plotters[mark](data=all_data_for_position,
                                                            save_dest=save_dest,
                                                            system_position=position,
                                                            system='All_Systems',
                                                            views_array=views_array,
                                                            view_to_use=view_to_use,
                                                            show_plots=show_plots,
                                                            save=save,
                                                            conceal_params=conceal_params,
                                                            customer_name=customer_name)
                        except:
                            print(position)
                            print(system)
                            print(all_data_for_position)
                            raise ValueError('Something went wrong.')
            if save:
                text = '\033[1m' + f"Dashboard for all of the data on {position} generated and placed within '{save_dest}.'" + '\033[0m'
                colored_text = colored(text=text, color='blue')
                print(colored_text)


# In[130]:


# Plot views for all systems:

interactive_plot_all_systems_data(DATA_FOLDER_PATH,
                                  save_dest=FIG_DIR,
                                  mark=dash_view_or_all,
                                  views_array=custom_views,
                                  show_plots=False,
                                  save=True,
                                  separate_by_swap=separate_by_swap,
                                  sep_and_whole=sep_and_whole,
                                  conceal_params=conceal_params,
                                  customer_name=customer_account_name)


# In[ ]: