from ev_utilities import DataAvailability
import os
import pandas as pd
from ev_utilities.db_utils.DataExtractor import get_db

ROOT_PATH = input('Please enter the path to where you would like the csv to be saved:\n\n')

while not os.path.exists:
    try:
        os.makedirs(ROOT_PATH)
    except:
        print('Could not find the path entered and could not create it either. Please enter another path.\n')
        ROOT_PATH = input('Please enter the path to where you would like the csv to be saved:\n\n')

RESOURCES_PATH = r'C:\Users\a00555655\OneDrive - ONEVIRTUALOFFICE\Documents\Python Scripts\Dashboarder\Resources'

server_con = DataAvailability.con_create(RESOURCES_PATH=RESOURCES_PATH)

get_dbs_query = ("select name "
                 +"from master.sys.databases "
                 +"where name like '%scada_production%'")

customer_data_databases = pd.read_sql_query(get_dbs_query, server_con.connect())

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

availability = DataAvailability.get_avail(db = database, 
                                          save=True, 
                                          ROOT_PATH=ROOT_PATH, 
                                          RESOURCES_PATH=RESOURCES_PATH)

import imp
imp.reload(DataAvailability)