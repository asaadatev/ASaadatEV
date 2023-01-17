from stp import stp_indicator_dash
from os import listdir, path, mkdir, rmdir
import pandas as pd
import datetime
import warnings
import shutil
import os

warnings.filterwarnings("ignore", category=RuntimeWarning)

date = datetime.date.today().strftime("%Y-%m-%d")

config_file = r"C:\Users\Administrator\Documents\GitHub\stp_indicators\xfab_stp_indicator_config.csv"
config = pd.read_csv(config_file)

data_folder = r'D:\XFab_STP\data'
folder_names = listdir(data_folder)

latest_date = max([datetime.datetime.strptime(i, '%Y-%m-%d')
                   for i in folder_names]).strftime("%Y-%m-%d")
latest_folder = path.join(data_folder, latest_date)

file_names = listdir(latest_folder)

dash_parent_dir = r'D:\XFab_STP\dash'

dash_folder = path.join(dash_parent_dir, date)

try:"D:\XFab_STP\data\2023-01-09\LP42A-STP.parquet"
    mkdir(dash_folder)
except OSError:
    print("Failed to create new folder, check if already exist")
else:
    print("Successfully created new folder")

for file_name in file_names:
    file_path = path.join(latest_folder, file_name)
    data = pd.read_parquet(file_path)
    data = data.sort_index()
    tool_name = file_name.replace('.parquet', '')
    if tool_name in config.tool_name.values:
        current_pk_t = float(config[config.tool_name == tool_name].current_peak)
        vb_pk_t = float(config[config.tool_name == tool_name].vibB_peak)
        print(tool_name)
        stp_dash = stp_indicator_dash.StpDash(data, tool_name, current_pk_t=current_pk_t, vb_pk_t=vb_pk_t,
                                              normal_only=False, des_path=dash_folder)
    else:
        stp_dash = stp_indicator_dash.StpDash(data, tool_name, normal_only=False, des_path=dash_folder)

    stp_dash.save_plot()


# ASaadat additions for clean-up

def listdir_fullpath(dir):
    return [os.path.join(dir, file) for file in os.listdir(dir)]

data_parent_dir = data_folder

data_dir_paths = listdir_fullpath(data_parent_dir)

latest_data_dir = latest_folder

for data_dir_path in data_dir_paths:
    if latest_data_dir != data_dir_path:
        try:
            shutil.rmtree(data_dir_path)
            print(f'Purged \'{data_dir_path}\' containing old data.')
        except:
            files_in_dir = os.listdir(data_dir_path)
            if len(files_in_dir) < 1:
                print(f'The old data directory {data_dir_path} could not be deleted but all the files within have been purged.')
            else:
                print(f'The old data directory {data_dir_path} could not be deleted and it still contains {len(files_in_dir)} files.')

dash_dir_paths = listdir_fullpath(dash_parent_dir)

latest_dash_dir = dash_folder

for dash_dir_path in dash_dir_paths:
    if latest_dash_dir != dash_dir_path:
        try:
            shutil.rmtree(dash_dir_path)
            print(f'Purged \'{dash_dir_path}\' containing old dashboards.')
        except:
            files_in_dir = os.listdir(dash_dir_path)
            if len(files_in_dir) < 1:
                print(f'The old dashboard directory {dash_dir_path} could not be deleted but all the files within have been purged.')
            else:
                print(f'The old dashboard directory {dash_dir_path} could not be deleted and it still contains {len(files_in_dir)} files.')
    