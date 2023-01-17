from stp.stp_indicator_dash import StpDash
import stp
import pandas as pd
from os import listdir, path, mkdir, rmdir
import pandas as pd
import datetime
import warnings
import shutil
import os
import matplotlib.pyplot as plt
import pickle
from datetime import timedelta
import sys
from stp import stp_indicator_dash

ROOT_PATH = sys.path[0][:sys.path[0].rindex('\\')]

RESOURCES_PATH = os.path.join(ROOT_PATH, r'Resources')

config_file = os.path.join(RESOURCES_PATH, r"xfab_stp_indicator_config.csv")
config = pd.read_csv(config_file)


def DP_computer(system_file_path, data, dash_folder, config=config):
    
    tool_name = system_file_path[system_file_path.rindex('\\')+1:].replace('.parquet', '')


    if tool_name in config.tool_name.values:
       current_pk_t = float(config[config.tool_name == tool_name].current_peak)
       vb_pk_t = float(config[config.tool_name == tool_name].vibB_peak)
       print(tool_name)
       stp_dash =stp.stp_indicator_dash.StpDash(data, tool_name, current_pk_t=current_pk_t, vb_pk_t=vb_pk_t, normal_only=False, des_path = dash_folder)
    else:
        stp_dash = stp_indicator_dash.StpDash(data, tool_name, normal_only=False, des_path=dash_folder)
       
    _, _, indicator_plots = stp_dash.plot_indicators(plot=False, plots_configs=True)
    
    return indicator_plots