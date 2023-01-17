import pickle
from os import path, listdir
import matplotlib.pyplot as plt
import datetime

dash_folder = r'D:\XFab_STP\dash'
folder_names = listdir(dash_folder)

latest_date = max([datetime.datetime.strptime(i, '%Y-%m-%d')
                   for i in folder_names]).strftime("%Y-%m-%d")
latest_folder = path.join(dash_folder, latest_date)

file_name = 'LP42C-STP.pkl'
file_path = path.join(latest_folder, file_name)
with open(file_path, 'rb') as fid:
    ax = pickle.load(fid)
plt.show()