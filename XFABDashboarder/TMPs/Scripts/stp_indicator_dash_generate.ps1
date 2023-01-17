Set-ExecutionPolicy -ExecutionPolicy Bypass -Force

Set-Location $PSScriptRoot

$python_exe_loc = "C:\Program Files\Anaconda3\envs\stp\python.exe"

$code_dir_loc = "C:\Users\Administrator\Documents\GitHub\stp_indicators"

cd $code_dir_loc

conda activate stp

python "$code_dir_loc\xfab_STP_Data_Extract.py"
python "$code_dir_loc\xfab_indicator_dash_generate.py"
python "$code_dir_loc\xfab_plot_dash.py"

Read-Host -Prompt "Press any key to continue"