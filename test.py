from main1 import data_folder
from scripts.py_scripts.prepare_data import get_data_files

data_files = get_data_files(data_folder)
print(data_files["terminals"])