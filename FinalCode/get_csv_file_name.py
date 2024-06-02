import os
import glob

# Directory containing the .csv files
input_dir = "./processed_datas/"

filename_list = []

csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
for csv_file in csv_files:
    # just print the csv file name, without the file path
    file_name = os.path.basename(csv_file)
    filename_list.append(file_name[:-4])

print(filename_list)