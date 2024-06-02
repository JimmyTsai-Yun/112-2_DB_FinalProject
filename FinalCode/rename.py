import pandas as pd

# 讀取 CSV 文件
file_path = '/Users/caichengyun/Desktop/DB/FinalProject/processed_gold_update/GOLD_filtered_warperiod.csv'
data = pd.read_csv(file_path)

# 修改 company 欄位為 'GOLD_WAR'
data['company'] = 'GOLD_WAR'

# 保存為新的 CSV 文件
modified_file_path = '/Users/caichengyun/Desktop/DB/FinalProject/processed_gold_update/GOLD_filtered_warperiod_cleaned.csv'
data.to_csv(modified_file_path, index=False)

