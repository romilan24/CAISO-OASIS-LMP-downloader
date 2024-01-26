import pandas as pd
import requests
import csv
import time
from datetime import datetime, timedelta
import zipfile
import io

start_year = 2021
end_year = 2023
node = "TH_SP15_GEN-APND"

month_dict = {'Jan': ['0101', '0201'],
              'Feb': ['0201', '0301'],
              'Mar': ['0301', '0401'],
              'Apr': ['0401', '0501'],
              'May': ['0501', '0601'],
              'Jun': ['0601', '0701'],
              'Jul': ['0701', '0801'],
              'Aug': ['0801', '0901'],
              'Sep': ['0901', '1001'],
              'Oct': ['1001', '1101'],
              'Nov': ['1101', '1201'],
              'Dec': ['1201', '1231']
              }

base_url = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=12&"

all_data = []
for year in range(start_year, end_year + 1):
    links = [
        f"{base_url}startdatetime={year}{month_dict[month][0]}T08:00-0000&enddatetime={year}{month_dict[month][1]}T08:00-0000&market_run_id=DAM&node={node}"
        for month in month_dict.keys()
    ]

    # Create links file for the current year
    with open(f"links_{year}.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for link in links:
            writer.writerow([link])

    # Download and process data for each month in the year
    for link in links:
        response = requests.get(link)
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        for file in zip_file.namelist():
            if file.endswith(".csv"):
                content = zip_file.read(file)
                decoded_content = content.decode("utf-8")
                csvreader = csv.reader(decoded_content.splitlines(), delimiter=",")
                data = list(csvreader)
                df = pd.DataFrame(data)
                all_data.append(df)
        time.sleep(5)  # Delay to avoid overwhelming the API

# Combine data from all years and filter/sort
all_data = pd.concat(all_data)
all_data.columns = all_data.iloc[0]
all_data = all_data.iloc[1:]  # Remove the first row (which is now the header)
filtered_data = all_data[all_data["LMP_TYPE"].isin(["LMP", "MCC", "MCE", "MCL"])]
#filtered_data = filtered_data.sort_values(by=["OPR_DT", "OPR_HR"])
filtered_data = filtered_data.sort_values(by=["INTERVALSTARTTIME_GMT"])

# Write filtered data to CSV
filtered_data.to_csv("filtered_sorted_multiple_years.csv", index=False)
