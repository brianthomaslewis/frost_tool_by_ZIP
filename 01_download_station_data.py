import os
import requests
import csv
import concurrent.futures
from bs4 import BeautifulSoup
from tqdm import tqdm


BASE_URL = 'https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/access/'
OUTPUT_FILE = './data_built/station_normals.csv'

# Define headers
headers = [
    "STATION", 
    "LATITUDE", 
    "LONGITUDE", 
    "ELEVATION", 
    "NAME", 
    "meas_flag_ANN-TMIN-PRBLST-T32FP30", # Marker to indicate if there is measurement error. Dict is below
    "ANN-TMIN-PRBLST-T32FP30", # Date for 30% probability of last frost of the Spring
    "meas_flag_ANN-TMIN-PRBFST-T32FP30", # Marker to indicate if there is measurement error. Dict is below
    "ANN-TMIN-PRBFST-T32FP30", # Date for 30% probability of first frost of the Fall
    "ANN-TAVG-NORMAL" # Average temperature for the entire year
    ]

def process_csv_link(csv_link):
    output_rows = []
    file_content = requests.get(BASE_URL + csv_link).text.splitlines()
    csv_reader = csv.DictReader(file_content)
    for row in csv_reader:
        # If the CSV is missing any of the specified columns, fill with "null"
        processed_row = {header: row.get(header, 'null') for header in headers}
        output_rows.append(processed_row)
    return output_rows

def main():
    response = requests.get(BASE_URL)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all CSV links
    csv_links = [link.get('href') for link in soup.find_all('a') if link.get('href', '').endswith('.csv')]

    # Create the CSV writer
    with open(OUTPUT_FILE, 'w', newline='') as out_csv:
        writer = csv.DictWriter(out_csv, fieldnames=headers)
        writer.writeheader()
        
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # Use tqdm to display progress for the parallel processing tasks
            for result in tqdm(executor.map(process_csv_link, csv_links), total=len(csv_links), desc="Processing CSVs"):
                for row in result:
                    writer.writerow(row)

    print("All files processed!")

if __name__ == '__main__':
    main()

## SOURCE: https://www.ncei.noaa.gov/data/normals-daily/1991-2020/doc/Readme_By-Variable_By-Station_Normals_Files.txt
# Measurement Flags
# M = Missing
# V = Year-round risk of frost-freeze; "too cold to compute"
# W = not used
# X = Nonzero value has rounded to zero
# Y = Insufficient values to perform computation
# Z = Computed valued created logical inconsistency with other values

# Completeness Flags
# S = Standard - meets WMO standards for data availability for 24 or more years 
# (missing months are filled with estimates based on surrounding stations where 
# available)
# R = Representative - meets WMO standards for data availability for 10 or more 
# years 	(missing months are filled with estimates based on surrounding stations)
# P = Provisional - meets WMO standards for data availability for 10 or more 
# years (missing months cannot be filled due to lack of surrounding stations)
# E = Estimated - meets WMO standards for data availability for 2 or more years 
# for all months (nearby stations with standard normals are available to estimate 
# normals statistically)