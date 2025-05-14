#%%
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os

#%%
def download_file(url, local_folder):
    """Download a single file from a URL."""
    # Extract the filename from the URL
    if not os.path.exists(local_folder):
            os.makedirs(local_folder)
    filename = url.split('/')[-1]
    local_filepath = os.path.join(local_folder, filename)
    print(f"Downloading {filename}...")
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses

    # Save the file
    with open(local_filepath, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded: {filename}")

#%%
data_file = "./../data/newPLANTsdata_JASPAR2026.tsv"
data_tbl = pd.read_csv(data_file,sep='\t')

#%%
data_tbl.assign(GEO_ID = lambda df_: [f.split("acc")[0] for f in df_.loc[:,'peaks available (GEO link)'].to_list()])
#%%
GSE_id = "GSE104038"
url = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE104nnn/GSE104038/suppl/"
local_folder = f"./data/{GSE_id}/"

response = requests.get(url)

soup = BeautifulSoup(response.text, 'html.parser')


for link in  soup.find_all('a'):
    tmp_file = link.get('href')
    if  GSE_id in tmp_file and "/" not in tmp_file:
        download_file(url+tmp_file, local_folder)
        print((url+tmp_file).split('/')[-1])