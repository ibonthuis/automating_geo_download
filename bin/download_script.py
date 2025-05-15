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
data_tbl = (data_tbl
 .assign(GEO_ID = lambda df_: [f.split("acc=")[1] for f in df_.loc[:,'peaks available (GEO link)'].to_list()])

)

#%%
data_tbl = data_tbl.assign(url = lambda df_: ["https://ftp.ncbi.nlm.nih.gov/geo/series/"+g[:-3]+'nnn/'+g+"/suppl/" for g in df_.GEO_ID.to_list()])

#%% 
def produce_PMID(x):
    tmp_str = x['paper link'].split("/")
    if len(tmp_str) >1:
         return tmp_str[3]
    else:
         return x['GEO_ID']
#%%
data_tbl = data_tbl.assign(PMID = lambda df_: df_.apply(produce_PMID,axis=1))

#%%
person_data_tbl = data_tbl.query("`Assigned person` == 'Vipin'")
#%%

for idx in range(person_data_tbl.shape[0]):

    tmp_row = person_data_tbl.iloc[idx,:]
    tmp_url = tmp_row.url
    tmp_geo_ID = tmp_row.GEO_ID
    tmp_PMID = tmp_row.PMID
    local_folder = f"./data/{tmp_PMID}/{tmp_geo_ID}"

    response = requests.get(tmp_url)

    soup = BeautifulSoup(response.text, 'html.parser')

    print(f"Downloading files for{tmp_geo_ID}")
    for link in  soup.find_all('a'):
        tmp_file = link.get('href')
        if  tmp_geo_ID in tmp_file and "/" not in tmp_file:
            download_file(tmp_url+tmp_file, local_folder)
            print((tmp_url+tmp_file).split('/')[-1])