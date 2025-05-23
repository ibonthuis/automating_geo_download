#%%
import pandas as pd
import os
import tarfile
import re
from bs4 import BeautifulSoup

#%%
# path to file table
data_file = "./../data/newPLANTsdata_JASPAR2026.tsv"

def extract_tar_filenames(example_tar_file):
    file_list = []
    with tarfile.open(example_tar_file, 'r') as tar:
            for member in tar.getmembers():
                file_list.append(member.name)
    return file_list

def extract_tar_content_with_pattern(tar_file,pattern,out_folder):
    file_list = []
    r = re.compile(pattern)
    with tarfile.open(tar_file, 'r') as tar:
            for member in tar.getmembers():
                if filter(r.match, member):
                    file_list.append(member.name)
                    tar.extract(member, path=out_folder)

    return file_list

def produce_PMID(x):
    tmp_str = x['paper link'].split("/")
    if len(tmp_str) >1 and 'pubmed' in tmp_str[2]:
         return tmp_str[3]
    else:
         return x['GEO_ID']

def produce_biotin_path(x):

    if x.GEO_ID == x.PMID:
        return f"/storage/mathelierarea/raw/JASPAR2026_data_for_curation/{x.GEO_ID}"
    if x.GEO_ID != x.PMID:
        return f"/storage/mathelierarea/raw/JASPAR2026_data_for_curation/{x.PMID}/{x.GEO_ID}"

def list_content(file_list,path):
    all_content = []
    for i in file_list:
          if re.search(r'tar',i):
                 all_content.extend(extract_tar_filenames(path+"/"+i))
          else:
                all_content.append(i)
    return all_content

def extract_tar_content(file_list,path,pattern):
    all_content = []
    for i in file_list:
          if re.search(r'tar',i):
                 all_content.extend(extract_tar_content_with_pattern(path+"/"+i,pattern,path))
          else:
                all_content.append(i)
    return all_content
#%%
data_tbl = pd.read_csv(data_file,sep='\t')

data_tbl = (data_tbl
 .assign(GEO_ID = lambda df_: [f.split("acc=")[1] for f in df_.loc[:,'peaks available (GEO link)'].to_list()])

)

data_tbl = data_tbl.assign(PMID = lambda df_: df_.apply(produce_PMID,axis=1))

# %%
person_data_tbl = data_tbl.query("`Assigned person` == 'Vipin'")

person_data_tbl = person_data_tbl.assign(data_path = lambda df_: df_.apply(produce_biotin_path,axis=1))
person_data_tbl = person_data_tbl.assign(folder_content = lambda df_: [ list_content(os.listdir(f),f) for f in df_.data_path.to_list()])

# %%
r = re.compile(".*[Pp]eak.*|.*bed\\..*")

(person_data_tbl
 .assign(peak_files = lambda df_: [list(filter(r.match, i)) for i in df_.folder_content.to_list()])
 .loc[:,['TF','Genome assembly','data type','PMID','GEO_ID','data_path','folder_content','peak_files']]
 .assign(npeakfile = lambda df_: [len(f)for f in df_.peak_files],
         nfile = lambda df_: [len(f)for f in df_.folder_content])
 .query("npeakfile > 0")
 .assign(files_in_folder = lambda df_: [list(filter(r.match,os.listdir(f))) for f in df_.data_path.to_list()])


)


# %%
tmp_file = "./../data/tmp_file.txt"
delimiters = r"[_\.]+"

(pd.DataFrame({'file':os.listdir("/storage/mathelierarea/raw/JASPAR2026_data_for_curation/38962989/GSE262337")})
 .assign(TF = lambda df_: [re.split(delimiters,f)[1] for f in df_.file.to_list()])
  .TF.to_csv(tmp_file,index=False)
)
# %%
os.getcwd()
# %%
'''
Special case for PMID:38167709: 
The file name don't indicate the actual TF probed in the experiment, 
instead we need to go back in the GEO website and fish out the table in the HTML file: 
- https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE201700
- https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE229946
and fetch the table with all the constitutive GSM links and annotations (saved as )


'''
#%%
html_tbl_dict = {
    "GSE201700":"./../data/GSE201700_GEO.html",
    "GSE229946":"./../data/GSE229946_GEO.html"
}
#%%
def get_HTML_tbl_content(HTML_file):
    tmp_html_file = open(HTML_file)
    soup = BeautifulSoup(tmp_html_file, 'html.parser')
    rows = soup.find('tbody').find_all('tr')

    # Extract the content from each row
    data = []
    for row in rows:
        cols = row.find_all('td')
        # Get text from each column and store the tuple in data
        col_data = [col.get_text(strip=True) for col in cols]
        data.append(col_data)
    return pd.DataFrame(data)

# %%
tmp_GEO = "GSE201700"
tmp_file = "./../data/tmp_file.txt"

tmp_GEO_tbl = get_HTML_tbl_content(html_tbl_dict[tmp_GEO])

(pd.DataFrame({'file':os.listdir(f"/storage/mathelierarea/raw/JASPAR2026_data_for_curation/38167709/{tmp_GEO}")})
  .assign(GSM = lambda df_: [re.split(delimiters,f)[0] for f in df_.file.to_list()])
  .merge(pd.DataFrame(tmp_GEO_tbl).rename(columns={0:'GSM',1:'TF'}),how='outer')
  .dropna().assign(GEO_ID = tmp_GEO)
  .merge(person_data_tbl .loc[:,['Genome assembly','data type','PMID','GEO_ID','data_path']],on=['GEO_ID'])
  .assign(peak_file = lambda df_: df_.data_path.astype(str) + df_.file.astype(str))
).TF.to_csv(tmp_file,index=False)
# %%
