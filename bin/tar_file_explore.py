#%%
import pandas as pd
import os
import tarfile
import re

#%%
# path to file table
data_file = "./../data/newPLANTsdata_JASPAR2026.tsv"

#path to tar file of interest and folder for extracted files
example_tar_file = "/home/vipink/Documents/JASPAR/data/GSE155028_RAW.tar"
test_out_folder = "/home/vipink/Documents/JASPAR/data/"
#%%
def extract_tar_filenames(example_tar_file):
    file_list = []
    with tarfile.open(example_tar_file, 'r') as tar:
            for member in tar.getmembers():
                file_list.append(member.name)
    return file_list

#%%
def extract_tar_content_with_pattern(tar_file,pattern,out_folder):
    file_list = []
    r = re.compile(pattern)
    with tarfile.open(tar_file, 'r') as tar:
            for member in tar.getmembers():
                if filter(r.match, member):
                    file_list.append(member.name)
                    tar.extract(member, path=out_folder)

    return file_list

#%%
data_tbl = pd.read_csv(data_file,sep='\t')
#%%
data_tbl = (data_tbl
 .assign(GEO_ID = lambda df_: [f.split("acc=")[1] for f in df_.loc[:,'peaks available (GEO link)'].to_list()])

)

#%%
def produce_PMID(x):
    tmp_str = x['paper link'].split("/")
    if len(tmp_str) >1 and 'pubmed' in tmp_str[2]:
         return tmp_str[3]
    else:
         return x['GEO_ID']
#%%
data_tbl = data_tbl.assign(PMID = lambda df_: df_.apply(produce_PMID,axis=1))
person_data_tbl = data_tbl.query("`Assigned person` == 'Ine'")

#%%
def produce_biotin_path(x):

    if x.GEO_ID == x.PMID:
        return f"/storage/mathelierarea/raw/JASPAR2026_data_for_curation/{x.GEO_ID}"
    if x.GEO_ID != x.PMID:
        return f"/storage/mathelierarea/raw/JASPAR2026_data_for_curation/{x.PMID}/{x.GEO_ID}"

person_data_tbl = person_data_tbl.assign(data_path = lambda df_: df_.apply(produce_biotin_path,axis=1))

#%%
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
person_data_tbl = person_data_tbl.assign(folder_content = lambda df_: [ list_content(os.listdir(f),f) for f in df_.data_path.to_list()])
#%%
person_data_tbl.loc[:,['PMID','GEO_ID','data_path','folder_content']].assign(nfile = lambda df_: [len(f)for f in df_.folder_content])
#%%
r = re.compile(".*[Pp]eak.*|.*bed\\..*")

(person_data_tbl.iloc[[26],:]
#   .assign(tar_out_content = lambda df_: [extract_tar_content(os.listdir(f),f,".*[Pp]eak.*|.*bed\\..*") for f in df_.data_path.to_list()])

 .assign(peak_files = lambda df_: [list(filter(r.match, i)) for i in df_.folder_content.to_list()])
 .loc[:,['TF','PMID','GEO_ID','data_path','folder_content','peak_files']]
 .assign(npeakfile = lambda df_: [len(f)for f in df_.peak_files],
         nfile = lambda df_: [len(f)for f in df_.folder_content])
)

# %%
example_tar_file = "/storage/mathelierarea/raw/JASPAR2026_data_for_curation/37725963/GSE240962/GSE240962_RAW.tar"
test_out_folder = "/storage/mathelierarea/raw/JASPAR2026_data_for_curation/37725963/GSE240962/"
extract_tar_content_with_pattern(example_tar_file,'GSM7712994_D_Y2',test_out_folder)
# %%
