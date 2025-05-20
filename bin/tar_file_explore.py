#%%
import pandas as pd
import os
import tarfile
import re

#%%
#path to tar file of interest and folder for extracted files
example_tar_file = "/storage/kuijjerarea/ine/jaspar_geo_downloads_plants/data/insilicoplants/GSE193400/GSE193400_RAW.tar"
test_out_folder = "/div/pythagoras/u1/inebont/jaspar_data_collection/data/insilicoplants/GSE193400"
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
    with tarfile.open(tar_file, 'r') as tar:
            for member in tar.getmembers():
                if re.search(pattern, member.name):
                    file_list.append(member.name)
                    tar.extract(member, path=out_folder)

    return file_list
#%%
extract_tar_filenames(example_tar_file)

# %%
extract_tar_content_with_pattern(example_tar_file,'.narrowPeak.gz',test_out_folder)
# %%
