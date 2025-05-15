#%%
import pandas as pd
import os
import tarfile

#%%
#path to tar file of interest
example_tar_file = "/home/vipink/Documents/JASPAR/data/GSE155028_RAW.tar"

#%%
def extract_tar_filenames(example_tar_file):
    file_list = []
    with tarfile.open(example_tar_file, 'r') as tar:
            for member in tar.getmembers():
                file_list.append(member.name)
    return file_list
# %%
extract_tar_filenames(example_tar_file)
# %%
