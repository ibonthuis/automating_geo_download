#%%
import pandas as pd
import os
import tarfile
import re

#%%
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
    with tarfile.open(tar_file, 'r') as tar:
            for member in tar.getmembers():
                if re.search(pattern, member.name):
                    file_list.append(member.name)
                    tar.extract(member, path=out_folder)

    return file_list

# %%
extract_tar_content_with_pattern(example_tar_file,'GSM4693877',test_out_folder)
# %%
