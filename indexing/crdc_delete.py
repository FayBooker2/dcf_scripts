#  crdc_delete.py
#  Script used to fetch indexd objects
#   and delete mis-specified guids submitted on 3/10
#

# default packages
endpoint='https://nci-crdc.datacommons.io'
#cred = '/Users/faybooker/Downloads/ncicrdc.json'
cred = '/Users/faybooker/Downloads/ncicrdc.json'
import requests
import pandas as pd
import sys

import gen3
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.tools import indexing

auth = Gen3Auth(endpoint, refresh_file=cred)
index=Gen3Index(auth)

#study=pd.read_csv("/Users/faybooker/Downloads/aml/AML_Ex_Vivo_Drug_Response_manifest_correctedGUIDs (1).tsv",sep='\t')
study=pd.read_csv("/Users/faybooker/Downloads/phs001437/GuidsOnly.tsv",sep='\t')
guids=study["guid"]
# initialize i
i=1
print("Number of items:", len(guids))
for guid in guids:
    if (i%10 == 0):
         print(i," ",guid)
    i=i+1
    Gen3Index.delete_record(index,guid)
