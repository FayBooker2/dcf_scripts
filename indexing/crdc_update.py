# crdc_update.py
# python script to manually update records in CRDC indexd
#    This version manually tests the format of the command, automation to come validate_manifest_format
#

import requests
import pandas as pd
import sys
import itertools as it
import asyncio
import logging

import gen3
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.tools import indexing


api='https://nci-crdc.datacommons.io'
cred = '/Users/faybooker/Downloads/ncicrdc.json'

auth = Gen3Auth(api, refresh_file=cred)
index=Gen3Index(auth)

logging.basicConfig(filename="/Users/faybooker/Downloads/CDS_Buckets/phs002432_update_20230721.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
# manifest with guids
MANIFEST = (
#    "/Users/faybooker/Downloads/phs002790/June2023/cds_MetaMerge20230525_NewBucket20230525_index20230526.tsv"
     "/Users/faybooker/Downloads/CDS_Buckets/phs002432_newBucket20230719.tsv"
    )

if not index.is_healthy():
    print(f"uh oh! The indexing service is not healthy in the commons {api}")
    exit()

# Manifest based updates here
study=pd.read_csv(MANIFEST,sep='\t')
# correct for case at some point
for ind in study.index:
    # print(study['guid'][ind], study['url'][ind])
    if (ind%10 == 0):
         print(ind," ",study['guid'][ind])
    myguid=study['guid'][ind]
    myurl=study['url'][ind].strip('"')
    myurls=[myurl]
    newurlmd={myurl: {} }
    #newacl=study['acl'][ind].strip('"').strip('[]').strip("'")
    #newacl=[newacl]
    #print("URL: ",myurls)
    #print("MD: ", newurlmd)
    print("Update GUID record:", myguid)
    try:
       #response = index.update_record(myguid, urls=newurl, urls_metadata=newurlmd)
       response = index.update_record(myguid, urls=myurls, urls_metadata=newurlmd)
       #print(index.get(guid=myguid))
    except Exception as exc:
       print(
         "\nERROR ocurred when trying to update the record, you probably don't have access."
       )
