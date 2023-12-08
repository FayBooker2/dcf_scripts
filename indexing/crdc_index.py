# crdc_index.py
# python script to manually index manifest while s3 storage is unable to upload data via GUI
# Note need to reformat this for efficiency including path and manifest file name

import requests
import pandas as pd
import numpy as np
import sys
import itertools as it
import asyncio
import logging

import gen3
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.tools import indexing

# Gen3 Creds
api='https://nci-crdc.datacommons.io'
cred = '/Users/faybooker/Downloads/ncicrdc.json'

auth = Gen3Auth(api, refresh_file=cred)
index=Gen3Index(auth)

# May want to re-institute logging at some point
#logging.basicConfig(filename="/Users/faybooker/Downloads/DR3.2/PDC000357-PDC000358-PDC000359-PDC000360-PDC000361-PDC000362-Files-080202023.log", level=logging.DEBUG)
#logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# Indexing File
MANIFEST = (
    "/Users/faybooker/Downloads/phs002790/Nov2023/MetaMerge20231024_NewBucket20231025_wGUID20231027_update_index20231030.tsv"
)
# This is created if we need to write out a manifest with generatied GUIDs
OUTMANIFEST = (
# Note that the outmanifest has a DCF_ prefix in the file name -- typical DCF_manifestname
    "/Users/faybooker/Downloads/phs002790/Nov2023/DCF_MetaMerge20231024_NewBucket20231025_wGUID20231027_update_index20231030.tsv"
)

if not index.is_healthy():
    print(f"uh oh! The indexing service is not healthy in the commons {api}")
    exit()

study=pd.read_csv(MANIFEST,sep='\t')

# correct for case at some point
# See if there are guids in the manifest if not, will generate them
newguids=[]
noguid=0
if 'guid' not in study.columns:
    noguid=1


study=pd.read_csv(MANIFEST,sep='\t')
for ind in study.index:
    mymd5=study['md5'][ind]
    mysize=(study['size'][ind]).item()
    myacl=study['acl'][ind].strip('"').strip('[]').strip("'")
    # set up AuthZ here
    if 'authz' in study.columns:
        myauthz=study['authz'][ind].strip('"').strip('[]').strip("'")
        myauthz=[myauthz]
    else:
        ma="/programs/"+myacl
        myauthz=[ma]
    myacl=[myacl]
    myurls=study['url'][ind].strip("[").strip("]".strip(" "))
    myurls=myurls.split(",")
    if 'filename' in study.columns:
        myfilename=(study['filename']['ind']).item()
    else:
        myfilename=myurls[0]
        # Get just the file_name
        q=myfilename.rindex("/")
        myfilename=myfilename[q+1:]
    if noguid==1:
        try:
            response = index.create_record(
            hashes={"md5": mymd5}, size=mysize, acl=myacl,urls=myurls,authz=myauthz,file_name=myfilename)
            myguid=response['did']
            newguids.append(myguid)
        except Exception as exc:
            print(
            "\nERROR ocurred when trying to create the record, you probably don't have access."
            )
    else:
        myguid=study['guid'][ind]
        try:
            response = index.create_record(
            hashes={"md5": mymd5}, size=mysize, acl=myacl,did=myguid,urls=myurls,authz=myauthz,file_name=myfilename
            )
        except Exception as exc:
            print(
            "\nERROR ocurred when trying to create the record, you probably don't have access."
            )
    print(f"Processed record {ind}: {myguid}")

if len(newguids) > 0:
    study['guid']=newguids
    study.to_csv(OUTMANIFEST, index=False, sep="\t")
