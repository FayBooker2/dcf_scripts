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
from datetime import datetime

import gen3
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.tools import indexing


# Gen3 Creds
api='https://nci-crdc.datacommons.io'
cred = '/Users/faybooker/Downloads/ncicrdc.json'

auth = Gen3Auth(api, refresh_file=cred)
index = Gen3Index(auth)

# May want to re-institute logging at some point
#logging.basicConfig(filename="/Users/faybooker/Downloads/DR3.2/PDC000357-PDC000358-PDC000359-PDC000360-PDC000361-PDC000362-Files-080202023.log", level=logging.DEBUG)
#logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# Indexing File
MANIFEST = (
  "/Users/faybooker/Downloads/iodc/Mar2024/iodc_10021_crc_manifest.tsv"
)

# This is created if we need to write out a manifest with generatied GUIDs, automatically:
# Splits MANIFEST to extract the directory path and filename.
# Checks if the filename starts with "DCF_" and prepends it if not.
# Removes the existing file extension and appends the new suffix "_indexed_YYYMMDD.tsv".
# Joins everything back together to form the OUTMANIFEST path.
# OUTMANIFEST = "/".join(MANIFEST.rsplit('/', 1)[0:-1]) + "/" + ("DCF_" if not MANIFEST.split('/')[-1].startswith("DCF_") else "") + MANIFEST.split('/')[-1].rsplit('.', 1)[0] + "_indexed_" + datetime.now().strftime("%Y%m%d") + ".tsv"
OUTMANIFEST = (
# Note that the outmanifest has a DCF_ prefix in the file name -- typical DCF_manifestname
  "/Users/faybooker/Downloads/iodc/Mar2024/DCF_iodc_10021_crc_manifest_wGuid.tsv"
)

# Check to ensure this does not overwrite existing logs
PROCESSING_LOG = f"{MANIFEST.rsplit('/', 1)[0]}/DCF_processing_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROCESSING_LOG),  # Log to file
        logging.StreamHandler(sys.stdout)     # Log to console
    ]
)

if not index.is_healthy():
    logging.error(f"uh oh! The indexing service is not healthy in the commons {api}")
    sys.exit(1)

# Function to determine the delimiter based on file extension
def get_delimiter(file_name):
    if file_name.endswith('.csv'):
        return ','
    elif file_name.endswith('.tsv'):
        return '\t'
    else:
        # Default delimiter can be set here if file extension is neither .csv nor .tsv
        return '\t'

# Load the Dataframe
study = pd.read_csv(MANIFEST, sep=get_delimiter(MANIFEST))

# correct for case at some point
# See if there are guids in the manifest if not, will generate them
newguids=[]
noguid=0
if 'guid' not in study.columns:
    noguid=1

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
    myurls=study['urls'][ind].strip("[").strip("]".strip(" "))
    myurls=myurls.split(",")
    if 'file_name' in study.columns:
        myfilename=study['file_name'][ind]
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
            logging.info(f"Processed record {ind}: {myguid}")
        except Exception as exc:
            logging.error("Error occurred when trying to create the record", exc_info=True)

    else:
        myguid=study['guid'][ind]
        try:
            response = index.create_record(
            hashes={"md5": mymd5}, size=mysize, acl=myacl,did=myguid,urls=myurls,authz=myauthz,file_name=myfilename
            )
            logging.info(f"Processed record {ind}: {myguid}")

        except Exception as exc:
            logging.error("Error occurred when trying to create the record", exc_info=True)


if len(newguids) > 0:
    study['guid']=newguids
    study.to_csv(OUTMANIFEST, index=False, sep="\t")
