# crdc_validate.py
# script used to validate Indexing
# input is source Manifest
# output is dcf generated manifest

import pandas as pd
import numpy as np
import sys
import itertools as it
import asyncio
import logging
from datetime import datetime

from gen3.tools.indexing import is_valid_manifest_format
import gen3
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.tools import indexing

#logging.basicConfig(filename="/Users/faybooker/Downloads/DR3.2validate_20230802.log", level=logging.DEBUG)
#logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

IN_MANIFEST = (
  "/Users/faybooker/Downloads/iodc/Mar2024/iodc_10021_crc_manifest.tsv"
)
# Splits MANIFEST to extract the directory path and filename.
# Checks if the filename starts with "DCF_" and prepends it if not.
# Removes the existing file extension and appends the new suffix "_indexed_YYYMMDD.tsv".
# Joins everything back together to form the OUTMANIFEST path.
OUT_MANIFEST = "/".join(IN_MANIFEST.rsplit('/', 1)[0:-1]) + "/" + ("DCF_" if not IN_MANIFEST.split('/')[-1].startswith("DCF_") else "") + IN_MANIFEST.split('/')[-1].rsplit('.', 1)[0] + "_validated_" + datetime.now().strftime("%Y%m%d") + ".tsv"
# OUT_MANIFEST= (
#     "/Users/jdorsheimer/Projects/DCF/phs002050/DCF_generatedmanifest_20240116.tsv"
# )

# Check to ensure this does not overwrite existing logs
PROCESSING_LOG = f"{IN_MANIFEST.rsplit('/', 1)[0]}/DCF_validation_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROCESSING_LOG),  # Log to file
        logging.StreamHandler(sys.stdout)     # Log to console
    ]

api='https://nci-crdc.datacommons.io'
cred = '/Users/faybooker/Downloads/ncicrdc.json'

auth = Gen3Auth(api, refresh_file=cred)
index=Gen3Index(auth)

# manifest with guids

if not index.is_healthy():
    print(f"uh oh! The indexing service is not healthy in the commons {api}")
    exit()

# Function to determine the delimiter based on file extension
def get_delimiter(file_name):
    if file_name.endswith('.csv'):
        return ','
    elif file_name.endswith('.tsv'):
        return '\t'
    else:
        # Default delimiter can be set here if file extension is neither .csv nor .tsv
        return '\t'


study=pd.read_csv(IN_MANIFEST, sep=get_delimiter(IN_MANIFEST))

# inialize variables
thisguid=[]
thismd5=[]
thissize=[]
thisacl=[]
thisurl=[]
thisauthz=[]
thisfilename=[]
toterrs=0
badrecs=0

for ind in study.index:
    try:
        if (ind%10 == 0):
            logging.info(f"Processing record: {ind}")
        myguid=study['guid'][ind]
        mymd5=study['md5'][ind]
        mysize=(study['size'][ind]).item()
        myacl=[study['acl'][ind].strip('"').strip('[]').strip("'")]
        myauthz=[study['authz'][ind].strip('"').strip('[]').strip("'")]
        # Add an if/else statement for file_name
        #myfilename=[study['file_name'][ind].strip('"').strip('[]').strip("'")]
        #myacl=[myacl]
        #myacl=myacl.strip(']')
        # Handle either 'url' or 'urls'
        url_column = 'urls' if 'urls' in study.columns else 'url'
        myurl = [study[url_column][ind].strip('"')]
        v=index.get(guid=myguid)
        bad=0
        # shoud loop this but...
        if mymd5 != v['hashes']['md5']:  bad=bad+1; logging.error(f"{myguid} MD5 error")
        if mysize != v['size']: bad=bad+1; logging.error(f"{myguid} size error")
        if myacl != v['acl']: bad=bad+1; logging.error(f"{myguid} acl error")
        if myurl != v['urls']:  bad=bad+1; logging.error(f"{myguid} urls error")
        if bad> 0:
            logging.warning(f"PROBLEMS WITH VALIDATION for {myguid}")
            toterrs=toterrs+bad
            badrecs=badrecs+1
        else:
            thisguid.append(myguid)
            thismd5.append(mymd5)
            thissize.append(mysize)
            thisacl.append(myacl)
            thisauthz.append(myauthz)
            thisurl.append(myurl)
            #thisfilename.append(myfilename)
    except Exception as exc:
        logging.error(f"Error occurred when processing record {ind}", exc_info=True)
        badrecs += 1
logging.info(f"Total Records = {len(study.index)}")
logging.info(f"Total Errors = {toterrs}")
logging.info(f"Bad Records = {badrecs}")
fsize=sum(thissize)*1e-9
logging.info(f"FileSize (GB) = {fsize}")

url_key = 'urls' if 'urls' in study.columns else 'url'


# produce output manifest
dict={'guid':thisguid, 'md5': thismd5, 'size': thissize, 'acl': thisacl, url_key:thisurl, 'authz': thisauthz} #, 'filename': thisfilename}
df=pd.DataFrame(dict)
df.to_csv(OUT_MANIFEST, index=False, sep="\t")
