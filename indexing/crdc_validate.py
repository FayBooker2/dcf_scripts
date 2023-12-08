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
from gen3.tools.indexing import is_valid_manifest_format
import gen3
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
from gen3.tools import indexing

#logging.basicConfig(filename="/Users/faybooker/Downloads/DR3.2validate_20230802.log", level=logging.DEBUG)
#logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

IN_MANIFEST = (
    "/Users/faybooker/Downloads/phs002790/Nov2023/MetaMerge20231024_NewBucket20231025_wGUID20231027_update_index20231030.tsv"
)
OUT_MANIFEST= (
    "/Users/faybooker/Downloads/phs002790/Nov2023/DCF_generatedmanifest_20231208.tsv"
)

api='https://nci-crdc.datacommons.io'
cred = '/Users/faybooker/Downloads/ncicrdc.json'

auth = Gen3Auth(api, refresh_file=cred)
index=Gen3Index(auth)

# manifest with guids

if not index.is_healthy():
    print(f"uh oh! The indexing service is not healthy in the commons {api}")
    exit()

study=pd.read_csv(IN_MANIFEST,sep='\t')

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
    if (ind%10 == 0):
        print("Processing record: ", ind)
    myguid=study['guid'][ind]
    mymd5=study['md5'][ind]
    mysize=(study['size'][ind]).item()
    myacl=[study['acl'][ind].strip('"').strip('[]').strip("'")]
    myauthz=[study['authz'][ind].strip('"').strip('[]').strip("'")]
    myfilename=[study['file_name'][ind].strip('"').strip('[]').strip("'")]
    #myacl=[myacl]
    #myacl=myacl.strip(']')
    myurl=[study['urls'][ind].strip('"')]
    v=index.get(guid=myguid)
    bad=0
    # shoud loop this but...
    if mymd5 != v['hashes']['md5']:  bad=bad+1; print(myguid," MD5 error")
    if mysize != v['size']: bad=bad+1; print(myguid, " size error")
    if myacl != v['acl']: bad=bad+1; print(myguid, "acl error")
    if myurl != v['urls']:  bad=bad+1; print(myguid, "urls error")
    if bad> 0:
        print("PROBLEMS WITH VALIDATION")
        #print(myguid, mymd5, mysize, myacl, myurl)
        #print(myguid, v['hashes']['md5'], v['acl'], v['urls'])
        print(myguid)
        print(myacl, v['acl'])
        print(myurl, v['urls'])
        toterrs=toterrs+bad
        badrecs=badrecs+1
    else:
        thisguid.append(myguid)
        thismd5.append(mymd5)
        thissize.append(mysize)
        thisacl.append(myacl)
        thisauthz.append(myauthz)
        thisurl.append(myurl)
        thisfilename.append(myfilename)
    if ind == study.index[-1]:
        break
print("Total Recods =", study.index)
print("Total Errors =",toterrs)
print("Bad Records = ",badrecs)

# produce output manifest
dict={'guid':thisguid, 'md5': thismd5, 'size': thissize, 'acl': thisacl, 'urls':thisurl, 'authz': thisauthz, 'filename': thisfilename}
df=pd.DataFrame(dict)
df.to_csv(OUT_MANIFEST, index=False, sep="\t")
