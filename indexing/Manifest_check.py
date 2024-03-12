# Manifest_check.py
# This script is used for very basic manifest validation is it missing some features that we want to add
# such as:  GUID prefix checking, strict enforcement of column headers, authz format, and a few others.

import sys
import logging

from gen3.tools.indexing import is_valid_manifest_format

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

MANIFEST = (
  "/Users/faybooker/Downloads/phs002431/um_phs002431_ccdi_metadata_v28_v1.7.1_qc20240214_Updater_v1.7.2_2024-02-14_CatchERR20240214_Index20240214.tsv"
)


def main():

    is_valid_manifest_format(
        manifest_path=MANIFEST,
        column_names_to_enums=None,
        allowed_protocols=["s3", "gs"],
        allow_base64_encoded_md5=False,
        error_on_empty_url=True,
        line_limit=None,
    )


if __name__ == "__main__":
    main()
