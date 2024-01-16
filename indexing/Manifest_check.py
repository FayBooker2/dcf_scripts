# Manifest_check.py
# This script is used for very basic manifest validation is it missing some features that we want to add
# such as:  GUID prefix checking, strict enforcement of column headers, authz format, and a few others.

import sys
import logging

from gen3.tools.indexing import is_valid_manifest_format

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

MANIFEST = (
    "/Users/faybooker/Downloads/phs002517/Dec2023/phs002517_CHOP_CCDI_Submission_v1.7.1_Updater20231128_CatchERR20231128_ONLY_IMAGING_CDS20231129_index20231129.tsv"
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
