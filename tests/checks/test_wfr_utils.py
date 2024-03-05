from unittest.mock import patch
import json

from chalicelib_smaht.checks.helpers.wfr_utils import (
    get_latest_md5_mwf,
    paginate_list
)

# TO RUN THESE TESTS LOCALLY USE: pytest --noconftest

class TestWfrUtils:

    test_metadata = None

    def load_metadata(self):
        with open('tests/checks/wfr_testdata.json', 'r') as f:
            self.test_metadata = json.load(f)

    def search_metadata_md5_mwf_single(self, query, key):
        return self.test_metadata["md5_mwf_single"]
    
    def search_metadata_md5_mwf_multiple(self, query, key):
        return self.test_metadata["md5_mwf_multiple"]


    @patch('dcicutils.ff_utils.search_metadata')
    def test_get_lastest_md5_mwf(self, mock_search_metadata):
        self.load_metadata()
        mock_search_metadata.side_effect = self.search_metadata_md5_mwf_single
        mwf = get_latest_md5_mwf(None)
        assert mwf['uuid'] == "mwf_1"

        mock_search_metadata.side_effect = self.search_metadata_md5_mwf_multiple
        mwf = get_latest_md5_mwf(None)
        assert mwf['uuid'] == "mwf_2"

    def test_paginate_list(self):
        list = [1, 2, 3, 4, 5, 6, 7, 8, 9]
        p_list = paginate_list(list, 4)
        assert p_list == [[1, 2, 3, 4], [5, 6, 7, 8], [9]]