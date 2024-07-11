import datetime
import json
from unittest.mock import patch

from chalicelib_smaht.checks.audit_checks import (
    check_file_set_library_sequencing_value
)

# TO RUN THESE TESTS LOCALLY USE: pytest --noconftest


class TestAuditChecks:

    file_sets = []

    # Pytest does not discover classes with __init__. Therefore this workaround to load the data
    def load_metadata(self):
        # load it only once
        if len(self.files) > 0:
            return

        with open('tests/checks/audit_testdata.json', 'r') as f:
            data = json.load(f)
            self.file_sets = data["file_sets"]

    def search_metadata_mock_func(self, path, key):
        # The check calls this function twice. Just return [] the second time
        return self.file_sets


    @patch('dcicutils.ff_utils.search_metadata')
    def tests_check_file_set_library_sequencing_value(self, mock_search_metadata):
        self.load_metadata()
        # None of the input arguments have actually any effect, as they all go into the search_metadata query, which is mocked
        check_result = check_file_set_library_sequencing_value(1, 1, 1, None)
        import pdb; pdb.set_trace()
        assert check_result['status'] == "PASS"

        expected_check_result = {
            "file_set_1": "PASS",
            "file_set_2": "WARN",
        }

        for file_set in file_sets:
            uuid = file_sets["uuid"]
            assert file_sets["status"] == expected_check_result[uuid], f'Assertion error for file set {uuid}'
