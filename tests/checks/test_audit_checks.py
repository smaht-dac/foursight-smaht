"""Tests for check_suggested_enum_values and its helpers."""
import copy
from unittest.mock import MagicMock, patch

from chalicelib_smaht.checks import audit_checks as ac
from chalicelib_smaht.checks.helpers import constants


PROPERTIES = {
    "flow_cell": {"type": "string", "suggested_enum": ["A", "B"]},
    "enzymes": {
        "type": "array",
        "items": {"type": "string", "suggested_enum": ["X"], "ignored_enum": ["Y"]},
    },
    "call_set": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "suggested_enum": ["SNV V1"]},
            "category": {"type": "string", "enum": ["Somatic"]},
        },
    },
    "parts": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {"kind": {"type": "string", "suggested_enum": ["K"]}},
        },
    },
    "computed": {"type": "string", "calculatedProperty": True, "suggested_enum": ["C"]},
    "plain": {"type": "string"},
}


class TestFindSuggestedEnumFields:
    def test_finds_all_field_shapes(self):
        fields = dict(ac._find_suggested_enum_fields(PROPERTIES))
        assert fields == {
            "flow_cell": {"A", "B"},
            "enzymes": {"X", "Y"},
            "call_set.name": {"SNV V1"},
            "parts.kind": {"K"},
        }

    def test_does_not_mutate_schema(self):
        original = copy.deepcopy(PROPERTIES)
        ac._find_suggested_enum_fields(PROPERTIES)
        assert PROPERTIES == original


class TestGetValuesAtPath:
    def test_scalar(self):
        assert ac._get_values_at_path({"a": "x"}, "a") == ["x"]

    def test_list(self):
        assert ac._get_values_at_path({"a": ["x", "", "y"]}, "a") == ["x", "y"]

    def test_nested_object(self):
        assert ac._get_values_at_path({"a": {"b": "x"}}, "a.b") == ["x"]

    def test_list_of_objects(self):
        item = {"a": [{"b": "x"}, {"b": ["y", "z"]}, {"c": "q"}]}
        assert ac._get_values_at_path(item, "a.b") == ["x", "y", "z"]

    def test_missing_intermediate(self):
        assert ac._get_values_at_path({"a": None}, "a.b") == []
        assert ac._get_values_at_path({}, "a.b") == []


def _run_check(schemas, search_results):
    connection = MagicMock()
    with patch.object(ac.ff_utils, "get_metadata", return_value=schemas), \
            patch.object(ac.ff_utils, "search_metadata",
                         side_effect=lambda q, key: search_results.get(q, [])) as search, \
            patch.object(ac.time, "sleep"), \
            patch.object(ac, "CheckResult", side_effect=lambda conn, name: MagicMock()):
        # bypass the check_function decorator, which records results to S3
        check = ac.check_suggested_enum_values.__wrapped__(connection)
    return check, search


class TestCheckSuggestedEnumValues:
    SCHEMAS = {
        "File": {"isAbstract": True, "properties": {"enzymes": PROPERTIES["enzymes"]}},
        "Library": {"properties": {"enzymes": PROPERTIES["enzymes"]}},
    }
    QUERY = "search/?type=Library&enzymes!=No+value&field=enzymes"

    def test_reports_new_value_mixed_with_known_and_skips_abstract(self):
        results = {self.QUERY: [{"enzymes": ["X", "NEW"]}, {"enzymes": ["NEW", "Y"]}]}
        check, search = _run_check(self.SCHEMAS, results)
        assert check.status == constants.CHECK_WARN
        assert check.full_output == [
            {"item_type": "Library", "field": "enzymes", "new_values": {"NEW": 2}}
        ]
        assert check.brief_output == ["Library: enzymes"]
        queried = [call.args[0] for call in search.call_args_list]
        assert queried == [self.QUERY]

    def test_pass_when_no_new_values(self):
        results = {self.QUERY: [{"enzymes": ["X"]}, {"enzymes": ["Y"]}]}
        check, _ = _run_check(self.SCHEMAS, results)
        assert check.status == constants.CHECK_PASS
        assert check.full_output == []
