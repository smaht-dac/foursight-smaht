"""Tests for TPC tissue sample metadata synchronization."""
from unittest.mock import patch, MagicMock

from chalicelib_smaht.checks.wrangler_checks import (
    _build_tpc_field_patch,
    _process_tpc_sync_candidates,
)


class TestTPCMetadataSync:
    """Test suite for TPC → GCC tissue sample metadata synchronization."""

    # -------------------------------------------------------------------------
    # _build_tpc_field_patch unit tests
    # -------------------------------------------------------------------------

    def test_build_patch_core_size_copy_when_gcc_lacks_it(self):
        """GCC lacks core_size: copy from TPC."""
        tpc = {"core_size": "5mm"}
        gcc = {}
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        assert patch == {"core_size": "5mm"}
        assert mismatch is None

    def test_build_patch_core_size_matching_values(self):
        """Both have core_size and they match: no action needed."""
        tpc = {"core_size": "5mm"}
        gcc = {"core_size": "5mm"}
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        assert patch is None
        assert mismatch is None

    def test_build_patch_core_size_mismatch(self):
        """Both have core_size and they differ: return mismatch reason."""
        tpc = {"core_size": "5mm"}
        gcc = {"core_size": "3mm"}
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        assert patch is None
        assert mismatch is not None
        assert "core_size mismatch" in mismatch
        assert "TPC=5mm" in mismatch
        assert "GCC=3mm" in mismatch

    def test_build_patch_gcc_has_core_size_tpc_does_not(self):
        """Regression: GCC has core_size, TPC lacks it: preserve GCC, no mismatch."""
        tpc = {}
        gcc = {"core_size": "3mm"}
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        assert patch is None
        assert mismatch is None

    def test_build_patch_existing_preservation_type(self):
        """Existing field: preservation_type copied when GCC lacks it."""
        tpc = {"preservation_type": "Frozen"}
        gcc = {}
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        assert patch == {"preservation_type": "Frozen"}
        assert mismatch is None

    def test_build_patch_existing_description(self):
        """Existing field: description merged when both present."""
        tpc = {"description": "TPC note"}
        gcc = {"description": "GCC note"}
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        assert patch == {"description": "GCC: GCC note; TPC: TPC note"}
        assert mismatch is None

    def test_build_patch_existing_processing_notes(self):
        """Existing field: processing_notes copied when only TPC has it."""
        tpc = {"processing_notes": "TPC processing"}
        gcc = {}
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        assert patch == {"processing_notes": "TPC: TPC processing"}
        assert mismatch is None

    def test_build_patch_core_size_plus_existing_fields(self):
        """core_size copy combined with existing field synchronization."""
        tpc = {
            "core_size": "5mm",
            "preservation_type": "Frozen",
            "description": "TPC desc",
        }
        gcc = {"description": "GCC desc"}
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        assert patch == {
            "core_size": "5mm",
            "preservation_type": "Frozen",
            "description": "GCC: GCC desc; TPC: TPC desc",
        }
        assert mismatch is None

    def test_build_patch_core_size_mismatch_excludes_all_fields(self):
        """core_size mismatch excludes sample entirely, even if other fields patchable."""
        tpc = {
            "core_size": "5mm",
            "preservation_type": "Frozen",
        }
        gcc = {
            "core_size": "3mm",
            # GCC lacks preservation_type, so it would normally be copied
        }
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        # Sample excluded entirely due to mismatch
        assert patch is None
        assert mismatch is not None
        assert "core_size mismatch" in mismatch

    def test_build_patch_no_fields_to_sync(self):
        """No fields to sync: returns None patch, no mismatch."""
        tpc = {"some_other_field": "value"}
        gcc = {"some_other_field": "value"}
        patch, mismatch = _build_tpc_field_patch(tpc, gcc)
        assert patch is None
        assert mismatch is None

    # -------------------------------------------------------------------------
    # _process_tpc_sync_candidates integration tests
    # -------------------------------------------------------------------------

    @patch("chalicelib_smaht.checks.wrangler_checks._get_tpc_sample")
    def test_process_candidates_normal_copy(self, mock_get_tpc):
        """Process identifies sample needing core_size copy."""
        candidates = [{"uuid": "gcc-1", "external_id": "SAMPLE001"}]
        mock_get_tpc.return_value = {"core_size": "5mm"}

        to_patch, mismatched = _process_tpc_sync_candidates(candidates, None)

        assert len(to_patch) == 1
        assert to_patch[0]["patch"] == {"core_size": "5mm"}
        assert len(mismatched) == 0

    @patch("chalicelib_smaht.checks.wrangler_checks._get_tpc_sample")
    def test_process_candidates_matching_values(self, mock_get_tpc):
        """Process handles samples with matching core_size (no patch needed)."""
        candidates = [
            {"uuid": "gcc-1", "external_id": "SAMPLE001", "core_size": "5mm"}
        ]
        mock_get_tpc.return_value = {"core_size": "5mm"}

        to_patch, mismatched = _process_tpc_sync_candidates(candidates, None)

        # Sample included in to_patch with empty patch dict (no fields to sync)
        assert len(to_patch) == 1
        assert to_patch[0]["patch"] == {}
        assert len(mismatched) == 0

    @patch("chalicelib_smaht.checks.wrangler_checks._get_tpc_sample")
    def test_process_candidates_mismatch_warning(self, mock_get_tpc):
        """Process warns about core_size mismatch and excludes sample from to_patch."""
        candidates = [
            {"uuid": "gcc-1", "external_id": "SAMPLE001", "core_size": "3mm"}
        ]
        mock_get_tpc.return_value = {"core_size": "5mm"}

        to_patch, mismatched = _process_tpc_sync_candidates(candidates, None)

        assert len(to_patch) == 0
        assert len(mismatched) == 1
        mismatch = mismatched[0]
        assert mismatch["uuid"] == "gcc-1"
        assert mismatch["external_id"] == "SAMPLE001"
        assert "core_size mismatch" in mismatch["reason"]
        assert "TPC=5mm" in mismatch["reason"]
        assert "GCC=3mm" in mismatch["reason"]

    @patch("chalicelib_smaht.checks.wrangler_checks._get_tpc_sample")
    def test_process_candidates_mixed_patchable_and_mismatched(self, mock_get_tpc):
        """Process handles mix of patchable samples and mismatched samples."""
        candidates = [
            {"uuid": "gcc-1", "external_id": "SAMPLE001"},  # missing core_size
            {"uuid": "gcc-2", "external_id": "SAMPLE002", "core_size": "3mm"},  # mismatch
            {"uuid": "gcc-3", "external_id": "SAMPLE003", "core_size": "5mm"},  # match
        ]

        def get_tpc_side_effect(external_id, ff_keys):
            return {"core_size": "5mm"}

        mock_get_tpc.side_effect = get_tpc_side_effect

        to_patch, mismatched = _process_tpc_sync_candidates(candidates, None)

        assert len(to_patch) == 2  # gcc-1 and gcc-3
        assert len(mismatched) == 1  # gcc-2

        # Verify gcc-1 in to_patch with core_size copy
        gcc1_patch = next(
            (p for p in to_patch if p["uuid"] == "gcc-1"),
            None
        )
        assert gcc1_patch is not None
        assert gcc1_patch["patch"] == {"core_size": "5mm"}

        # Verify gcc-3 in to_patch with empty patch (matching values)
        gcc3_patch = next(
            (p for p in to_patch if p["uuid"] == "gcc-3"),
            None
        )
        assert gcc3_patch is not None
        assert gcc3_patch["patch"] == {}

        # Verify gcc-2 in mismatched_samples
        gcc2_mismatch = mismatched[0]
        assert gcc2_mismatch["uuid"] == "gcc-2"
        assert "core_size mismatch" in gcc2_mismatch["reason"]

    @patch("chalicelib_smaht.checks.wrangler_checks._get_tpc_sample")
    def test_process_candidates_no_tpc_sample(self, mock_get_tpc):
        """Process skips candidates when no matching TPC sample found."""
        candidates = [{"uuid": "gcc-1", "external_id": "SAMPLE001"}]
        mock_get_tpc.return_value = None

        to_patch, mismatched = _process_tpc_sync_candidates(candidates, None)

        assert len(to_patch) == 0
        assert len(mismatched) == 0

    @patch("chalicelib_smaht.checks.wrangler_checks._get_tpc_sample")
    def test_process_candidates_no_external_id(self, mock_get_tpc):
        """Process skips candidates without external_id."""
        candidates = [{"uuid": "gcc-1"}]  # missing external_id

        to_patch, mismatched = _process_tpc_sync_candidates(candidates, None)

        assert len(to_patch) == 0
        assert len(mismatched) == 0
        mock_get_tpc.assert_not_called()

    # -------------------------------------------------------------------------
    # Existing behavior preservation tests
    # -------------------------------------------------------------------------

    @patch("chalicelib_smaht.checks.wrangler_checks._get_tpc_sample")
    def test_preserves_existing_field_behavior(self, mock_get_tpc):
        """Verify that core_size logic coexists with existing field sync."""
        candidates = [
            {"uuid": "gcc-1", "external_id": "SAMPLE001", "description": "GCC note"}
        ]
        mock_get_tpc.return_value = {
            "core_size": "5mm",
            "preservation_type": "Frozen",
            "description": "TPC note",
        }

        to_patch, mismatched = _process_tpc_sync_candidates(candidates, None)

        assert len(to_patch) == 1
        patch = to_patch[0]["patch"]
        assert patch["core_size"] == "5mm"
        assert patch["preservation_type"] == "Frozen"
        assert patch["description"] == "GCC: GCC note; TPC: TPC note"
        assert len(mismatched) == 0

    @patch("chalicelib_smaht.checks.wrangler_checks._get_tpc_sample")
    def test_matching_core_size_preserves_tag_only_behavior(self, mock_get_tpc):
        """Matching core_size with no other changes results in tag-only patch."""
        candidates = [
            {"uuid": "gcc-1", "external_id": "SAMPLE001", "core_size": "5mm"}
        ]
        mock_get_tpc.return_value = {"core_size": "5mm"}

        to_patch, mismatched = _process_tpc_sync_candidates(candidates, None)

        # Sample included with empty patch dict - will be tagged without field changes
        assert len(to_patch) == 1
        assert to_patch[0]["patch"] == {}
        assert len(mismatched) == 0


class TestSyncTPCTissueSampleMetadataCheck:
    """Integration tests for sync_tpc_tissue_sample_metadata check function."""

    @patch("chalicelib_smaht.checks.wrangler_checks._process_tpc_sync_candidates")
    @patch("dcicutils.ff_utils.search_metadata")
    def test_check_pass_when_no_candidates(self, mock_search, mock_process):
        """CHECK_PASS when no samples need syncing."""
        from chalicelib_smaht.checks.wrangler_checks import sync_tpc_tissue_sample_metadata
        from chalicelib_smaht.checks.helpers import constants

        mock_search.return_value = []
        mock_process.return_value = ([], [])

        mock_conn = MagicMock()
        result = sync_tpc_tissue_sample_metadata(mock_conn)

        assert result["status"] == constants.CHECK_PASS
        assert result["allow_action"] is False
        assert "synced with TPC metadata" in result["summary"]

    @patch("chalicelib_smaht.checks.wrangler_checks._process_tpc_sync_candidates")
    @patch("dcicutils.ff_utils.search_metadata")
    def test_check_warn_with_patchable_samples(self, mock_search, mock_process):
        """CHECK_WARN with allow_action=True when samples can be patched."""
        from chalicelib_smaht.checks.wrangler_checks import sync_tpc_tissue_sample_metadata
        from chalicelib_smaht.checks.helpers import constants

        mock_search.return_value = [{"uuid": "gcc-1", "external_id": "SAMPLE001"}]
        mock_process.return_value = (
            [{"uuid": "gcc-1", "external_id": "SAMPLE001", "patch": {"core_size": "5mm"}}],
            [],
        )

        mock_conn = MagicMock()
        result = sync_tpc_tissue_sample_metadata(mock_conn)

        assert result["status"] == constants.CHECK_WARN
        assert result["allow_action"] is True
        assert "1 sample(s) to sync" in result["summary"]
        assert "1 samples to patch" in result["brief_output"]
        assert len(result["full_output"]["to_patch"]) == 1
        assert len(result["full_output"].get("mismatched_samples", [])) == 0

    @patch("chalicelib_smaht.checks.wrangler_checks._process_tpc_sync_candidates")
    @patch("dcicutils.ff_utils.search_metadata")
    def test_check_warn_with_only_mismatches(self, mock_search, mock_process):
        """CHECK_WARN with allow_action=False when only mismatched samples exist."""
        from chalicelib_smaht.checks.wrangler_checks import sync_tpc_tissue_sample_metadata
        from chalicelib_smaht.checks.helpers import constants

        mock_search.return_value = [{"uuid": "gcc-1", "external_id": "SAMPLE001"}]
        mock_process.return_value = (
            [],
            [{
                "uuid": "gcc-1",
                "external_id": "SAMPLE001",
                "reason": "core_size mismatch: TPC=5mm, GCC=3mm",
            }],
        )

        mock_conn = MagicMock()
        result = sync_tpc_tissue_sample_metadata(mock_conn)

        assert result["status"] == constants.CHECK_WARN
        assert result["allow_action"] is False
        assert "1 sample(s) with core_size mismatch" in result["summary"]
        assert "1 excluded due to core_size mismatch" in result["brief_output"]
        assert len(result["full_output"].get("to_patch", [])) == 0
        assert len(result["full_output"]["mismatched_samples"]) == 1

    @patch("chalicelib_smaht.checks.wrangler_checks._process_tpc_sync_candidates")
    @patch("dcicutils.ff_utils.search_metadata")
    def test_check_warn_with_mixed_patchable_and_mismatches(self, mock_search, mock_process):
        """CHECK_WARN with allow_action=True when both patchable and mismatched samples."""
        from chalicelib_smaht.checks.wrangler_checks import sync_tpc_tissue_sample_metadata
        from chalicelib_smaht.checks.helpers import constants

        mock_search.return_value = [
            {"uuid": "gcc-1", "external_id": "SAMPLE001"},
            {"uuid": "gcc-2", "external_id": "SAMPLE002"},
        ]
        mock_process.return_value = (
            [{"uuid": "gcc-1", "external_id": "SAMPLE001", "patch": {"core_size": "5mm"}}],
            [{
                "uuid": "gcc-2",
                "external_id": "SAMPLE002",
                "reason": "core_size mismatch: TPC=5mm, GCC=3mm",
            }],
        )

        mock_conn = MagicMock()
        result = sync_tpc_tissue_sample_metadata(mock_conn)

        assert result["status"] == constants.CHECK_WARN
        assert result["allow_action"] is True
        assert "1 sample(s) to sync" in result["summary"]
        assert "1 sample(s) with core_size mismatch" in result["summary"]
        assert "1 samples to patch" in result["brief_output"]
        assert "1 excluded due to core_size mismatch" in result["brief_output"]
        assert len(result["full_output"]["to_patch"]) == 1
        assert len(result["full_output"]["mismatched_samples"]) == 1

    @patch("chalicelib_smaht.checks.wrangler_checks._process_tpc_sync_candidates")
    @patch("dcicutils.ff_utils.search_metadata")
    def test_check_outputs_structure(self, mock_search, mock_process):
        """Verify full_output structure includes to_patch and mismatched_samples."""
        from chalicelib_smaht.checks.wrangler_checks import sync_tpc_tissue_sample_metadata

        mock_search.return_value = [{"uuid": "gcc-1", "external_id": "SAMPLE001"}]
        mock_process.return_value = (
            [{"uuid": "gcc-1", "external_id": "SAMPLE001", "patch": {}}],
            [],
        )

        mock_conn = MagicMock()
        result = sync_tpc_tissue_sample_metadata(mock_conn)

        assert "to_patch" in result["full_output"]
        assert "mismatched_samples" in result["full_output"]
        assert isinstance(result["full_output"]["to_patch"], list)
        assert isinstance(result["full_output"]["mismatched_samples"], list)


class TestPatchTPCTissueSampleMetadataAction:
    """Integration tests for patch_tpc_tissue_sample_metadata action function."""

    @patch("dcicutils.ff_utils.patch_metadata")
    @patch("dcicutils.ff_utils.get_metadata")
    @patch("foursight_core.run_result.ActionResult.get_associated_check_result")
    def test_action_ignores_mismatched_samples(self, mock_get_check, mock_get, mock_patch):
        """Action processes only to_patch items; mismatched_samples are ignored."""
        from chalicelib_smaht.checks.wrangler_checks import patch_tpc_tissue_sample_metadata
        from chalicelib_smaht.checks.helpers import constants

        mock_conn = MagicMock()
        mock_conn.ff_keys = MagicMock()
        mock_get.return_value = {"tags": []}

        # Simulate check result with both to_patch and mismatched_samples
        check_result_dict = {
            "full_output": {
                "to_patch": [
                    {"uuid": "gcc-1", "external_id": "SAMPLE001", "patch": {"core_size": "5mm"}},
                ],
                "mismatched_samples": [
                    {
                        "uuid": "gcc-2",
                        "external_id": "SAMPLE002",
                        "reason": "core_size mismatch: TPC=5mm, GCC=3mm",
                    },
                ],
            }
        }

        # Mock get_associated_check_result to return our check_result_dict
        mock_get_check.return_value = check_result_dict
        result = patch_tpc_tissue_sample_metadata(mock_conn, check_name="sync_tpc_tissue_sample_metadata", called_by="test")

        # Only gcc-1 should be patched; gcc-2 (mismatched) is ignored
        assert mock_patch.call_count == 1
        call_args = mock_patch.call_args
        assert call_args[1]["obj_id"] == "gcc-1"
        patch_body = call_args[0][0]
        assert "core_size" in patch_body
        assert patch_body["core_size"] == "5mm"
        assert constants.TPC_METADATA_SYNCED_TAG in patch_body["tags"]

        # Verify result status
        assert result["status"] == constants.ACTION_PASS
        assert len(result["output"]["patch_success"]) == 1
        assert len(result["output"]["patch_failure"]) == 0

    @patch("dcicutils.ff_utils.patch_metadata")
    @patch("dcicutils.ff_utils.get_metadata")
    @patch("foursight_core.run_result.ActionResult.get_associated_check_result")
    def test_action_patches_tag_only_when_empty_patch(self, mock_get_check, mock_get, mock_patch):
        """Action patches only tag when patch dict is empty (matching core_size case)."""
        from chalicelib_smaht.checks.wrangler_checks import patch_tpc_tissue_sample_metadata
        from chalicelib_smaht.checks.helpers import constants

        mock_conn = MagicMock()
        mock_conn.ff_keys = MagicMock()
        mock_get.return_value = {"tags": []}

        check_result_dict = {
            "full_output": {
                "to_patch": [
                    {"uuid": "gcc-1", "external_id": "SAMPLE001", "patch": {}},
                ],
                "mismatched_samples": [],
            }
        }

        mock_get_check.return_value = check_result_dict
        result = patch_tpc_tissue_sample_metadata(mock_conn, check_name="sync_tpc_tissue_sample_metadata", called_by="test")

        assert mock_patch.call_count == 1
        patch_body = mock_patch.call_args[0][0]
        # Only tags should be in patch_body (empty patch dict contributes nothing)
        assert "tags" in patch_body
        assert constants.TPC_METADATA_SYNCED_TAG in patch_body["tags"]
        # No other fields
        assert len(patch_body) == 1

        assert result["status"] == constants.ACTION_PASS
