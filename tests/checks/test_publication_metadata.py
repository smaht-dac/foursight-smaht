"""Tests for Publication metadata preparation (prepare_pub_metadata / update_pub_metadata)."""
from unittest.mock import MagicMock, patch

import pytest

from chalicelib_smaht.checks import wrangler_checks as wc

DOI = "10.1101/2025.12.12.692823"
NEW_DOI = "10.1038/s41586-026-00001-1"
PUBMED_URL = "https://pubmed.ncbi.nlm.nih.gov/41446189/"
DOI_URL = f"https://doi.org/{DOI}"
BODY = "Detecting somatic mutations at scale."


# ---------------------------------------------------------------------------
# Abstract normalization
# ---------------------------------------------------------------------------

class TestCrossrefAbstract:
    def test_label_title_removed(self):
        raw = f"<jats:title>Abstract</jats:title><jats:p>{BODY}</jats:p>"
        assert wc.parse_crossref_metadata({"abstract": raw})["abstract"] == BODY

    def test_without_label_title_unchanged(self):
        raw = f"<jats:p>{BODY}</jats:p>"
        assert wc.parse_crossref_metadata({"abstract": raw})["abstract"] == BODY

    def test_graphical_abstract_section_and_figures_removed(self):
        raw = (
            f"<jats:title>Abstract</jats:title><jats:p>{BODY}</jats:p>"
            "<jats:sec><jats:title>Graphical Abstract</jats:title>"
            "<jats:fig><jats:caption><jats:p>A figure</jats:p></jats:caption></jats:fig></jats:sec>"
        )
        assert wc.parse_crossref_metadata({"abstract": raw})["abstract"] == BODY

    def test_other_sections_kept_and_separated(self):
        raw = (
            "<jats:sec><jats:title>Background</jats:title><jats:p>Why.</jats:p></jats:sec>"
            "<jats:sec><jats:title>Results</jats:title><jats:p>What <jats:italic>E</jats:italic>.coli did.</jats:p></jats:sec>"
        )
        assert wc.parse_crossref_metadata({"abstract": raw})["abstract"] == (
            "Background Why. Results What E.coli did."
        )

    def test_prose_starting_with_abstract_preserved(self):
        raw = "<jats:p>Abstract algebra underlies the method.</jats:p>"
        assert (
            wc.parse_crossref_metadata({"abstract": raw})["abstract"]
            == "Abstract algebra underlies the method."
        )

    def test_label_only_abstract_is_empty(self):
        assert wc.parse_crossref_metadata(
            {"abstract": "<jats:title>Abstract</jats:title>"}
        )["abstract"] is None


class TestRxivAbstract:
    @pytest.mark.parametrize("raw", [
        f"{BODY} Graphical Abstract",
        f"{BODY}\nGraphical Abstract",
        f"{BODY} Graphical Abstract Open in new tab Download powerpoint",
        f"Abstract: {BODY}",
        f"Abstract\n{BODY}",
        f"  {BODY}  ",
    ])
    def test_labels_removed(self, raw):
        assert wc.parse_rxiv_data({"abstract": raw}, "biorxiv")["abstract"] == BODY

    @pytest.mark.parametrize("prose", [
        "Abstract ideas are hard to measure.",
        "We show the graphical abstract of the pipeline is stable.",
        "Results. Graphical Abstract tools improve outcomes in many settings.",
    ])
    def test_prose_preserved(self, prose):
        assert wc.parse_rxiv_data({"abstract": prose}, "biorxiv")["abstract"] == prose


class TestPubmedAbstract:
    def test_structured_abstract_joined_without_labels(self):
        xml = (
            "<PubmedArticleSet><PubmedArticle><MedlineCitation><Article>"
            "<Abstract>"
            '<AbstractText Label="BACKGROUND">Why it matters.</AbstractText>'
            '<AbstractText Label="RESULTS">What <i>E</i>. coli did.</AbstractText>'
            "</Abstract>"
            "<OtherAbstract><AbstractText>Resumen.</AbstractText></OtherAbstract>"
            "</Article></MedlineCitation></PubmedArticle></PubmedArticleSet>"
        )
        assert wc.parse_pubmed_xml(xml)["abstract"] == "Why it matters. What E. coli did."


# ---------------------------------------------------------------------------
# fetch_publication_info
# ---------------------------------------------------------------------------

def _curr(**kw):
    pub = {
        "uuid": "u-1",
        "doi": DOI,
        "consortia": ["/consortia/abc/"],
        "is_preprint": True,
        "journal": "biorxiv",
        "title": "T",
        "abstract": BODY,
        "authors": [{"last_name": "Doe"}],
        "date_published": "2025-12-15",
        "pubmed_id": "41446189",
        "repository_urls": [DOI_URL, PUBMED_URL],
    }
    pub.update(kw)
    return pub


CROSSREF = {
    "title": "T", "abstract": BODY, "authors": [{"last_name": "Doe"}], "journal": "biorxiv",
    "journal_url": None, "date_published": "2025-12-15", "is_preprint": True,
}


def _fetch(curr, doi=DOI, pmid=None, crossref=CROSSREF, pubmed_xml=None):
    """Run fetch_publication_info for an update with all externals mocked."""
    connection = MagicMock()
    with patch.object(wc.ff_utils, "get_metadata", return_value=curr), \
         patch.object(wc, "get_crossref_metadata", return_value={"x": 1}), \
         patch.object(wc, "parse_crossref_metadata", return_value=dict(crossref)), \
         patch.object(wc, "get_rxiv_metadata", return_value=(None, None)), \
         patch.object(wc, "get_pmid_from_doi", return_value=pmid), \
         patch.object(wc, "get_pubmed_metadata", return_value=pubmed_xml) as get_pubmed:
        result = wc.fetch_publication_info(connection, ("update", doi, "SMAPBAAAAAAA"))
    return result, get_pubmed


class TestFetchPublicationInfo:
    def test_missing_pmid_does_not_fetch_pubmed(self):
        _, get_pubmed = _fetch(_curr(), pmid=None)
        get_pubmed.assert_not_called()

    def test_noop_is_reported_unchanged(self):
        (status, payload), _ = _fetch(_curr(), pmid=None, doi=DOI)
        # DOI URL is derived for rxiv DOIs; PubMed URL needs a PMID
        assert status == "update"  # pubmed url would be dropped without a pmid
        (status, payload), _ = _fetch(_curr(repository_urls=[DOI_URL]), pmid=None)
        assert status == "unchanged"
        assert payload["uuid"] == "u-1"

    def test_noop_ignores_empty_equivalents_and_consortia_reference(self):
        curr = _curr(repository_urls=[DOI_URL], previous_repository_urls=[], pubmed_id=None)
        (status, _), _ = _fetch(curr, pmid=None)
        assert status == "unchanged"

    def test_missing_source_value_does_not_clear(self):
        crossref = dict(CROSSREF, abstract=None, title=None)
        (status, payload), _ = _fetch(_curr(repository_urls=[DOI_URL]), pmid=None, crossref=crossref)
        assert status == "unchanged"

    def test_removed_url_moves_to_history_and_existing_history_kept(self):
        curr = _curr(repository_urls=[DOI_URL, PUBMED_URL], previous_repository_urls=["https://old/"])
        (status, payload), _ = _fetch(curr, pmid=None)
        assert status == "update"
        assert payload["repository_urls"] == [DOI_URL]
        assert payload["previous_repository_urls"] == ["https://old/", PUBMED_URL]

    def test_history_is_idempotent_and_deduplicated(self):
        curr = _curr(repository_urls=[DOI_URL, PUBMED_URL], previous_repository_urls=[PUBMED_URL])
        (_, payload), _ = _fetch(curr, pmid=None)
        assert "previous_repository_urls" not in payload
        assert payload["repository_urls"] == [DOI_URL]

    def test_doi_change_without_new_urls_clears_and_archives(self):
        curr = _curr()
        crossref = dict(CROSSREF, is_preprint=False, journal="Nature")
        (status, payload), _ = _fetch(curr, doi=NEW_DOI, pmid=None, crossref=crossref)
        assert status == "update"
        assert payload["doi"] == NEW_DOI
        assert payload["repository_urls"] == []
        assert payload["pubmed_id"] is None
        assert payload["previous_repository_urls"] == [DOI_URL, PUBMED_URL]

    def test_same_doi_without_new_urls_does_not_clear_pubmed_id(self):
        (_, payload), _ = _fetch(_curr(), pmid=None)
        assert "pubmed_id" not in payload

    def test_baseline_records_current_values(self):
        (_, payload), _ = _fetch(_curr(), pmid=None)
        baseline = payload["_baseline"]
        assert baseline["repository_urls"] == [DOI_URL, PUBMED_URL]
        assert baseline["previous_repository_urls"] is None

    def test_create_payload_unchanged(self):
        connection = MagicMock()
        with patch.object(wc.ff_utils, "search_metadata", return_value=[]), \
             patch.object(wc, "get_crossref_metadata", return_value={"x": 1}), \
             patch.object(wc, "parse_crossref_metadata", return_value=dict(CROSSREF)), \
             patch.object(wc, "get_rxiv_metadata", return_value=(None, None)), \
             patch.object(wc, "get_pmid_from_doi", return_value=None):
            status, payload = wc.fetch_publication_info(connection, ("create", DOI, None))
        assert status == "create"
        assert payload["repository_urls"] == [DOI_URL]
        assert payload["consortia"] == ["smaht"]


# ---------------------------------------------------------------------------
# prepare_pub_metadata
# ---------------------------------------------------------------------------

class TestPrepareCheck:
    def _run(self, results):
        with patch.object(wc.time, "sleep"), \
             patch.object(wc, "fetch_publication_info", side_effect=results):
            return wc.prepare_pub_metadata(MagicMock(), doi_acc_list=f"{DOI}|SMAPBAAAAAAA")

    def test_unchanged_not_queued(self):
        check = self._run([("unchanged", {"doi": DOI, "uuid": "u-1"})])
        assert check["full_output"]["pubs_to_patch"] == []
        assert check["full_output"]["unchanged"] == [{"doi": DOI, "uuid": "u-1"}]
        assert check["status"] == "PASS"
        assert not check["allow_action"]


# ---------------------------------------------------------------------------
# update_pub_metadata action
# ---------------------------------------------------------------------------

def _run_action(patches=(), posts=(), current=None, search=None):
    action = MagicMock()
    action.get_associated_check_result.return_value = {
        "full_output": {"pubs_to_post": list(posts), "pubs_to_patch": list(patches)}
    }
    connection = MagicMock()
    with patch.object(wc, "ActionResult", return_value=action), \
         patch.object(wc.ff_utils, "get_metadata", return_value=current) as get_md, \
         patch.object(wc.ff_utils, "search_metadata", return_value=search or []), \
         patch.object(wc.ff_utils, "patch_metadata") as patch_md, \
         patch.object(wc.ff_utils, "post_metadata") as post_md:
        # call the undecorated body; the decorator would wrap the result in a dict
        result = wc.update_pub_metadata.__wrapped__(connection)
    return result, patch_md, post_md, get_md


def _patch_entry(**kw):
    entry = {
        "uuid": "u-1",
        "repository_urls": [DOI_URL],
        "previous_repository_urls": [PUBMED_URL],
        "_baseline": {
            "repository_urls": [DOI_URL, PUBMED_URL],
            "previous_repository_urls": None,
        },
    }
    entry.update(kw)
    return entry


class TestUpdateAction:
    def test_patch_applied_when_unchanged_since_check(self):
        current = _curr()
        result, patch_md, _, _ = _run_action(patches=[_patch_entry()], current=current)
        patch_md.assert_called_once()
        args, kwargs = patch_md.call_args
        assert args[0] == {
            "repository_urls": [DOI_URL], "previous_repository_urls": [PUBMED_URL],
        }
        assert kwargs["obj_id"] == "u-1"
        assert kwargs["add_on"] == ""
        assert len(result.output["patch_success"]) == 1

    def test_stale_check_is_not_written(self):
        current = _curr(repository_urls=[DOI_URL, PUBMED_URL, "https://added.later/"])
        result, patch_md, _, _ = _run_action(patches=[_patch_entry()], current=current)
        patch_md.assert_not_called()
        assert "changed since the check" in result.output["patch_failure"][0]

    def test_missing_baseline_is_not_written(self):
        entry = _patch_entry()
        del entry["_baseline"]
        result, patch_md, _, _ = _run_action(patches=[entry], current=_curr())
        patch_md.assert_not_called()
        assert "no check-time baseline" in result.output["patch_failure"][0]

    def test_duplicate_action_is_idempotent(self):
        current = _curr(repository_urls=[DOI_URL], previous_repository_urls=[PUBMED_URL])
        result, patch_md, _, _ = _run_action(patches=[_patch_entry()], current=current)
        patch_md.assert_not_called()
        assert result.output["patch_failure"] == []
        assert "already up to date" in result.output["patch_success"][0]

    def test_refetch_failure_is_not_written(self):
        result, patch_md, _, _ = _run_action(patches=[_patch_entry()], current=None)
        patch_md.assert_not_called()
        assert len(result.output["patch_failure"]) == 1

    def test_explicit_empty_list_and_scalar_clear_are_sent(self):
        entry = {
            "uuid": "u-1", "doi": NEW_DOI, "repository_urls": [], "pubmed_id": None,
            "previous_repository_urls": [DOI_URL, PUBMED_URL],
            "_baseline": {
                "doi": DOI, "repository_urls": [DOI_URL, PUBMED_URL],
                "pubmed_id": "41446189", "previous_repository_urls": None,
            },
        }
        _, patch_md, _, _ = _run_action(patches=[entry], current=_curr())
        args, kwargs = patch_md.call_args
        assert args[0] == {
            "doi": NEW_DOI, "repository_urls": [],
            "previous_repository_urls": [DOI_URL, PUBMED_URL],
        }
        assert kwargs["add_on"] == "delete_fields=pubmed_id"

    def test_non_clearable_empty_values_are_dropped_from_patch(self):
        entry = _patch_entry(abstract=None, authors=[])
        entry["_baseline"].update(abstract=BODY, authors=[{"last_name": "Doe"}])
        _, patch_md, _, _ = _run_action(patches=[entry], current=_curr())
        args, kwargs = patch_md.call_args
        assert "abstract" not in args[0] and "authors" not in args[0]
        assert kwargs["add_on"] == ""

    def test_post_strips_empty_values(self):
        post = {"doi": DOI, "pubmed_id": None, "repository_urls": [], "title": "T"}
        _, _, post_md, _ = _run_action(posts=[post])
        assert post_md.call_args[0][0] == {"doi": DOI, "title": "T"}

    def test_post_skipped_when_doi_now_exists(self):
        _, _, post_md, _ = _run_action(posts=[{"doi": DOI}], search=[{"uuid": "u-9"}])
        post_md.assert_not_called()
