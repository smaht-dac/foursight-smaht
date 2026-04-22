import json
from os import name
import time
import random
from unittest import result
from datetime import datetime
import requests
import re
import html
from typing import Optional, Dict, List, Any
from dcicutils import ff_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *
from .helpers import wrangler_utils as wr_utils
from .helpers import constants

# use a random number to stagger checks
random_wait = 20


@check_function(action="tag_donors_with_released_files")
def untagged_donors_with_released_files(connection, **kwargs):
    check = CheckResult(connection, "untagged_donors_with_released_files")
    check.action = "tag_donors_with_released_files"
    check.allow_action = False
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    QUERY_STEM = "search/?type=File&dataset=tissue&field=donors"
    status_str = "".join(f"&status={s}" for s in constants.RELEASED_FILE_STATUSES)
    query = QUERY_STEM + status_str
    files = ff_utils.search_metadata(query, key=connection.ff_keys)
    unique_donor_ids = list(
        {d["uuid"] for f in files for d in f.get("donors", []) if "uuid" in d}
    )
    donors_with_released_files = [
        ff_utils.get_metadata(did, key=connection.ff_keys) for did in unique_donor_ids
    ]
    # first we are excluding donors that already have the tag and then including only those in Production study
    donors_to_tag = wr_utils.include_items_with_properties(
        wr_utils.exclude_items_with_properties(
            donors_with_released_files, {"tags": constants.DONOR_W_FILES_TAG}
        ),
        {"study": "Production"},
    )
    donors_to_tag.extend(
        [d.get("protected_donor") for d in donors_to_tag if "protected_donor" in d]
    )

    if not donors_to_tag:
        check.summary = "All donors with released files are tagged"
        check.description = f"With the tag - {constants.DONOR_W_FILES_TAG}"
        check.status = constants.CHECK_PASS
        return check

    donor_info = [
        f"{d.get('external_id', '')}    {d.get('accession', '')}   {d.get('@id', '')}"
        for d in donors_to_tag
    ]
    uuids = [d.get("uuid") for d in donors_to_tag if "uuid" in d]
    check.allow_action = True
    check.brief_output = "{} donors with released files to be tagged".format(
        len(donors_to_tag)
    )
    check.full_output = {"info": donor_info, "uuids": uuids}
    check.status = constants.CHECK_WARN
    check.summary = "Donors with released files need tagging"
    return check


@action_function()
def tag_donors_with_released_files(connection, **kwargs):
    action = ActionResult(connection, "tag_donors_with_released_files")
    action_logs = {"patch_failure": [], "patch_success": []}
    # get the associated untagged_donors_with_released_files result
    donors_to_tag_check_result = action.get_associated_check_result(kwargs)
    donors_to_tag = donors_to_tag_check_result.get("full_output", {}).get("uuids", [])
    for donor_uuid in donors_to_tag:
        try:
            existing_tags = ff_utils.get_metadata(
                donor_uuid, key=connection.ff_keys
            ).get("tags", [])
        except Exception as e:
            action.status = constants.ACTION_WARN
            action_logs["patch_failure"].append(
                f"Error fetching donor {donor_uuid}: {e}"
            )
            continue
        patch_body = {"tags": list(set(existing_tags + [constants.DONOR_W_FILES_TAG]))}
        try:
            ff_utils.patch_metadata(
                patch_body, obj_id=donor_uuid, key=connection.ff_keys
            )
            action_logs["patch_success"].append(
                f"Successfully tagged donor {donor_uuid}"
            )
        except Exception as e:
            action.status = constants.ACTION_WARN
            action_logs["patch_failure"].append(
                f"Error tagging donor {donor_uuid}: {e}"
            )
            continue

    if not action_logs.get("patch_failure") and len(
        action_logs.get("patch_success", [])
    ) == len(donors_to_tag):
        action.summary = f"Success"
        action.description = f"Successfully tagged {len(donors_to_tag)} donors with {constants.DONOR_W_FILES_TAG}"
        action.status = constants.ACTION_PASS
    action.output = action_logs
    return action


@check_function()
def item_counts_by_type(connection, **kwargs):
    def process_counts(count_str):
        # specifically formatted for FF health page
        ret = {}
        split_str = count_str.split()
        ret[split_str[0].strip(":")] = int(split_str[1])
        ret[split_str[2].strip(":")] = int(split_str[3])
        return ret

    check = CheckResult(connection, "item_counts_by_type")
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # run the check
    item_counts = {}
    warn_item_counts = {}
    req_location = "".join([connection.ff_server, "/counts?format=json"])
    counts_res = ff_utils.authorized_request(req_location, auth=connection.ff_keys)
    if counts_res.status_code >= 400:
        check.status = "ERROR"
        check.description = (
            "Error (bad status code %s) connecting to the counts endpoint at: %s."
            % (counts_res.status_code, req_location)
        )
        return check
    counts_json = json.loads(counts_res.text)
    for index in counts_json["db_es_compare"]:
        counts = process_counts(counts_json["db_es_compare"][index])
        item_counts[index] = counts
        if counts["DB"] != counts["ES"]:
            warn_item_counts[index] = counts
    # add ALL for total counts
    total_counts = process_counts(counts_json["db_es_total"])
    item_counts["ALL"] = total_counts
    # set fields, store result
    if not item_counts:
        check.status = "FAIL"
        check.summary = check.description = "Error on fourfront health page"
    elif warn_item_counts:
        check.status = "WARN"
        check.summary = check.description = "DB and ES item counts are not equal"
        check.brief_output = warn_item_counts
    else:
        check.status = "PASS"
        check.summary = check.description = "DB and ES item counts are equal"
    check.full_output = item_counts
    return check


## helpers for publication metadata retrieval
def is_rxiv_doi(doi: str) -> bool:
    """Check DOI prefix for Rxiv servers."""
    return any(doi.startswith(prefix) for prefix in constants.RXIV_PREFIXES)


def get_pmid_from_doi(doi: str) -> Optional[str]:
    """Look up PMID using DOI."""
    eutil = f"{constants.EUTIL_ESEARCH}db=pubmed&term={doi}[DOI]&retmode=json"
    pmid = None
    try:
        eutil_response = requests.get(eutil, timeout=10)
        pmid = eutil_response.json().get("esearchresult", {}).get("idlist", [])
    except Exception as e:
        print(f"Error fetching PMID: {e}")
    if pmid and len(pmid) == 1:
        return pmid[0]
    return None


def get_crossref_metadata(doi: str) -> Dict[str, Any]:
    """Retrieve metadata from Crossref using doi"""
    try:
        r = requests.get(f"{constants.CROSSREF_API}{doi}", timeout=10)
        r.raise_for_status()
        return r.json()["message"]
    except Exception as e:
        print(f"Error fetching Crossref metadata: {e}")
        return {}


def get_pubmed_metadata(pmid: str) -> Optional[Dict[str, Any]]:
    """Retrieve metadata from PubMed using PMID."""
    eutil = f"{constants.EUTIL_EFETCH}db=pubmed&id={pmid}&retmode=xml"
    try:
        response = requests.get(eutil, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching PubMed metadata: {e}")
        return None


def get_rxiv_metadata(doi: str) -> Optional[Dict[str, Any]]:
    """Retrieve metadata from bioRxiv/medRxiv."""
    try:
        # Extract the bioRxiv ID from DOI (e.g., 10.1101/2023.01.01.522534)
        # rxiv_id = doi.split("/")[-1]
        servers = constants.RXIV_PREFIXES.get(doi.split("/")[0], [])
        for server in servers:
            url = f"{constants.RXIV_API}/{server}/{doi}"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            if data.get("collection") and len(data["collection"]) > 0:
                return server, data["collection"][0]
    except Exception as e:
        print(f"Error fetching Rxiv metadata: {e}")

    return None, None


def parse_pubmed_xml(xml_text: str) -> Dict[str, Any]:
    """Parse PubMed XML response to extract metadata."""
    import xml.etree.ElementTree as ET

    result = {
        "title": None,
        "abstract": None,
        "authors": [],
        "journal": None,
        "date_published": None,
    }

    try:
        root = ET.fromstring(xml_text)

        # Extract title
        title_elem = root.find(".//ArticleTitle")
        if title_elem is not None and title_elem.text:
            result["title"] = title_elem.text

        # Extract abstract
        abstract_elem = root.find(".//AbstractText")
        if abstract_elem is not None and abstract_elem.text:
            result["abstract"] = abstract_elem.text

        # Extract authors
        authors = []
        for author in root.findall(".//Author"):
            last_name = author.find("LastName")
            fore_name = author.find("ForeName")
            if last_name is not None and fore_name is not None:
                authors.append({"first_name": fore_name.text, "last_name": last_name.text})
            elif last_name is not None:
                authors.append({"last_name": last_name.text})
        result["authors"] = authors

        # Extract journal
        journal_elem = root.find(".//Journal/Title")
        if journal_elem is not None and journal_elem.text:
            result["journal"] = journal_elem.text

        # Extract publication date
        pub_date = root.find(".//PubDate")
        if pub_date is not None:
            year = pub_date.find("Year")
            month = pub_date.find("Month")
            day = pub_date.find("Day")
            if year is not None:
                date_str = year.text
                if month is not None:
                    date_str += f"-{month.text}"
                if day is not None:
                    date_str += f"-{day.text}"
                result["date_published"] = normalize_date(date_str)

    except Exception as e:
        print(f"Error parsing PubMed XML: {e}")

    return result


def _clean_text(text: str) -> str:
    """Clean text by removing XML/HTML tags and normalizing whitespace."""
    if not text:
        return None

    # Decode HTML entities (e.g., &amp; -> &, &lt; -> <)
    text = html.unescape(text)

    # Remove XML/HTML tags (including JATS tags)
    text = re.sub(r"<[^>]+>", "", text)

    # Replace multiple whitespace characters (spaces, tabs, newlines) with single space
    text = re.sub(r"\s+", " ", text)

    # Strip leading and trailing whitespace
    text = text.strip()

    return text if text else None


def _format_crossref_date_parts(date_parts: List[int]) -> Optional[str]:
    # Format: ISO: yyyy-mm-dd
    formatted_parts = [
        str(part).zfill(2) if i > 0 else str(part) 
        for i, part in enumerate(date_parts)
    ]   
    return "-".join(formatted_parts)    


def _get_date_published_from_crossref(crossref_data: Dict[str, Any]) -> Optional[str]:
    date_fields = ["published-print", "published-online", "published"]
    for field in date_fields:
        if date_parts := crossref_data.get(field, {}).get("date-parts", [[]])[0]:
            return _format_crossref_date_parts(date_parts)
    return None


def crossref_is_not_journal_article(crossref_data: Dict[str, Any]) -> bool:
    """  
    Returns True for anything that isn't type "journal-article"
    """
    return crossref_data.get("type") != "journal-article"


def parse_crossref_metadata(crossref_data: Dict[str, Any]) -> Dict[str, Any]:
    """Parse CrossRef metadata into standardized format."""
    result = {
        "title": None,
        "abstract": None,
        "authors": [],
        "journal": None,
        "journal_url": None,
        "date_published": None,
        "is_preprint": False,
    }

    # Title
    if "title" in crossref_data and crossref_data["title"]:
        result["title"] = _clean_text(crossref_data["title"][0])

    # Abstract
    if "abstract" in crossref_data:
        result["abstract"] = _clean_text(crossref_data["abstract"])

    # Authors
    authors = []
    if "author" in crossref_data:
        for author in crossref_data["author"]:
            given = author.get("given", "")
            family = author.get("family", "")
            if given and family:
                authors.append({"first_name": given, "last_name": family})
            elif family:
                authors.append({"last_name": family})
    result["authors"] = authors

    # Journal
    if "container-title" in crossref_data and crossref_data["container-title"]:
        result["journal"] = crossref_data["container-title"][0]
    elif isinstance(crossref_data.get("institution"), dict):
        doi_prefix = crossref_data.get('DOI', '').split('/')[0]
        inst_name = crossref_data["institution"].get("name")
        if inst_name in constants.RXIV_PREFIXES.get(doi_prefix, []):
            result["journal"] = inst_name

    # Journal URL
    if "resource" in crossref_data and "primary" in crossref_data["resource"]:
        if crossref_data["resource"]["primary"].get("URL"):
            result["journal_url"] = crossref_data["resource"]["primary"]["URL"]

    # Publication date
    result["date_published"] = normalize_date(
        _get_date_published_from_crossref(crossref_data)
    )

    # Check if preprint - may be more types in future
    result["is_preprint"] = crossref_is_not_journal_article(crossref_data)

    return result


_MONTH_NAMES: Dict[str, str] = {
    # abbreviated
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    # full
    "january": "01", "february": "02", "march": "03",  "april": "04",
    "june":    "06", "july":     "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def _normalize_month(month_str: str) -> Optional[str]:
    """
    Convert a month string to a zero-padded two-digit numeric string.

    Accepts:
        - Numeric strings: "1" -> "01", "12" -> "12"
        - Abbreviated names: "Jan" -> "01"
        - Full names: "January" -> "01"

    Returns None if the value cannot be interpreted as a valid month.
    """
    if not month_str:
        return None
    lower = month_str.lower().strip()
    if lower in _MONTH_NAMES:
        return _MONTH_NAMES[lower]
    try:
        month_int = int(month_str)
        if 1 <= month_int <= 12:
            return str(month_int).zfill(2)
    except ValueError:
        pass
    return None


def normalize_date(date_value: Any) -> Optional[str]:
    """
    Normalize a date value from any publication source into a
    standardized string format.

    Returns:
        'YYYY-MM-DD'  if year, month, and day are all present and valid
        'YYYY-MM'     if year and month are present and valid
        'YYYY'        if only year is present and valid
        None          if the value cannot be interpreted as a date

    Accepts:
        - ISO-like strings:       "2023-01-15", "2023-1-5"
        - Month-name strings:     "2023-Jan-15", "2023-January-5"
        - Partial strings:        "2023-01", "2023-Jan", "2023"
        - datetime objects
        - None or empty string    -> None

    Sources and example inputs:
        CrossRef   : "2023-1-5"     (numeric, may lack zero-padding)
        PubMed XML : "2023-Jan-05"  (month as 3-letter abbreviation)
        bioRxiv    : "2023-01-15"   (typically already ISO format)
    """
    if date_value is None:
        return None

    # datetime object — format directly
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%d")

    if not isinstance(date_value, str):
        return None

    date_str = date_value.strip()
    if not date_str:
        return None

    # Split on hyphens or forward slashes
    parts = re.split(r"[-/]", date_str)

    # --- Year ---
    try:
        year_int = int(parts[0])
        if not (1000 <= year_int <= 9999):
            return None
        year_str = str(year_int)
    except (ValueError, IndexError):
        return None

    if len(parts) == 1:
        return year_str

    # --- Month ---
    month_str = _normalize_month(parts[1])
    if not month_str:
        # month unparseable — return year only rather than malformed string
        return year_str

    if len(parts) == 2:
        return f"{year_str}-{month_str}"

    # --- Day ---
    try:
        day_int = int(parts[2])
        if 1 <= day_int <= 31:
            return f"{year_str}-{month_str}-{str(day_int).zfill(2)}"
    except (ValueError, IndexError):
        pass

    # day unparseable — return year-month
    return f"{year_str}-{month_str}"


def _parse_author_name(author: str) -> Dict[str, str]:
    """
    Parse a single author string into a dictionary with last_name
    and optionally first_name.

    Handles common formats:
        "Smith, John"         -> {"last_name": "Smith", "first_name": "John"}
        "Smith, John A."      -> {"last_name": "Smith", "first_name": "John A."}
        "Smith, J."           -> {"last_name": "Smith", "first_name": "J."}
        "Smith JA"            -> {"last_name": "Smith", "first_name": "JA"}
        "John Smith"          -> {"last_name": "Smith", "first_name": "John"}
        "Smith"               -> {"last_name": "Smith"}
    """
    author = author.strip()
    if not author:
        return {}

    # Format: "Last, First [Middle/Initials]" — comma present
    if "," in author:
        parts = author.split(",", 1)
        last_name = parts[0].strip()
        first_name = parts[1].strip() if len(parts) > 1 else None
        result = {"last_name": last_name}
        if first_name:
            result["first_name"] = first_name
        return result

    # Format: "Last InitialsWithoutPeriods" e.g. "Smith JA" or "Smith J"
    # Detect by checking if the last token is all uppercase letters (initials)
    parts = author.split()
    if len(parts) == 2 and parts[1].isupper():
        return {"last_name": parts[0], "first_name": parts[1]}

    # Format: "First [Middle] Last" — no comma, last word is last name
    if len(parts) >= 2:
        last_name = parts[-1]
        first_name = " ".join(parts[:-1])
        return {"last_name": last_name, "first_name": first_name}

    # Single token — treat as last name only
    return {"last_name": author}


def _parse_authors(authors_str: str) -> List[Dict[str, str]]:
    """
    Parse a semicolon-separated author string into a list of author dicts.

    Each dict has:
        "last_name"  : str  (always present)
        "first_name" : str  (present when parseable)

    Example:
        "Smith, John A.; Doe, Jane; Brown JR"
        -> [
              {"last_name": "Smith", "first_name": "John A."},
              {"last_name": "Doe",   "first_name": "Jane"},
              {"last_name": "Brown", "first_name": "JR"},
           ]
    """
    if not authors_str:
        return []

    return [
        parsed
        for raw in authors_str.split(";")
        if (parsed := _parse_author_name(raw)) and parsed.get("last_name")
    ]


def parse_rxiv_data(rxiv_data: Dict[str, Any], rxiv_server: str) -> Dict[str, Any]:
    """Parse bioRxiv/medRxiv metadata into standardized format."""
    result = {
        "title": rxiv_data.get("title"),
        "abstract": rxiv_data.get("abstract"),
        "authors": _parse_authors(rxiv_data.get("authors", "")),
        "journal": rxiv_server,
        "date_published": normalize_date(rxiv_data.get("date")),
        "is_preprint": True,
        "preprint_version": rxiv_data.get("version"),
    }

    return result


def _make_hashable(value: Any) -> Any:
    """
    Recursively convert a value into a hashable representation
    so it can be placed in a set for order-independent list comparison.

    dict  → frozenset of (key, hashable_value) pairs
    list  → frozenset of hashable_value items  (order-independent)
    other → value as-is (must already be hashable: str, int, float, bool, None)
    """
    if isinstance(value, dict):
        return frozenset(
            (k, _make_hashable(v)) for k, v in sorted(value.items())
        )
    if isinstance(value, list):
        return frozenset(_make_hashable(item) for item in value)
    return value


def publication_values_equal(a: Any, b: Any) -> bool:
    """
    Determine whether two values derived from Publication schema data
    contain the same content regardless of ordering in lists or dicts.

    Handles every type produced by the Publication schema and its mixins:
    NOTE: may need to be updated upon schema changes.
        None, str, bool, int, float
            → direct equality

        dict  (e.g. a single author object, last_modified, attachment)
            → recursive key-value equality; key insertion order is ignored

        list  (e.g. authors, repository_urls, submission_centers, tags)
            → element equality regardless of element order;
              works for nested structures such as a list of author dicts.
              NOTE: consistent with the schema's pervasive 'uniqueItems: true',
              duplicate elements within a list are not counted separately —
              [x, x] and [x] are considered equal.  If you need
              multiplicity-aware comparison, replace the frozenset approach
              with a Counter-based one.

    Parameters
    ----------
    a, b : Any
        Two values to compare.

    Returns
    -------
    bool
        True if both values are semantically equal, False otherwise.

    Examples
    --------
    # Scalars
    publication_values_equal("2023-01-15", "2023-01-15")   # True
    publication_values_equal(True, False)                   # False
    publication_values_equal(None, None)                    # True

    # Author list — order-independent, nested dicts
    publication_values_equal(
        [{"last_name": "Smith", "first_name": "John"}, {"last_name": "Doe"}],
        [{"last_name": "Doe"}, {"last_name": "Smith", "first_name": "John"}],
    )  # True

    # URL list — order-independent
    publication_values_equal(
        ["https://pubmed.ncbi.nlm.nih.gov/123/", "https://biorxiv.org/456"],
        ["https://biorxiv.org/456", "https://pubmed.ncbi.nlm.nih.gov/123/"],
    )  # True

    # Dict — key order does not matter
    publication_values_equal(
        {"first_name": "Jane", "last_name": "Doe"},
        {"last_name": "Doe", "first_name": "Jane"},
    )  # True
    """
    # ------------------------------------------------------------------ #
    # None
    # ------------------------------------------------------------------ #
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False

    # ------------------------------------------------------------------ #
    # Type mismatch — treat bool/int as distinct (schema separates them)
    # ------------------------------------------------------------------ #
    if type(a) is not type(b):
        return False

    # ------------------------------------------------------------------ #
    # dict — recurse over keys; insertion order is irrelevant
    # ------------------------------------------------------------------ #
    if isinstance(a, dict):
        if set(a.keys()) != set(b.keys()):
            return False
        return all(publication_values_equal(a[k], b[k]) for k in a)

    # ------------------------------------------------------------------ #
    # list — order-independent via hashable set representation
    # ------------------------------------------------------------------ #
    if isinstance(a, list):
        if len(a) != len(b):
            return False
        try:
            return (
                {_make_hashable(item) for item in a}
                == {_make_hashable(item) for item in b}
            )
        except TypeError:
            # Unhashable leaf type encountered — fall back to sorted string
            # comparison so the function never raises unexpectedly.
            return sorted(str(x) for x in a) == sorted(str(x) for x in b)

    # ------------------------------------------------------------------ #
    # Scalars: str, bool, int, float
    # ------------------------------------------------------------------ #
    return a == b


def fetch_publication_info(connection, info: tuple) -> Dict[str, Any]:
    """
    Fetch publication information from external repositories given a DOI.

    Args:
        connection: The database connection object.
        info: A tuple containing the operation, DOI and optional accession number.

    Returns:
        Dictionary containing publication metadata
    """
    if info[0] == 'invalid':
        print(f"Invalid input: {info}")
        return {}
    curr_pub = None
    if info[0] == 'update' and not (
        curr_pub := ff_utils.get_metadata(info[2], key=connection.ff_keys)
    ):
        print(f"Invalid input for update operation (check your accession): {info}")
        return {}
    doi = info[1]
    if info[0] != 'update' and (
        duplicate_pub := ff_utils.search_metadata(
            f"search/?type=Publication&doi={doi}",
            key=connection.ff_keys
        )
    ):
        print(f"Publication with DOI {doi} already exists: {duplicate_pub['uuid']}")
        return {}

    pub_info = {
        "consortia": ["smaht"],
        "doi": doi,
        "pubmed_id": None,
        "is_preprint": False,
        "journal": None,
        "journal_url": None,
        "repository_urls": [],
        "title": None,
        "abstract": None,
        "authors": [],
        "date_published": None,
        "preprint_version": None,
    }
    crossref_metadata = None
    rxiv_metadata = None    
    pubmed_metadata = None

    if not doi.startswith("10."):
        print(f"Invalid DOI: {doi}")
        return {}

    # Try to fetch from CrossRef (works for most DOIs)
    crossref_response = get_crossref_metadata(doi)
    if crossref_response:
        crossref_metadata = parse_crossref_metadata(crossref_response)
        
    if crossref_metadata:
        # populate pub_info from CrossRef data
        pub_info.update(crossref_metadata)

    if is_rxiv_doi(doi):
        print(f"Detected Rxiv preprint: {doi}")
        rxiv_server, rxiv_response = get_rxiv_metadata(doi)
        if rxiv_server and rxiv_response:
            rxiv_metadata = parse_rxiv_data(rxiv_response, rxiv_server)

    # see if we can get pubmed id from doi
    pmid = get_pmid_from_doi(doi)

    if rxiv_metadata:
        # Merge data, preferring existing pub_info values
        for key, value in rxiv_metadata.items():
            if value and not pub_info[key]:
                pub_info[key] = value

        # add Rxiv URL to repository URLs - this resolves to either bioRxiv or medRxiv
        pub_info["repository_urls"].append(f"https://doi.org/{doi}")
        if pmid:
            pub_info["pubmed_id"] = pmid
            pub_info["repository_urls"].append(f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/")

    # Fetch PubMed metadata
    pubmed_xml = get_pubmed_metadata(pmid)
    if pubmed_xml:
        pubmed_metadata = parse_pubmed_xml(pubmed_xml)
        # what we do with this here depends on what we have already from CrossRef/Rxiv
        if crossref_metadata and pub_info["authors"]:
            # do we want to compare as sanity check?
            pass
        else:
            if pubmed_metadata.get("authors"):
                pub_info["authors"] = pubmed_metadata["authors"]
            
        # Merge data, preferring PubMed data for certain fields
        for key, value in pubmed_metadata.items():
            if key == "authors":
                continue  # handle authors separately above 
            if value and not pub_info[key]:
                pub_info[key] = value

        # Add PubMed URL to repository URLs
        pubmed_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
        if pubmed_url not in pub_info["repository_urls"]:
            pub_info["repository_urls"].append(pubmed_url)

    # here is where to compare existing metdata if there if curr_pub and apply updates
    if curr_pub:
        update_fields = {}
        for key in pub_info:
            if key not in curr_pub and pub_info[key]:
                # new field to add
                update_fields[key] = pub_info[key]
            elif key in curr_pub and curr_pub.get(key) != pub_info[key]:
                # assure ourselves there is really a different value
                if not publication_values_equal(curr_pub.get(key), pub_info[key]):
                    # we have an update to make
                    update_fields[key] = pub_info[key]
            else:
                continue
            if update_fields:
                update_fields["uuid"] = curr_pub["uuid"]

    return info[0], update_fields if info[0] == 'update' else pub_info


ACCESSION_PATTERN = re.compile(r'SMAPB[A-Z0-9]{7}')
DOI_PATTERN = re.compile(r'10\.\d{4,}/[^|\s]+')

RECORD_PATTERN = re.compile(
    rf'^(?P<doi>{DOI_PATTERN.pattern})(?:\|(?P<accession>{ACCESSION_PATTERN.pattern}))?$'
)


def parse_input_ids(input_string: str) -> list[tuple[str, Optional[str], Optional[str]]]:
    """
    Parse a comma-separated string into a list of tuples.

    Record formats
    --------------
    Update  →  <doi>|SMHTPB<7 digits>   e.g. '10.1000/xyz123|SMHTPB1234567'
    Create  →  <doi> alone              e.g. '10.1000/abc456'

    Parameters
    ----------
    input_string : str
        Raw comma-separated input. Whitespace around each value is ignored.

    Returns
    -------
    list of tuples: (operation, doi, accession)
        operation - 'create', 'update', or 'invalid'
        doi       - DOI string, or None if invalid
        accession - accession string if update, None otherwise
    """
    result = []

    if not input_string or not input_string.strip():
        return result

    for token in [t.strip() for t in input_string.split(',')]:
        if not token:
            continue

        match = RECORD_PATTERN.match(token)

        if match:
            doi       = match.group('doi')
            accession = match.group('accession')  # None if not present

            if accession:
                result.append(('update', doi, accession))
            else:
                result.append(('create', doi, None))
        else:
            result.append(('invalid', None, None))

    return result


@check_function(action="update_pub_metadata", doi_acc_list=[])
def prepare_pub_metadata(connection, **kwargs):
    # can take as input a comma-separated list of DOIs with optional accessions 
    # for existing publications to update, in the format:
    # 10.1000/xyz123|SMHTPB1234567, 10.1000/abc456
    check = CheckResult(connection, "prepare_pub_metadata")
    check.action = "update_pub_metadata"
    check.allow_action = False
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    id_str = kwargs.get('doi_acc_list')
    if not id_str:
        check.status = constants.CHECK_PASS
        check.summary = "No IDs provided for metadata update"
        return check
    pubs_to_post = []
    pubs_to_patch = []
    problems = []
    id_list = parse_input_ids(id_str)
    for idinfo in id_list:
        if pub_info := fetch_publication_info(connection, idinfo):
            if pub_info[0] == 'create':
                pubs_to_post.append(pub_info[1])
            elif pub_info[0] == 'update':
                pubs_to_patch.append(pub_info[1])
            else:
                problems.append(idinfo)
    check.full_output = {"input": id_str,
                         "parsed_idinfo": id_list,
                         "pubs_to_post": pubs_to_post,
                         "pubs_to_patch": pubs_to_patch,
                         "problems": problems
                         }
    if pubs_to_post or pubs_to_patch:
        check.status = constants.CHECK_WARN
        check.summary = f"{len(pubs_to_post)} pubs to create, {len(pubs_to_patch)} pubs to update, {len(problems)} invalid records"
        check.allow_action = True
    else:
        check.status = constants.CHECK_FAIL
        check.summary = "No valid publication metadata retrieved for provided IDs"

    return check


@action_function()
def update_pub_metadata(connection, **kwargs):
    """
    Action for prepare_pub_metadata check.

    Reads pubs_to_post / pubs_to_patch from the check's full_output and:
      - POSTs each new publication to the portal as a Publication item.
      - PATCHes each existing publication using the uuid stored in the dict.

    None-valued fields are stripped from POST bodies so the server schema
    validator does not see empty required/optional fields.
    """
    action = ActionResult(connection, "update_pub_metadata")
    action_logs = {
        "post_success": [],
        "post_failure": [],
        "patch_success": [],
        "patch_failure": [],
    }

    # Retrieve the paired check result
    check_result = action.get_associated_check_result(kwargs)
    full_output = check_result.get("full_output", {})

    pubs_to_post = full_output.get("pubs_to_post", [])
    pubs_to_patch = full_output.get("pubs_to_patch", [])

    # ------------------------------------------------------------------
    # POST – create new Publication items
    # ------------------------------------------------------------------
    for pub_data in pubs_to_post:
        doi = pub_data.get("doi", "unknown")
        # Strip None/empty values so the server validator stays happy
        clean_data = {k: v for k, v in pub_data.items() if v is not None and v != []}
        try:
            ff_utils.post_metadata(clean_data, "Publication", key=connection.ff_keys)
            action_logs["post_success"].append(
                f"Created publication — DOI: {doi}"
            )
        except Exception as e:
            action_logs["post_failure"].append(
                f"Failed to create publication — DOI: {doi} | Error: {e}"
            )

    # ------------------------------------------------------------------
    # PATCH – update existing Publication items
    # ------------------------------------------------------------------
    for patch_data in pubs_to_patch:
        uuid = patch_data.get("uuid")
        if not uuid:
            action_logs["patch_failure"].append(
                f"Skipped patch — no UUID found in patch data: {patch_data}"
            )
            continue

        # uuid is only needed to address the item; exclude it from the body
        patch_body = {
            k: v for k, v in patch_data.items()
            if k != "uuid" and v is not None and v != []
        }
        doi = patch_body.get("doi", uuid)

        try:
            ff_utils.patch_metadata(patch_body, obj_id=uuid, key=connection.ff_keys)
            action_logs["patch_success"].append(
                f"Updated publication {uuid} — DOI: {doi}"
            )
        except Exception as e:
            action_logs["patch_failure"].append(
                f"Failed to update publication {uuid} — DOI: {doi} | Error: {e}"
            )

    # ------------------------------------------------------------------
    # Determine final action status
    # ------------------------------------------------------------------
    total_attempted = len(pubs_to_post) + len(pubs_to_patch)
    total_success   = len(action_logs["post_success"]) + len(action_logs["patch_success"])
    total_failure   = len(action_logs["post_failure"]) + len(action_logs["patch_failure"])

    if total_attempted == 0:
        action.status  = constants.ACTION_WARN
        action.summary = "Action ran but found nothing to post or patch"
    elif total_failure == 0:
        action.status  = constants.ACTION_PASS
        action.summary = (
            f"All {total_attempted} operations succeeded — "
            f"created: {len(action_logs['post_success'])}, "
            f"updated: {len(action_logs['patch_success'])}"
        )
    else:
        action.status  = constants.ACTION_WARN
        action.summary = (
            f"{total_success} succeeded, {total_failure} failed "
            f"out of {total_attempted} total operations"
        )

    action.output = action_logs
    return action