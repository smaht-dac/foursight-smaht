# Check status
CHECK_PASS = "PASS"
CHECK_WARN = "WARN"
CHECK_FAIL = "FAIL"
CHECK_ERROR = "ERROR"
CHECK_IGNORE = "IGNORE"

# Action status
ACTION_PASS = "DONE"
ACTION_PEND = "PEND"
ACTION_FAIL = "FAIL"
ACTION_WARN = "WARN"

# MetaWorkflowRun final_status
MWFR_RUNNING = "running"
MWFR_INACTIVE = "inactive"
MWFR_PENDING = "pending"
MWFR_FAILED = "failed"

# wrangler check constants
RELEASED_FILE_STATUSES = [
    'open', 'open-early', 'open-network',
    'protected', 'protected-early', 'protected-network'
]
DONOR_W_FILES_TAG = "has_released_files"
TPC_NAME = "NDRI TPC"
DAC_NAME = "HMS DAC"

# constants for external resource retrieval
CROSSREF_API = "https://api.crossref.org/works/"
RXIV_API = "https://api.biorxiv.org/details"
NIHEUTIL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
EUTIL_ESEARCH = NIHEUTIL + "esearch.fcgi?"
EUTIL_EFETCH = NIHEUTIL + "efetch.fcgi?"
RXIV_PREFIXES = {
    "10.1101": ["biorxiv", "medrxiv"]
}
