import json
from datetime import datetime
from dcicutils import ff_utils
from dcicutils.s3_utils import s3Utils
from packaging import version


def get_md5_mwfrs_for_file(my_auth, file_uuid):
    query = f"/search/?type=MetaWorkflowRun&meta_workflow.name=md5&input.files.file.uuid={file_uuid}"
    return ff_utils.search_metadata(query, key=my_auth)


def get_latest_md5_mwf(my_auth):
    # We assume that md5 MetaWorkflows have name "md5". We have a similar strong assumption in Tibanna.
    query = f"/search/?type=MetaWorkflow&name=md5"
    search_results = ff_utils.search_metadata(query, key=my_auth)
    
    if len(search_results) == 0:
        return None
    
    latest_md5_item = search_results[0]
    if len(search_results) == 1:
        return latest_md5_item
    
    # There are multiple MWFs. Get the latest version
    for search_result in search_results:
        if version.parse(latest_md5_item["version"]) < version.parse(search_result["version"]):
            latest_md5_item = search_result
    return latest_md5_item

# Unused right now - maybe useful later
def paginate_list(list, page_size):
    """ 
    Paginate a list. Example:
    paginate_list([a, b, c, d, e, f, g, h, i], 4) returns [[a, b, c, d], [e, f, g, h], [i]]
    """
    return [list[i:i+page_size] for i in range(0, len(list), page_size)]


def string_to_list(string):
    "Given a string that is either comma separated values, or a python list, parse to list"
    for a_sep in "'\":[] ":
        values = string.replace(a_sep, ",")
    values = [i.strip() for i in values.split(',') if i]
    return values
