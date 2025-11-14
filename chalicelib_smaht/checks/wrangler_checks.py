import json
import time
import random
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
    check = CheckResult(connection, 'untagged_donors_with_released_files')
    check.action = "tag_donors_with_released_files"
    check.allow_action = False
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    QUERY_STEM = "search/?type=File&dataset=tissue&field=donors"
    status_str = ''.join(f"&status={s}" for s in constants.RELEASED_FILE_STATUSES)
    query = QUERY_STEM + status_str
    files = ff_utils.search_metadata(query, key=connection.ff_keys)
    unique_donor_ids = list({d["uuid"] for f in files for d in f.get("donors", []) if "uuid" in d})
    donors_with_released_files = [ff_utils.get_metadata(did, key=connection.ff_keys) for did in unique_donor_ids]
    # first we are excluding donors that already have the tag and then including only those in Production study
    donors_to_tag = wr_utils.include_items_with_properties(
        wr_utils.exclude_items_with_properties(donors_with_released_files, {"tags": constants.DONOR_W_FILES_TAG}),
        {"study": "Production"})
    donors_to_tag.extend([d.get('protected_donor') for d in donors_to_tag if 'protected_donor' in d])

    if not donors_to_tag:
        check.summary = 'All donors with released files are tagged'
        check.description = f'With the tag - {constants.DONOR_W_FILES_TAG}'
        check.status = constants.CHECK_PASS
        return check

    donor_info = [f"{d.get('external_id', '')}    {d.get('accession', '')}   {d.get('@id', '')}" for d in donors_to_tag]
    uuids = [d.get('uuid') for d in donors_to_tag if 'uuid' in d]
    check.allow_action = True
    check.brief_output = '{} donors with released files to be tagged'.format(len(donors_to_tag))
    check.full_output = {'info': donor_info, 'uuids': uuids}
    check.status = constants.CHECK_WARN
    check.summary = 'Donors with released files need tagging'
    return check


@action_function()
def tag_donors_with_released_files(connection, **kwargs):
    action = ActionResult(connection, 'tag_donors_with_released_files')
    action_logs = {'patch_failure': [], 'patch_success': []}
    # get the associated untagged_donors_with_released_files result
    donors_to_tag_check_result = action.get_associated_check_result(kwargs)
    donors_to_tag = donors_to_tag_check_result.get('full_output', {}).get('uuids', [])
    for donor_uuid in donors_to_tag:
        try:
            existing_tags = ff_utils.get_metadata(donor_uuid, key=connection.ff_keys).get('tags', [])
        except Exception as e:
            action.status = constants.ACTION_WARN
            action_logs['patch_failure'].append(f'Error fetching donor {donor_uuid}: {e}')
            continue
        patch_body = {'tags': list(set(existing_tags + [constants.DONOR_W_FILES_TAG]))}
        try:
            ff_utils.patch_metadata(patch_body, obj_id=donor_uuid, key=connection.ff_keys)
            action_logs['patch_success'].append(f'Successfully tagged donor {donor_uuid}')
        except Exception as e:
            action.status = constants.ACTION_WARN
            action_logs['patch_failure'].append(f'Error tagging donor {donor_uuid}: {e}')
            continue

    if not action_logs.get('patch_failure') and len(action_logs.get('patch_success', [])) == len(donors_to_tag):
        action.summary = f'Success'
        action.description = f'Successfully tagged {len(donors_to_tag)} donors with {constants.DONOR_W_FILES_TAG}'
        action.status = constants.ACTION_PASS
    action.output = action_logs
    return action


@check_function()
def item_counts_by_type(connection, **kwargs):
    def process_counts(count_str):
        # specifically formatted for FF health page
        ret = {}
        split_str = count_str.split()
        ret[split_str[0].strip(':')] = int(split_str[1])
        ret[split_str[2].strip(':')] = int(split_str[3])
        return ret

    check = CheckResult(connection, 'item_counts_by_type')
    # add random wait
    wait = round(random.uniform(0.1, random_wait), 1)
    time.sleep(wait)
    # run the check
    item_counts = {}
    warn_item_counts = {}
    req_location = ''.join([connection.ff_server, '/counts?format=json'])
    counts_res = ff_utils.authorized_request(req_location, auth=connection.ff_keys)
    if counts_res.status_code >= 400:
        check.status = 'ERROR'
        check.description = 'Error (bad status code %s) connecting to the counts endpoint at: %s.' % (counts_res.status_code, req_location)
        return check
    counts_json = json.loads(counts_res.text)
    for index in counts_json['db_es_compare']:
        counts = process_counts(counts_json['db_es_compare'][index])
        item_counts[index] = counts
        if counts['DB'] != counts['ES']:
            warn_item_counts[index] = counts
    # add ALL for total counts
    total_counts = process_counts(counts_json['db_es_total'])
    item_counts['ALL'] = total_counts
    # set fields, store result
    if not item_counts:
        check.status = 'FAIL'
        check.summary = check.description = 'Error on fourfront health page'
    elif warn_item_counts:
        check.status = 'WARN'
        check.summary = check.description = 'DB and ES item counts are not equal'
        check.brief_output = warn_item_counts
    else:
        check.status = 'PASS'
        check.summary = check.description = 'DB and ES item counts are equal'
    check.full_output = item_counts
    return check
