from dcicutils import ff_utils
# from foursight_core.checks.helpers import wrangler_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *
from .helpers import constants

STATUS_LEVEL = {
    'released': 4,
    'archived': 4,
    'current': 4,
    'revoked': 4,
    'released to project': 3,
    'pre-release': 2,
    'restricted': 4,
    'planned': 2,
    'archived to project': 3,
    'in review by lab': 1,
    'released to lab': 1,
    'submission in progress': 1,
    'to be uploaded by workflow': 1,
    'uploading': 1,
    'uploaded': 1,
    'upload failed': 1,
    'draft': 1,
    'deleted': 0,
    'replaced': 0,
    'obsolete': 0,
}


@check_function()
def page_children_routes(connection, **kwargs):
    check = CheckResult(connection, 'page_children_routes')

    page_search = 'search/?type=Page&format=json&children.name%21=No+value'
    results = ff_utils.search_metadata(page_search, key=connection.ff_keys)
    problem_routes = {}
    for result in results:
        if result['name'] != 'resources/data-collections':
            bad_children = [child['name'] for child in result['children'] if
                            child['name'] != result['name'] + '/' + child['name'].split('/')[-1]]
            if bad_children:
                problem_routes[result['name']] = bad_children

    if problem_routes:
        check.status = constants.CHECK_WARN
        check.summary = 'Pages with bad routes found'
        check.description = ('{} child pages whose route is not a direct sub-route of parent'
                             ''.format(sum([len(val) for val in problem_routes.values()])))
    else:
        check.status = constants.CHECK_PASS
        check.summary = 'No pages with bad routes'
        check.description = 'All routes of child pages are a direct sub-route of parent page'
    check.full_output = problem_routes
    return check


@check_function()
def check_validation_errors(connection, **kwargs):
    '''
    Counts number of items in fourfront with schema validation errors,
    returns link to search if found.
    '''
    check = CheckResult(connection, 'check_validation_errors')

    search_url = 'search/?validation_errors.name!=No+value&type=Item'
    results = ff_utils.search_metadata(search_url + '&field=@id', key=connection.ff_keys)
    if results:
        types = {item for result in results for item in result['@type'] if item != 'Item'}
        check.status = constants.CHECK_WARN
        check.summary = 'Validation errors found'
        check.description = ('{} items found with validation errors, comprising the following '
                             'item types: {}. \nFor search results see link below.'.format(
                                 len(results), ', '.join(list(types))))
        check.ff_link = connection.ff_server + search_url
    else:
        check.status = constants.CHECK_PASS
        check.summary = 'No validation errors'
        check.description = 'No validation errors found.'
    return check


@check_function(last_mod_date=None)
def check_submitted_md5(connection, **kwargs):
    """ Check that any submitted md5s are consistent with the ones we generated """
    check = CheckResult(connection, 'check_submitted_md5')

    if not (last_mod_date := kwargs.get('last_mod_date')):
        last_result = check.get_primary_result()
        if last_result:
            lr_uuid = last_result.get('uuid')
            last_mod_date = lr_uuid.split("T")[0]

    search_stem = 'search/?type=SubmittedFile&submitted_md5sum!=No+value&md5sum!=No+value'
    if last_mod_date and last_mod_date != 'check_all':
        search_stem += f'&last_modified.date_modified.from={last_mod_date}'
    search_url = search_stem + '&field=submitted_md5sum&field=md5sum'
    results = ff_utils.search_metadata(search_url, key=connection.ff_keys, is_generator=True)
    atids = {result['@id'] for result in results if result['submitted_md5sum'] != result['md5sum']}
    if atids:
        check.status = constants.CHECK_WARN
        check.summary = 'Inconsistent Content Md5Sum(s) Found!'
        check.description = f'{len(atids)} items found with inconsistent md5sum, for results see below link.'
        check.full_output = {
            'items': list(atids)
        }
        check.ff_link = connection.ff_server + search_stem
    else:
        check.status = constants.CHECK_PASS
        check.summary = 'No inconsistent md5sums.'
        check.description = 'No inconsistent md5sums.'
    return check


@check_function()
def check_for_new_submissions(connection, **kwargs):
    """ Weekly check that will compare against the previous week to determine if any new submissions
        need attention
    """
    check = CheckResult(connection, 'check_for_new_submissions')
    last_result = check.get_primary_result()
    search_url = f'search/?type=IngestionSubmission&submission_centers.display_title%21={constants.DAC_NAME}'
    results = ff_utils.search_metadata(search_url, key=connection.ff_keys)
    current_result_count = len(results)
    if not last_result or last_result.get('status') == constants.CHECK_ERROR:
        check.status = constants.CHECK_PASS
        check.summary = 'First result - setting a baseline'
        check.full_output = {
            'submission_count': current_result_count
        }
    else:
        last_result_count = int(last_result['full_output'].get('submission_count', 0))
        if last_result_count >= current_result_count:
            check.status = constants.CHECK_PASS
            check.summary = 'No change in submissions detected'
            check.full_output = {
                'submission_count': current_result_count
            }
        else:  # we detected an increase in submissions not touched by us
            check.status = constants.CHECK_WARN
            check.summary = f'Detected {current_result_count - last_result_count} new submission for review'
            check.full_output = {
                'submission_count': current_result_count
            }
    return check


@check_function(last_mod_date=None)
def check_tissue_sample_properties(connection, **kwargs):
    """Weekly check of GCC-submitted tissue samples to make sure they match the metadata
        from corresponding TPC-submitted tissue sample item.
    """
    check = CheckResult(connection, 'check_tissue_sample_properties')
    # if true will run on all replicate sets

    last_mod_date = kwargs['last_mod_date']
    check_properties = [
        "category",
        "sample_sources",
        "preservation_type",
        "core_size"
    ]
    incorrect = []
    search_url = "search/?type=TissueSample&submission_centers.display_title=NDRI+TPC"
    if last_mod_date:
        search_url += f"&last_modified.date_modified.from={last_mod_date}"
    else:
        search_url += "&limit=500"
    tpc_tissue_samples = ff_utils.search_metadata(search_url,key=connection.ff_keys)
    for tissue_sample in tpc_tissue_samples:
        external_id = tissue_sample['external_id']
        gcc_search_url = f"search/?type=TissueSample&submission_centers.display_title!=NDRI+TPC&external_id={external_id}"
        gcc_tissue_samples = ff_utils.search_metadata(gcc_search_url,key=connection.ff_keys)
        gcc_tissue_sample = gcc_tissue_samples[0]
        if len(gcc_tissue_samples) > 1:
            for dup_sample in gcc_tissue_samples:
                incorrect.append({'uuid': dup_sample['uuid'],
                    '@id': dup_sample['@id'],
                    'description': dup_sample.get('description'),
                    'error': f"Multiple tissue sample items for one TPC-submitted tissue sample {tissue_sample.get('accession')}"})
        for check_property in check_properties:
            if check_property in gcc_tissue_sample and check_property in tissue_sample:
                if gcc_tissue_sample[check_property] != tissue_sample[check_property]:
                    incorrect.append({'uuid': gcc_tissue_sample['uuid'],
                        '@id': gcc_tissue_sample['@id'],
                        'description': gcc_tissue_sample.get('description'),
                        check_property: gcc_tissue_sample.get(check_property),
                        'error': f"Metadata properties inconsistent with TPC-submitted tissue sample {tissue_sample.get('accession')}"})
    check.full_output = incorrect
    check.brief_output = [item['@id'] for item in incorrect]
    if incorrect:
        check.status = 'WARN'
        check.summary = 'TissueSample found with inconsistent metadata'
    else:
        check.status = 'PASS'
        check.summary = 'No tissue samples with inconsistent metadata'
    return check
