from dcicutils import ff_utils
import re
import requests
from foursight_core.checks.helpers import wrangler_utils

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


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
        check.status = 'WARN'
        check.summary = 'Pages with bad routes found'
        check.description = ('{} child pages whose route is not a direct sub-route of parent'
                             ''.format(sum([len(val) for val in problem_routes.values()])))
    else:
        check.status = 'PASS'
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
        check.status = 'WARN'
        check.summary = 'Validation errors found'
        check.description = ('{} items found with validation errors, comprising the following '
                             'item types: {}. \nFor search results see link below.'.format(
                                 len(results), ', '.join(list(types))))
        check.ff_link = connection.ff_server + search_url
    else:
        check.status = 'PASS'
        check.summary = 'No validation errors'
        check.description = 'No validation errors found.'
    return check


@check_function()
def check_submitted_md5(connection):
    """ Check that any submitted md5s are consistent with the ones we generated """
    check = CheckResult(connection, 'check_submitted_md5')

    search_url = 'search/?type=SubmittedFile&submitted_md5sum!=No+value&content_md5sum!=No+value&limit=500' \
                 '&field=submitted_md5sum&field=content_md5sum'
    results = ff_utils.search_metadata(search_url, key=connection.ff_keys)
    atids = {result['@id'] for result in results if result['submitted_md5sum'] != result['content_md5sum']}
    if atids:
        check.status = 'WARN'
        check.summary = 'Inconsistent Content Md5Sum(s) Found!'
        check.description = f'{len(atids)} items found with inconsistent md5sum, for results see below link.'
        check.full_output = {
            'items': atids
        }
        check.ff_link = connection.ff_server + search_url
    else:
        check.status = 'PASS'
        check.summary = 'No inconsistent md5sums.'
        check.description = 'No inconsistent md5sums.'
    return check


@check_function()
def check_for_new_submissions(connection):
    """ Weekly check that will compare against the previous week to determine if any new submissions
        need attention
    """
    check = CheckResult(connection, 'check_for_new_submissions')
    last_result = check.get_primary_result()
    search_url = 'search/?type=IngestionSubmission&submission_centers.display_title%21=HMS+DAC'
    results = ff_utils.search_metadata(search_url, key=connection.ff_keys)
    current_result_count = results['total']
    if not last_result:
        check.status = 'PASS'
        check.summary = 'First result - setting a baseline'
        check.full_output = {
            'submission_count': current_result_count
        }
    else:
        last_result_count = int(last_result['full_output']['submission_count'])
        if last_result_count <= current_result_count:
            check.status = 'PASS'
            check.summary = 'No change in submissions detected'
            check.full_output = {
                'submission_count': current_result_count
            }
        else:  # we detected an increase in submissions not touched by us
            check.status = 'WARN'
            check.summary = f'Detected {current_result_count - last_result_count} new submission for review'
            check.full_output = {
                'submission_count': current_result_count
            }
    return check
