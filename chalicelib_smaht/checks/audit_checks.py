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
