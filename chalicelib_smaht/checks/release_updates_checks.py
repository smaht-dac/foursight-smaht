import os
from .helpers.google_utils import GoogleAPISyncer
import datetime

# Use confchecks to import decorators object and its methods for each check module
# rather than importing check_function, action_function, CheckResult, ActionResult
# individually - they're now part of class Decorators in foursight-core::decorators
# that requires initialization with foursight prefix.
from .helpers.confchecks import *


#### CHECKS / ACTIONS #####

@check_function(start_date=None, end_date=None)
def sync_google_analytics_data(connection, **kwargs):
    '''
    This checks the last time that analytics data was fetched (if any) and then
    triggers an action to fill up smaht with incremented google_analytics TrackingItems.

    TODO: No use case yet, but we could accept start_date and end_date here & maybe in action eventually.
    '''
    check = CheckResult(connection, 'sync_google_analytics_data')

    if os.environ.get('chalice_stage', 'dev') != 'prod':
        check.summary = check.description = 'This check only runs on Foursight prod'
        return check

    recent_passing_run = False
    recent_runs, total_unused = check.get_result_history(0, 20, after_date=datetime.datetime.now() - datetime.timedelta(hours=3))
    for run in recent_runs:
        # recent_runs is a list of lists. [status, None, kwargdict]
        # Status is at index 0.
        if run[0] == 'PASS':
            recent_passing_run = True
            break

    if recent_passing_run:
        check.summary = check.description = 'This check was run within last 3 hours; skipping because need time for TrackingItems to be indexed.'
        check.status = 'FAIL'
        return check

    google = GoogleAPISyncer(connection.ff_keys)

    action_logs = { 'daily_created' : [], 'monthly_created' : [] }

    res = google.analytics.fill_with_tracking_items('daily')
    action_logs['daily_created'] = res.get('created', [])

    res = google.analytics.fill_with_tracking_items('monthly')
    action_logs['monthly_created'] = res.get('created', [])

    check.full_output = action_logs
    check.status = 'PASS' if (len(action_logs['daily_created']) > 0 or len(action_logs['monthly_created']) > 0) else 'WARN'
    check.description = 'Created %s daily items and %s monthly Items.' % (str(len(action_logs['daily_created'])), str(len(action_logs['monthly_created'])))
    return check
