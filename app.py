from __future__ import print_function, unicode_literals
from chalice import Chalice, Cron, Rate, Response
import json
import os
import requests
import datetime
from .chalicelib.app_utils import AppUtils

app = Chalice(app_name='foursight-cgap')
app.debug = True
STAGE = os.environ.get('chalice_stage', 'dev')
DEFAULT_ENV = 'cgap'

'''######### SCHEDULED FXNS #########'''


def effectively_never():
    """Every February 31st, a.k.a. 'never'."""
    return Cron('0', '0', '31', '2', '?', '?')


def end_of_day_on_weekdays():
    """ Cron schedule that runs at 6pm EST (22:00 UTC) on weekdays. Used for deployments. """
    return Cron('0', '22', '?', '*', 'MON-FRI', '*')

# this dictionary defines the CRON schedules for the dev and prod foursight
# stagger them to reduce the load on Fourfront. Times are UTC
# info: https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
# TODO: remove hardcoding of stage
foursight_cron_by_schedule = {
    'prod': {
        'ten_min_checks': Cron('0/10', '*', '*', '*', '?', '*'),
        'thirty_min_checks': Cron('0/30', '*', '*', '*', '?', '*'),
        'hourly_checks': Cron('0', '0/1', '*', '*', '?', '*'),
        'hourly_checks_2': Cron('15', '0/1', '*', '*', '?', '*'),
        'early_morning_checks': Cron('0', '8', '*', '*', '?', '*'),
        'morning_checks': Cron('0', '10', '*', '*', '?', '*'),
        'morning_checks_2': Cron('15', '10', '*', '*', '?', '*'),
        'monday_checks': Cron('0', '9', '?', '*', '2', '*'),
        'monthly_checks': Cron('0', '9', '1', '*', '?', '*'),
        'manual_checks': effectively_never(),
        'deployment_checks': end_of_day_on_weekdays()
    },
    'dev': {
        'ten_min_checks': Cron('5/10', '*', '*', '*', '?', '*'),
        'thirty_min_checks': Cron('15/30', '*', '*', '*', '?', '*'),
        'hourly_checks': Cron('30', '0/1', '*', '*', '?', '*'),
        'hourly_checks_2': Cron('45', '0/1', '*', '*', '?', '*'),
        'early_morning_checks': Cron('0', '8', '*', '*', '?', '*'),
        'morning_checks': Cron('30', '10', '*', '*', '?', '*'),
        'morning_checks_2': Cron('45', '10', '*', '*', '?', '*'),
        'monday_checks': Cron('30', '9', '?', '*', '2', '*'),
        'monthly_checks': Cron('30', '9', '1', '*', '?', '*'),
        'manual_checks': effectively_never(),
        'deployment_checks': end_of_day_on_weekdays()  # disabled, see schedule below
    }
}


@app.schedule(foursight_cron_by_schedule[STAGE]['ten_min_checks'])
def ten_min_checks(event):
    AppUtils.queue_scheduled_checks('all', 'ten_min_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['thirty_min_checks'])
def thirty_min_checks(event):
    AppUtils.queue_scheduled_checks('all', 'thirty_min_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['hourly_checks'])
def hourly_checks(event):
    AppUtils.queue_scheduled_checks('all', 'hourly_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['hourly_checks_2'])
def hourly_checks_2(event):
    AppUtils.queue_scheduled_checks('all', 'hourly_checks_2')


@app.schedule(foursight_cron_by_schedule[STAGE]['early_morning_checks'])
def early_morning_checks(event):
    AppUtils.queue_scheduled_checks('all', 'early_morning_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['morning_checks'])
def morning_checks(event):
    AppUtils.queue_scheduled_checks('all', 'morning_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['morning_checks_2'])
def morning_checks_2(event):
    AppUtils.queue_scheduled_checks('all', 'morning_checks_2')


@app.schedule(foursight_cron_by_schedule[STAGE]['monday_checks'])
def monday_checks(event):
    AppUtils.queue_scheduled_checks('all', 'monday_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['monthly_checks'])
def monthly_checks(event):
    AppUtils.queue_scheduled_checks('all', 'monthly_checks')


@app.schedule(foursight_cron_by_schedule[STAGE]['deployment_checks'])
def deployment_checks(event):
    if STAGE == 'dev':
        return  # do not schedule the deployment checks on dev
    AppUtils.queue_scheduled_checks('all', 'deployment_checks')


'''######### END SCHEDULED FXNS #########'''


@app.route('/callback')
def auth0_callback():
    """
    Special callback route, only to be used as a callback from auth0
    Will return a redirect to view on error/any missing callback info.
    """
    request = app.current_request
    req_dict = request.to_dict()
    domain, context = AppUtils.get_domain_and_context(req_dict)
    # extract redir cookie
    cookies = req_dict.get('headers', {}).get('cookie')
    redir_url = context + 'view/' + DEFAULT_ENV
    for cookie in cookies.split(';'):
        name, val = cookie.strip().split('=')
        if name == 'redir':
            redir_url = val
    resp_headers = {'Location': redir_url}
    params = req_dict.get('query_params')
    if not params:
        return AppUtils.forbidden_response()
    auth0_code = params.get('code', None)
    auth0_client = os.environ.get('CLIENT_ID', None)
    auth0_secret = os.environ.get('CLIENT_SECRET', None)
    if not (domain and auth0_code and auth0_client and auth0_secret):
        return Response(status_code=301, body=json.dumps(resp_headers),
                        headers=resp_headers)
    payload = {
        'grant_type': 'authorization_code',
        'client_id': auth0_client,
        'client_secret': auth0_secret,
        'code': auth0_code,
        'redirect_uri': ''.join(['https://', domain, context, 'callback/'])
    }
    json_payload = json.dumps(payload)
    headers = { 'content-type': "application/json" }
    res = requests.post("https://hms-dbmi.auth0.com/oauth/token", data=json_payload, headers=headers)
    id_token = res.json().get('id_token', None)
    if id_token:
        cookie_str = ''.join(['jwtToken=', id_token, '; Domain=', domain, '; Path=/;'])
        expires_in = res.json().get('expires_in', None)
        if expires_in:
            expires = datetime.datetime.utcnow() + datetime.timedelta(seconds=expires_in)
            cookie_str += (' Expires=' + expires.strftime("%a, %d %b %Y %H:%M:%S GMT") + ';')
        resp_headers['Set-Cookie'] = cookie_str
    return Response(status_code=302, body=json.dumps(resp_headers), headers=resp_headers)


@app.route('/', methods=['GET'])
def index():
    """
    Redirect with 302 to view page of DEFAULT_ENV
    Non-protected route
    """
    domain, context = get_domain_and_context(app.current_request.to_dict())
    resp_headers = {'Location': context + 'view/' + DEFAULT_ENV}
    return Response(status_code=302, body=json.dumps(resp_headers),
                    headers=resp_headers)


@app.route('/introspect', methods=['GET'])
def introspect(environ):
    """
    Test route
    """
    auth = AppUtils.check_authorization(app.current_request.to_dict(), environ)
    if auth:
        return Response(status_code=200, body=json.dumps(app.current_request.to_dict()))
    else:
        return AppUtils.forbidden_response()


@app.route('/view_run/{environ}/{check}/{method}', methods=['GET'])
def view_run_route(environ, check, method):
    """
    Protected route
    """
    req_dict = app.current_request.to_dict()
    domain, context = get_domain_and_context(req_dict)
    query_params = req_dict.get('query_params', {})
    if AppUtils.check_authorization(req_dict, environ):
        if method == 'action':
            return AppUtils.view_run_action(environ, check, query_params, context)
        else:
            return AppUtils.view_run_check(environ, check, query_params, context)
    else:
        return AppUtils.forbidden_response(context)


@app.route('/view/{environ}', methods=['GET'])
def view_route(environ):
    """
    Non-protected route
    """
    req_dict = app.current_request.to_dict()
    domain, context = get_domain_and_context(req_dict)
    return AppUtils.view_foursight(environ, AppUtils.check_authorization(req_dict, environ), domain, context)


@app.route('/view/{environ}/{check}/{uuid}', methods=['GET'])
def view_check_route(environ, check, uuid):
    """
    Protected route
    """
    req_dict = app.current_request.to_dict()
    domain, context = get_domain_and_context(req_dict)
    if AppUtils.check_authorization(req_dict, environ):
        return AppUtils.view_foursight_check(environ, check, uuid, True, domain, context)
    else:
        return AppUtils.forbidden_response()


@app.route('/history/{environ}/{check}', methods=['GET'])
def history_route(environ, check):
    """
    Non-protected route
    """
    # get some query params
    req_dict = app.current_request.to_dict()
    query_params = req_dict.get('query_params')
    start = int(query_params.get('start', '0')) if query_params else 0
    limit = int(query_params.get('limit', '25')) if query_params else 25
    domain, context = get_domain_and_context(req_dict)
    return AppUtils.view_foursight_history(environ, check, start, limit,
                                  AppUtils.check_authorization(req_dict, environ), domain, context)


@app.route('/checks/{environ}/{check}/{uuid}', methods=['GET'])
def get_check_with_uuid_route(environ, check, uuid):
    """
    Protected route
    """
    if AppUtils.check_authorization(app.current_request.to_dict(), environ):
        return AppUtils.run_get_check(environ, check, uuid)
    else:
        return AppUtils.forbidden_response()


@app.route('/checks/{environ}/{check}', methods=['GET'])
def get_check_route(environ, check):
    """
    Protected route
    """
    if AppUtils.check_authorization(app.current_request.to_dict(), environ):
        return AppUtils.run_get_check(environ, check, None)
    else:
        return AppUtils.forbidden_response()


@app.route('/checks/{environ}/{check}', methods=['PUT'])
def put_check_route(environ, check):
    """
    Take a PUT request. Body of the request should be a json object with keys
    corresponding to the fields in CheckResult, namely:
    title, status, description, brief_output, full_output, uuid.
    If uuid is provided and a previous check is found, the default
    behavior is to append brief_output and full_output.

    Protected route
    """
    request = app.current_request
    if AppUtils.check_authorization(request.to_dict(), environ):
        put_data = request.json_body
        return AppUtils.run_put_check(environ, check, put_data)
    else:
        return AppUtils.forbidden_response()


@app.route('/environments/{environ}', methods=['PUT'])
def put_environment(environ):
    """
    Take a PUT request that has a json payload with 'fourfront' (ff server)
    and 'es' (es server).
    Attempts to generate an new environment and runs all checks initially
    if successful.

    Protected route
    """
    request = app.current_request
    if AppUtils.check_authorization(request.to_dict(), environ):
        env_data = request.json_body
        return AppUtils.run_put_environment(environ, env_data)
    else:
        return AppUtils.forbidden_response()


@app.route('/environments/{environ}', methods=['GET'])
def get_environment_route(environ):
    """
    Protected route
    """
    if AppUtils.check_authorization(app.current_request.to_dict(), environ):
        return AppUtils.run_get_environment(environ)
    else:
        return AppUtils.forbidden_response()


@app.route('/environments/{environ}/delete', methods=['DELETE'])
def delete_environment(environ):
    """
    Takes a DELETE request and purges the foursight environment specified by 'environ'.
    NOTE: This only de-schedules all checks, it does NOT wipe data associated with this
    environment - that can only be done directly from S3 (for safety reasons).

    Protected route
    """
    if AppUtils.check_authorization(app.current_request.to_dict(), environ):  # TODO (C4-138) Centralize authorization check
        return AppUtils.run_delete_environment(environ)
    else:
        return AppUtils.forbidden_response()


######### PURE LAMBDA FUNCTIONS #########

@app.lambda_function()
def check_runner(event, context):
    """
    Pure lambda function to pull run and check information from SQS and run
    the checks. Self propogates. event is a dict of information passed into
    the lambda at invocation time.
    """
    if not event:
        return
    AppUtils.run_check_runner(event)

######### MISC UTILITY FUNCTIONS #########


def set_stage(stage):
    from foursight_core.deploy import CONFIG_BASE
    if stage != 'test' and stage not in CONFIG_BASE['stages']:
        print('ERROR! Input stage is not valid. Must be one of: %s' % str(list(CONFIG_BASE['stages'].keys()).extend('test')))
    os.environ['chalice_stage'] = stage


def set_timeout(timeout):
    from foursight_core.chalicelib.utils import CHECK_TIMEOUT
    try:
        timeout = int(timeout)
    except ValueError:
        print('ERROR! Timeout must be an integer. You gave: %s' % timeout)
    else:
        CHECK_TIMEOUT = timeout
