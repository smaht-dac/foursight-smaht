from __future__ import print_function, unicode_literals
from .utils import get_methods_by_deco, check_method_deco, CHECK_DECO
from .checkresult import CheckResult
from .check_groups import *
import sys
import importlib
import datetime
import copy

# import modules that contain the checks
for check_mod in CHECK_MODULES:
    try:
        globals()[check_mod] = importlib.import_module('.'.join(['chalicelib', check_mod]))
    except ImportError:
        print(''.join(['ERROR importing checks from ', check_mod]), file=sys.stderr)


def get_check_strings(specific_check=None):
    """
    Return check formatted check strings (<module>/<check_name>) for checks.
    By default runs on all checks (specific_check == None), but can be used
    to get the check string of a certain check name as well.
    """
    all_checks = []
    for check_mod in CHECK_MODULES:
        if globals().get(check_mod):
            methods = get_methods_by_deco(globals()[check_mod], CHECK_DECO)
            for method in methods:
                check_str = '/'.join([check_mod, method.__name__])
                if specific_check and specific_check == method.__name__:
                    return check_str
                else:
                    all_checks.append(check_str)
    if specific_check:
        # if we've gotten here, it means the specific check was not checks_found
        return None
    else:
        return list(set(all_checks))


def run_check_group(connection, name):
    """
    For now return a simple list of check results
    """
    check_results = []
    check_group = fetch_check_group(name)
    if not check_group:
        return check_results
    group_timestamp = datetime.datetime.utcnow().isoformat()
    for check_info in check_group:
        if len(check_info) != 3:
            check_results.append(' '.join(['ERROR with', str(check_info), 'in group:', name]))
        else:
            # add uuid to each kwargs dict if not already specified
            # this will have the effect of giving all checks the same id
            # and combining results from repeats in the same check_group
            [check_str, check_kwargs, check_deps] = check_info
            if 'uuid' not in check_kwargs:
                check_kwargs['uuid'] = group_timestamp
            # nothing done with dependencies yet
            result = run_check(connection, check_str, check_kwargs)
            if result:
                check_results.append(result)
    return check_results


def get_check_group_latest(connection, name):
    """
    Initialize check results for each check in a group and get latest results,
    sorted alphabetically
    """
    latest_results = []
    check_group = fetch_check_group(name)
    if not check_group:
        return latest_results
    for check_info in check_group:
        if len(check_info) != 3:
            continue
        check_name = check_info[0].strip().split('/')[1]
        TempCheck = CheckResult(connection.s3_connection, check_name)
        found = TempCheck.get_latest_check()
        # checks with no records will return None
        if found:
            latest_results.append(found)
    # sort them alphabetically
    latest_results = sorted(latest_results, key=lambda v: v['name'].lower())
    return latest_results


def fetch_check_group(name):
    """
    Will be none if the group is not defined.
    Special case for all_checks, which gets all checks and uses default kwargs
    """
    if name == 'all':
        all_checks_strs = get_check_strings()
        all_checks_group = [[check_str, {}, []] for check_str in all_checks_strs]
        return all_checks_group
    group = CHECK_GROUPS.get(name, None)
    # maybe it's a test groups
    if not group:
        group = TEST_CHECK_GROUPS.get(name, None)
    # ensure it is non-empty list
    if not isinstance(group, list) or len(group) == 0:
        return None
    # copy it and return
    return copy.deepcopy(group)


def run_check(connection, check_str, check_kwargs):
    """
    Takes a FS_connection object, a check string formatted as: <str check module/name>
    and a dictionary of check arguments.
    For example:
    check_str: 'system_checks/my_check'
    check_kwargs: '{"foo":123}'
    Fetches the check function and runs it (returning whatever it returns)
    Return a string for failed results, CheckResult object otherwise
    """
    # make sure parameters are good
    error_str = ' '.join(['Info: CHECK:', str(check_str), 'KWARGS:', str(check_kwargs)])
    if len(check_str.strip().split('/')) != 2:
        return ' '.join(['ERROR. Check string must be of form module/check_name.', error_str])
    check_mod_str = check_str.strip().split('/')[0]
    check_name_str = check_str.strip().split('/')[1]
    if not isinstance(check_kwargs, dict):
        return ' '.join(['ERROR. Check kwargs must be a dict.', error_str])
    check_mod = globals().get(check_mod_str)
    if not check_mod:
        return ' '.join(['ERROR. Check module is not valid.', error_str])
    check_method = check_mod.__dict__.get(check_name_str)
    if not check_method:
        return ' '.join(['ERROR. Check name is not valid.', error_str])
    if not check_method_deco(check_method, CHECK_DECO):
        return ' '.join(['ERROR. Ensure the check_function decorator is present.', error_str])
    return check_method(connection, **check_kwargs)