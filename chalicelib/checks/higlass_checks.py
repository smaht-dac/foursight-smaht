from __future__ import print_function, unicode_literals
from datetime import datetime, timedelta
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils
import requests
import json
import time
from copy import deepcopy

def get_reference_files(connection):
    """
    Find all of the tagged reference files needed to create Higlass view configs.

    Args:
        connection: The connection to Fourfront.

    Returns:
        Returns a dictionary of reference files.
            Each key is the genome assembly (examples: GRCm38, GRCh38)
            Each value is a list of uuids.
    """
    # first, find and cache the reference files
    reference_files_by_ga = {}
    ref_search_q = '/search/?type=File&tags=higlass_reference&higlass_uid!=No+value&genome_assembly!=No+value&file_format.file_format=beddb&file_format.file_format=chromsizes&field=genome_assembly&field=file_format&field=accession'
    ref_res = ff_utils.search_metadata(ref_search_q, key=connection.ff_keys, ff_env=connection.ff_env)
    for ref in ref_res:
        # file_format should be 'chromsizes' or 'beddb'
        ref_format = ref.get('file_format', {}).get('file_format')

        # cache reference files by genome_assembly
        if ref['genome_assembly'] not in reference_files_by_ga:
            reference_files_by_ga[ref['genome_assembly']] = []
        reference_files_by_ga[ref['genome_assembly']].append(ref['accession'])
    return reference_files_by_ga

def post_viewconf_to_visualization_endpoint(connection, reference_files, files, lab_uuid, contributing_labs, award_uuid, title, description, ff_auth, headers):
    """
    Given the list of files, contact fourfront and generate a higlass view config.
    Then post the view config.
    Returns the viewconf uuid upon success, or None otherwise.

    Args:
        connection              : The connection to Fourfront.
        reference_files(dict)   : Reference files, stored by genome assembly (see get_reference_files)
        files(list)             : A list of file objects.
        lab_uuid(string)        : Lab uuid to assigned to the Higlass viewconf.
        contributing_labs(list) : A list of uuids referring to the contributing labs to assign to the Higlass viewconf.
        award_uuid(string)      : Award uuid to assigned to the Higlass viewconf.
        title(string)           : Higlass view config title.
        description(string)     : Higlass view config description.
        ff_auth(dict)           : Authorization needed to post to Fourfront.
        headers(dict)           : Header information needed to post to Fourfront.

    Returns:
        A dictionary:
            view_config_uuid: string referring to the new Higlass view conf uuid if it succeeded. None otherwise.
            error: string describing the error (blank if there is no error.)
    """
    # start with the reference files and add the target files
    file_accessions = [ f["accession"] for f in files ]
    to_post = {'files': reference_files + file_accessions}

    view_conf_uuid = None
    # post to the visualization endpoint
    ff_endpoint = connection.ff_server + 'add_files_to_higlass_viewconf/'
    res = requests.post(ff_endpoint, data=json.dumps(to_post),
                        auth=ff_auth, headers=headers)

    # Handle the response.
    if res and res.json().get('success', False):
        view_conf = res.json()['new_viewconfig']

        # Get the new status.
        viewconf_status = get_viewconf_status(files)

        # Post the new view config.
        viewconf_description = {
            "award" : award_uuid,
            "contributing_labs" : contributing_labs,
            "genome_assembly": res.json()['new_genome_assembly'],
            "lab" : lab_uuid,
            "status": viewconf_status,
            "viewconfig": view_conf,
        }

        if description:
            viewconf_description["description"] = description
        if title:
            viewconf_description["title"] = title

        try:
            viewconf_res = ff_utils.post_metadata(viewconf_description, 'higlass-view-configs',
                                                  key=connection.ff_keys, ff_env=connection.ff_env)
            view_conf_uuid = viewconf_res['@graph'][0]['uuid']
            return {
                "view_config_uuid": view_conf_uuid,
                "error": ""
            }
        except Exception as e:
            return {
                "view_config_uuid": None,
                "error": str(e)
            }
    else:
        if res:
            return {
                "view_config_uuid": None,
                "error": res.json()["errors"]
            }

        return {
            "view_config_uuid": None,
            "error": "Could not contact visualization endpoint."
        }

def get_viewconf_status(files):
    """
    Determine the Higlass viewconf's status based on the files used to compose it.

    Args:
        files(list)             : A list of file objects that contain a status.

    Returns:
        A string.
    """

    # The viewconf will be in "released to lab" status if any file:
    # - Lacks a status
    # - Has one of the "released to lab" statuses
    # - Doesn't have a "released" or "released to project" status
    released_to_lab = [
        "uploading",
        "uploaded",
        "upload failed",
        "deleted",
        "replaced",
        "revoked",
        "archived",
        "pre-release",
        "to be uploaded by workflow"
    ]
    if any([ f["accession"] for f in files if f.get("status", None) in released_to_lab ]):
        return "released to lab"

    # If any file is in "released to project" the viewconf will also have that status.
    released_to_project = [
        "released to project",
        "archived to project",
    ]
    if any([ f["accession"] for f in files if f["status"] in released_to_project]):
        return "released to project"

    # All files are "released" so the viewconf is also released.
    return "released"

def add_viewconf_static_content_to_file(connection, item_uuid, higlass_item_uuid, static_content_section, sc_location):
    """
    Add some static content for the item that shows the view config created for it.
    Returns True upon success.

    Args:
        connection                  : The connection to Fourfront.
        item_uuid(str)              : Identifier for the item.
        higlass_item_uuid(str)      : Identifier for the Higlass Item.
        static_content_section(list): The current static content section for this item.
        sc_location(str)            : Name for the new Static Content's location field.

    Returns:
        boolean. True indicates success.
        string. Contains the error (or an empty string if there is no error.)
    """
    new_sc_section = {
        'location': sc_location,
        'content': higlass_item_uuid,
        'description': 'auto_generated_higlass_view_config'
    }

    patched_static_content = static_content_section
    # Look through the static content to see if this section exists already.
    reuse_existing = False
    for sc in patched_static_content:
        if sc["description"] == "auto_generated_higlass_view_config":
            sc.update(new_sc_section)
            reuse_existing = True
            break

    # If there is no existing Higlass static content, add the new content to the existing static_content
    if not reuse_existing:
        patched_static_content = static_content_section + [new_sc_section]

    try:
        ff_utils.patch_metadata(
            {'static_content': patched_static_content},
            obj_id=item_uuid,
            key=connection.ff_keys,
            ff_env=connection.ff_env
        )
    except Exception as e:
        return False, str(e)
    return True, ""

@check_function()
def check_higlass_items_for_new_files(connection, **kwargs):
    """
    Find files without Higlass Items.

    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        check results object.
    """

    return find_files_requiring_higlass_items(
        connection,
        check_name="check_higlass_items_for_new_files",
        action_name="patch_higlass_items_for_new_files",
        search_queries=["&tags!=higlass_reference&static_content.description!=auto_generated_higlass_view_config&file_type!=read+positions"],
    )

@action_function()
def patch_higlass_items_for_new_files(connection, **kwargs):
    """ Create Higlass Items for Files indicated in check_higlass_items_for_new_files.

    Args:
        connection: The connection to Fourfront.
        **kwargs, which may include:
            called_by(optional, string, default=None): uuid of the associated check. If None, use the primary check

    Returns:
        An action object.
    """

    return create_higlass_items_for_files(
        connection,
        called_by = kwargs.get('called_by', None),
        check_name="check_higlass_items_for_new_files",
        action_name="patch_higlass_items_for_new_files",
    )

@check_function()
def check_higlass_items_for_modified_files(connection, **kwargs):
    """
    Find files modified since the last time the check was run.

    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        check results object.
    """

    return find_files_requiring_higlass_items(
        connection,
        check_name="check_higlass_items_for_modified_files",
        action_name="patch_higlass_items_for_modified_files",
        search_queries=["&tags!=higlass_reference&last_modified.date_modified.from=<get_latest_action_completed_date>"],
    )

@action_function()
def patch_higlass_items_for_modified_files(connection, **kwargs):
    """ Create Higlass Items for Files indicated in check_higlass_items_for_modified_files.

    Args:
        connection: The connection to Fourfront.
        **kwargs, which may include:
            called_by(optional, string, default=None): uuid of the associated check. If None, use the primary check

    Returns:
        An action object.
    """

    return create_higlass_items_for_files(
        connection,
        called_by = kwargs.get('called_by', None),
        check_name="check_higlass_items_for_modified_files",
        action_name="patch_higlass_items_for_modified_files",
    )

@check_function(search_queries=[])
def check_higlass_items_for_queried_files(connection, **kwargs):
    """
    Create or Update HiGlass Items for files found in the given query.

    Args:
        connection: The connection to Fourfront.
        **kwargs, which may include:
            search_queries(list, optional, default=[]): A list of search queries. All Files found in at least one of the queries will be modified.

    Returns:
        check results object.
    """

    search_queries = kwargs.get('search_queries', [])

    return find_files_requiring_higlass_items(
        connection,
        check_name="check_higlass_items_for_queried_files",
        action_name="patch_higlass_items_for_queried_files",
        search_queries=search_queries,
    )

@action_function()
def patch_higlass_items_for_queried_files(connection, **kwargs):
    """ Create Higlass Items for Files indicated in check_higlass_items_for_queried_files.

    Args:
        connection: The connection to Fourfront.
        **kwargs, which may include:
            called_by(optional, string, default=None): uuid of the associated check. If None, use the primary check

    Returns:
        An action object.
    """
    return create_higlass_items_for_files(
        connection,
        called_by = kwargs.get('called_by', None),
        check_name="check_higlass_items_for_queried_files",
        action_name="patch_higlass_items_for_queried_files",
    )

def find_files_requiring_higlass_items(connection, check_name, action_name, search_queries):
    """
    Check to generate Higlass Items for appropriate files.

    Args:
        check_name(string): Name of Foursight check.
        action_name(string): Name of related Foursight action.
        search_queries(list, optional, default=[]): A list of search queries. All Files found in at least one of the queries will be modified.

    Returns:
        check results object.
    """

    # Create the initial check
    check = init_check_res(connection, check_name)
    check.full_output = {
        "search_queries":[]
    }
    check.queries = []
    check.action = action_name

    # If no search query was provided, fail
    if not search_queries:
        check.summary = check.description = "Search queries must be provided."
        check.status = 'FAIL'
        check.allow_action = False
        return check

    # Add the fields we want to return.
    fields_to_include = '&field=' + '&field='.join((
        'accession',
        'award.uuid',
        'genome_assembly',
        'lab.uuid',
        'contributing_labs.uuid',
        'static_content',
        'status',
        'track_and_facet_info.track_title',
    ))

    files_by_accession = {}
    # Use all of the search queries to make a list of the ExpSets we will work on.
    for query in search_queries:
        # Interpolate the timestamps, if needed
        query = interpolate_query_check_timestamps(connection, query, action_name, check)

        check.full_output["search_queries"].append(query)

        # Add to base search
        file_search_query = "/search/?type=File&higlass_uid!=No+value&genome_assembly!=No+value" + query + fields_to_include

        # Query the files
        search_res = ff_utils.search_metadata(file_search_query, key=connection.ff_keys, ff_env=connection.ff_env)

        # Collate the results into a dict of ExpSets, ordered by accession
        for found_file in search_res:
            files_by_accession[ found_file["accession"] ] = found_file

    # Look through the search results for files to change.
    target_files_by_ga = {}
    for hg_file in files_by_accession.values():
        accession = hg_file["accession"]
        genome_assembly = hg_file["genome_assembly"]
        contributing_labs = [ cl["uuid"] for cl in hg_file.get("contributing_labs", []) ]

        # Determine the track title.
        track_title = ""
        if "track_and_facet_info" in hg_file and "track_title" in hg_file["track_and_facet_info"]:
            track_title = hg_file["track_and_facet_info"]["track_title"]

        if genome_assembly not in target_files_by_ga:
            target_files_by_ga[genome_assembly] = {}

        target_files_by_ga[genome_assembly][accession] = {
            "accession" : accession,
            "award" : hg_file["award"]["uuid"],
            "contributing_labs" : contributing_labs,
            "lab" : hg_file["lab"]["uuid"],
            "static_content" : hg_file.get("static_content", []),
            "status" : hg_file["status"],
            "track_title" : track_title,
        }

    # Get the reference files
    reference_files_by_ga = get_reference_files(connection)
    check.full_output['reference_files'] = reference_files_by_ga

    # Check for missing reference files
    for ga in target_files_by_ga:
        if ga in reference_files_by_ga and len(reference_files_by_ga[ga]) >= 2:
            full_output_key = "ready"
        else:
            full_output_key = "missing_reference_files"
        if full_output_key not in check.full_output:
            check.full_output[full_output_key] = {}
        check.full_output[full_output_key][ga] = target_files_by_ga[ga]

    if not target_files_by_ga:
        # nothing new to generate
        check.summary = check.description = "No new view configs to generate"
        check.allow_action = False
        check.status = 'PASS'
    else:
        all_files = sum([len(x) for x in check.full_output["ready"].values()])
        check.summary = "Ready to generate %s Higlass view configs" % all_files
        check.description = check.summary + ". See full_output for details."
        check.allow_action = True
        check.status = 'WARN'
    return check

def create_higlass_items_for_files(connection, check_name, action_name, called_by):
    """ This action uses the results from check_files_for_higlass_viewconf
    to create or update new Higlass Items for the given Files.

    Args:
        connection: The connection to Fourfront.
        check_name(string): Name of Foursight check.
        action_name(string): Name of related Foursight action.
        called_by(string, optional, default=None): uuid of the check this action is associated with.
            If None, use the primary result.

    Returns:
        An action object.
    """
    action = init_action_res(connection, action_name)
    action_logs = {
        "success": {},
        "failed_to_create_higlass" : {},
        "failed_to_patch_file" : {},
    }

    # get latest results
    gen_check = init_check_res(connection, check_name)
    if called_by:
        gen_check_result = gen_check.get_result_by_uuid(called_by)
    else:
        gen_check_result = gen_check.get_primary_result()

    # make the fourfront auth key (in basic auth format)
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    reference_files_by_ga = gen_check_result['full_output'].get('reference_files', {})

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # these are the files we care about
    # loop by genome_assembly
    target_files_by_ga = gen_check_result['full_output'].get("ready", {})
    for ga in target_files_by_ga:
        if time_expired:
            break

        if ga not in reference_files_by_ga:
            # reference files not found
            if "missing_reference_files" not in action_logs:
                action_logs["missing_reference_files"] = {}
            action_logs["missing_reference_files"][ga] = target_files_by_ga[ga]
            continue

        ref_files = reference_files_by_ga[ga]

        for file_accession, file_info in target_files_by_ga[ga].items():
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            static_content_section = file_info["static_content"]
            # If the static_content has a higlass section, replace it with just the uuid. Posting or Patching only wants content uuid.
            for sc in [sc for sc in static_content_section if sc['description'] == 'auto_generated_higlass_view_config']:
                sc["content"] = sc["content"]["uuid"]

            status = file_info["status"]
            track_title = file_info["track_title"]

            # Post a new Higlass viewconf using the file list
            higlass_title = "{acc}".format(acc=file_accession)
            if file_info["track_title"]:
                higlass_title += " - " + file_info["track_title"]

            existing_higlass_uuid = None

            sc_uuids = [ sc["content"] for sc in file_info["static_content"] if sc["location"] == "tab:higlass"]
            if sc_uuids:
                existing_higlass_uuid = sc_uuids[0]

            higlass_item_results = create_or_update_higlass_item(
                connection,
                files={
                    "reference":ref_files,
                    "content":[file_info],
                },
                higlass_item={
                    "uuid":existing_higlass_uuid,
                    "title":higlass_title,
                    "desc":"",
                },
                ff_requests_auth={
                    "ff_auth": ff_auth,
                    "headers": headers,
                },
                attributions={
                    "lab": file_info["lab"],
                    "award": file_info["award"],
                    "contributing_labs": file_info["contributing_labs"],
                }
            )

            # If we failed to create/update the viewconf, leave an error here
            if higlass_item_results["error"]:
                action_logs['failed_to_create_higlass'][file_accession] = higlass_item_results["error"]
                continue

            # Create a new static content section with the description = "auto_generated_higlass_view_config" and the new viewconf as the content
            # Patch the ExpSet static content
            successful_patch, patch_error = add_viewconf_static_content_to_file(
                connection,
                file_accession,
                higlass_item_results["item_uuid"],
                static_content_section,
                "tab:higlass"
            )

            if not successful_patch:
                action_logs['failed_to_patch_file'][file_accession] = patch_error
                continue

            action_logs["success"][file_accession] = higlass_item_results["item_uuid"]

    action.status = 'DONE'
    action.output = action_logs

    target_files_by_ga = gen_check_result['full_output'].get('target_files', {})
    file_count = sum([len(target_files_by_ga[ga]) for ga in target_files_by_ga])
    action.progress = "Created Higlass viewconfs for {completed} out of {possible} files".format(
        completed=len(action_logs["success"].keys()),
        possible=file_count
    )
    action.output["completed_timestamp"] = datetime.utcnow().isoformat()
    return action

def gather_processedfiles_for_expset(expset):
    """Collects all of the files for processed files.

    Args:
        expset(dict): Contains the embedded Experiment Set data.

    Returns:
    A dictionary with the following keys:
        genome_assembly(string, optional, default=""): The genome assembly all
            of the files use. Blank if there is an error or no files are found.
        files(list)                         : A list of identifiers for the
            discovered files.
        auto_generated_higlass_view_config(string, optional, default=None): Returns the uuid of the Higlass Item generated by a previous check.
        manual_higlass_view_config(string, optional, default=None): Returns the uuid of the Higlass Item that wasn't automatically generated.
        error(string, optional, default="") : Describes any errors generated.
    """

    # Collect all of the Processed files with a higlass uid.
    processed_files = []

    if "processed_files" in expset:
        # The Experiment Set may have Processed Files.
        processed_files = [ pf for pf in expset["processed_files"] if "higlass_uid" in pf ]

    # Search each Experiment, they may have Processed Files.
    if "experiments_in_set" in expset:
        for experiment in [ exp for exp in expset["experiments_in_set"] if "processed_files" in exp]:
            exp_processed_files = [ pf for pf in experiment["processed_files"] if "higlass_uid" in pf ]
            processed_files += exp_processed_files

    if len(processed_files) < 1:
        return {
            "error": "No processed files found",
            "files": [],
            "genome_assembly": "",
        }

    # Make sure all of them have the same genome assembly.
    genome_assembly_set = { pf["genome_assembly"] for pf in processed_files if "genome_assembly" in pf }

    if len(genome_assembly_set) > 1:
        return {
            "error": "Too many genome assemblies {gas}".format(gas=genome_assembly_set),
            "files": [],
            "genome_assembly": ""
        }

    # Return all of the processed files.
    unique_accessions = { pf["accession"] for pf in processed_files }

    unique_files = [{ "accession":pf["accession"], "status":pf["status"] } for pf in processed_files ]

    # Get the higlass uuid, if an auto generated view conf already exists.
    auto_generated_higlass_view_config = None
    manual_higlass_view_config = None
    if expset.get("static_content", None):
        processed_file_tabs = [ sc for sc in expset["static_content"] if sc["location"] == "tab:processed-files" ]

        auto_processed_file_tabs = [ sc for sc in processed_file_tabs if sc["description"] ==  "auto_generated_higlass_view_config" ]

        if auto_processed_file_tabs:
            auto_generated_higlass_view_config = auto_processed_file_tabs[0]["content"]["uuid"]
        elif processed_file_tabs:
            manual_higlass_view_config = processed_file_tabs[0]["uuid"]

    return {
        "error": "",
        "files": unique_files,
        "auto_generated_higlass_view_config": auto_generated_higlass_view_config,
        "manual_higlass_view_config": manual_higlass_view_config,
        "genome_assembly": processed_files[0]["genome_assembly"]
    }

@check_function()
def check_expsets_processedfiles_for_new_higlass_items(connection, **kwargs):
    """ Search for Higlass Items from Experiment Set Processed Files that need to be updated.
        ExpSets are chosen based on the search queries.

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:

        Returns:
            check result object.
    """
    return find_expsets_processedfiles_requiring_higlass_items(
        connection,
        check_name="check_expsets_processedfiles_for_new_higlass_items",
        action_name="patch_expsets_processedfiles_for_new_higlass_items",
        search_queries=[
            "&processed_files.higlass_uid%21=No+value&static_content.description!=auto_generated_higlass_view_config",
            "&experiments_in_set.processed_files.higlass_uid%21=No+value&static_content.description!=auto_generated_higlass_view_config"
        ]
    )

@action_function()
def patch_expsets_processedfiles_for_new_higlass_items(connection, **kwargs):
    """ Update the Experiment Sets Higlass Items for its Processed Files.

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                called_by(optional, string, default=None): uuid of the associated check. If None, use the primary check

        Returns:
            action object.
    """
    return update_expsets_processedfiles_requiring_higlass_items(
        connection,
        called_by = kwargs.get('called_by', None),
        check_name="check_expsets_processedfiles_for_new_higlass_items",
        action_name="patch_expsets_processedfiles_for_new_higlass_items"
    )

@check_function()
def check_expsets_processedfiles_for_modified_higlass_items(connection, **kwargs):
    """ Search for Higlass Items from Experiment Set Processed Files that need to be updated.
        ExpSets are chosen based on the search queries.

        Args:
            connection: The connection to Fourfront.
            **kwargs

        Returns:
            check result object.
    """
    return find_expsets_processedfiles_requiring_higlass_items(
        connection,
        check_name="check_expsets_processedfiles_for_modified_higlass_items",
        action_name="patch_expsets_processedfiles_for_modified_higlass_items",
        search_queries=[
            "&experiments_in_set.processed_files.higlass_uid%21=No+value&experiments_in_set.last_modified.date_modified.from=<get_latest_action_completed_date>",
            "&processed_files.higlass_uid%21=No+value&last_modified.date_modified.from=<get_latest_action_completed_date>",
            "&experiments_in_set.processed_files.higlass_uid%21=No+value&experiments_in_set.processed_files.last_modified.date_modified.from=<get_latest_action_completed_date>"
        ]
    )

@action_function()
def patch_expsets_processedfiles_for_modified_higlass_items(connection, **kwargs):
    """ Update the Experiment Sets Higlass Items for its Processed Files.

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                called_by(optional, string, default=None): uuid of the associated check. If None, use the primary check

        Returns:
            action object.
    """
    return update_expsets_processedfiles_requiring_higlass_items(
        connection,
        called_by = kwargs.get('called_by', None),
        check_name="check_expsets_processedfiles_for_modified_higlass_items",
        action_name="patch_expsets_processedfiles_for_modified_higlass_items"
    )

@check_function(search_queries=[])
def check_expsets_processedfiles_for_queried_higlass_items(connection, **kwargs):
    """ Search for Higlass Items from Experiment Set Processed Files that need to be updated.
        ExpSets are chosen based on the search queries.

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                search_queries(list, optional, default=[]): A list of search queries. All ExpSets found in at least one of the queries will be modified.

        Returns:
            check result object.
    """
    search_queries = kwargs.get('search_queries', [])

    return find_expsets_processedfiles_requiring_higlass_items(
        connection,
        check_name="check_expsets_processedfiles_for_queried_higlass_items",
        action_name="patch_expsets_processedfiles_for_queried_higlass_items",
        search_queries=search_queries
    )

@action_function()
def patch_expsets_processedfiles_for_queried_higlass_items(connection, **kwargs):
    """ Update the Experiment Sets Higlass Items for its Processed Files.

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                called_by(optional, string, default=None): uuid of the associated check. If None, use the primary check

        Returns:
            action object.
    """
    return update_expsets_processedfiles_requiring_higlass_items(
        connection,
        called_by = kwargs.get('called_by', None),
        check_name="check_expsets_processedfiles_for_queried_higlass_items",
        action_name="patch_expsets_processedfiles_for_queried_higlass_items"
    )

def find_expsets_processedfiles_requiring_higlass_items(connection, check_name, action_name, search_queries):
    """ Discover which ExpSets need Higlass Item updates base on their Processed Files or Processed Files in Experiment Sets.

        Args:
            connection: The connection to Fourfront.
            check_name(string): Name of Foursight check.
            action_name(string): Name of related Foursight action.
            search_queries(list, optional, default=[]): A list of search queries. All ExpSets found in at least one of the queries will be modified.

        Returns:
            check result object.
    """
    # Create the check
    check = init_check_res(connection, check_name)
    check.action = action_name
    check.full_output = {}

    # Generate the terms each Experiment Set will return.
    fields_to_include = "&field=" + "&field=".join([
        "accession",
        "award.uuid",
        "contributing_labs.uuid",
        "description",
        "experiments_in_set.processed_files.accession",
        "experiments_in_set.processed_files.genome_assembly",
        "experiments_in_set.processed_files.higlass_uid",
        "experiments_in_set.processed_files.status",
        "lab.uuid",
        "processed_files.accession",
        "processed_files.genome_assembly",
        "processed_files.higlass_uid",
        "processed_files.status",
        "static_content",
    ])

    # If no search query was provided, fail
    if not search_queries:
        check.summary = check.description = "Search queries must be provided."
        check.status = 'FAIL'
        check.allow_action = False
        return check

    expsets_by_accession = {}
    # Use all of the search queries to make a list of the ExpSets we will work on.
    for query in search_queries:
        # Interpolate the timestamps, if needed
        query = interpolate_query_check_timestamps(connection, query, action_name, check)

        # Add to base search
        processed_expsets_query = "/search/?type=ExperimentSetReplicate" + query + fields_to_include

        # Query the Experiment Sets
        search_res = ff_utils.search_metadata(processed_expsets_query, key=connection.ff_keys, ff_env=connection.ff_env)

        # Collate the results into a dict of ExpSets, ordered by accession
        for expset in search_res:
            expsets_by_accession[ expset["accession"] ] = expset

    # Get the reference files
    reference_files_by_ga = get_reference_files(connection)
    check.full_output['reference_files'] = reference_files_by_ga

    # Collate all of the Higlass Items that need to be updated. Store them by genome assembly, then accession.
    target_files_by_ga = {}
    for expset_accession, expset in expsets_by_accession.items():
        # Get all of the processed files. Stop if there is an error.
        file_info = gather_processedfiles_for_expset(expset)

        if file_info["error"]:
            continue

        # If there is a manually created higlass item, don't clobber it with a automatically generated one.
        if file_info["manual_higlass_view_config"]:
            continue

        processed_file_genome_assembly = file_info["genome_assembly"]
        contributing_labs = [ cl["uuid"] for cl in expset.get("contributing_labs", []) ]

        if processed_file_genome_assembly not in target_files_by_ga:
            target_files_by_ga[ processed_file_genome_assembly ] = {}
        target_files_by_ga[ processed_file_genome_assembly ][expset_accession] = {
            "accession" : expset_accession,
            "award" : expset["award"]["uuid"],
            "contributing_labs" : contributing_labs,
            "description": expset["description"],
            "files" : file_info["files"],
            "lab" : expset["lab"]["uuid"],
            "static_content" : expset.get("static_content", []),
        }

    # Check for missing reference files
    for ga in target_files_by_ga:
        if ga in reference_files_by_ga and len(reference_files_by_ga[ga]) >= 2:
            full_output_key = "ready_expsets"
        else:
            full_output_key = "missing_reference_files"
        if full_output_key not in check.full_output:
            check.full_output[full_output_key] = {}
        check.full_output[full_output_key][ga] = target_files_by_ga[ga]

    ready_to_generate_count = 0
    if "ready_expsets" in check.full_output:
        ready_to_generate_count = sum([len(accessions) for x, accessions in check.full_output["ready_expsets"].items()])

    check.summary = ""
    # If there are no files to act upon, we're done.
    if not target_files_by_ga:
        check.summary = check.description = "No new view configs to generate"
        check.status = 'PASS'
        check.allow_action = False
        return check

    check.summary += "Ready to generate Higlass Items for {higlass_count} Experiment Sets. ".format(higlass_count=ready_to_generate_count)
    if "missing_reference_files" in check.full_output:
        check.summary += "Missing reference files for {gas}, skipping. ".format(
            gas=",  ".join(check.full_output["missing_reference_files"].keys())
        )

    check.status = 'WARN'
    check.description = check.summary + "See full_output for details."

    if ready_to_generate_count <= 0:
        check.allow_action = False
    else:
        check.allow_action = True
    return check

def update_expsets_processedfiles_requiring_higlass_items(connection, check_name, action_name, called_by):
    """ Create or update Higlass Items for the Experiment Set's Processed Files.

        Args:
            connection: The connection to Fourfront.
            check_name(string): Name of Foursight check.
            action_name(string): Name of related Foursight action.
            called_by(string, optional, default=None): uuid of the check this action is associated with.
                If None, use the primary result.

        Returns:
            An action object.
    """
    action = init_action_res(connection, action_name)

    # Time to act. Store the results here.
    action_logs = {
        "success": {},
        'failed_to_create_viewconf': {},
        'failed_to_patch_expset': {},
        'missing_reference_files': {},
    }

    # get latest results
    gen_check = init_check_res(connection, check_name)
    if called_by:
        gen_check_result = gen_check.get_result_by_uuid(called_by)
    else:
        gen_check_result = gen_check.get_primary_result()

    reference_files_by_ga = gen_check_result['full_output'].get('reference_files', {})
    target_files_by_ga = gen_check_result['full_output'].get('ready_expsets', {})
    number_expsets_to_update = sum([ len(x) for x in target_files_by_ga.values()])

    # make the fourfront auth key (in basic auth format)
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    # Start timer. We'll abort if the action takes too long to complete.
    start_time = time.time()
    time_expired = False

    # Iterate through the ExpSets.
    for ga in target_files_by_ga:

        # If we're out of time, stop
        if time_expired:
            break

        # Get the reference files for this genome assembly. Skip if they cannot be found.
        if ga not in reference_files_by_ga:
            action_logs["missing_reference_files"][ga]=target_files_by_ga[ga]
            continue
        ref_files = reference_files_by_ga[ga]

        # For all files in this genome assembly
        for expset_accession, file_info in target_files_by_ga[ga].items():
            # Stop if we're out of time
            if time.time() - start_time > 270:
                time_expired = True
                break

            # Get the files and static_content section to modify
            files_for_viewconf = file_info["files"]
            static_content_section = file_info["static_content"]

            # Set the title and description of the static_content
            higlass_title = "{acc} - Processed files".format(
                acc=expset_accession
            )

            higlass_desc = "{acc} ({description}): {files}".format(
                acc=expset_accession,
                description=file_info["description"],
                files=", ".join([ f["accession"] for f in files_for_viewconf ]),
            )

            existing_higlass_uuid = None
            sc_uuids = [ sc["content"] for sc in file_info["static_content"] if sc["location"] == "tab:processed-files"]
            if sc_uuids:
                existing_higlass_uuid = sc_uuids[0]["uuid"]

            # Create or update a HiglassItem based on these files.
            higlass_item_results = create_or_update_higlass_item(
                connection,
                files={
                    "reference":ref_files,
                    "content":files_for_viewconf,
                },
                higlass_item={
                    "uuid": existing_higlass_uuid,
                    "title":higlass_title,
                    "description":higlass_desc,
                },
                ff_requests_auth={
                    "ff_auth": ff_auth,
                    "headers": headers,
                },
                attributions={
                    "lab": file_info["lab"],
                    "award": file_info["award"],
                    "contributing_labs": file_info["contributing_labs"],
                }
            )

            # If we failed to create/update the viewconf, leave an error here
            if higlass_item_results["error"]:
                action_logs['failed_to_create_viewconf'][expset_accession] = higlass_item_results["error"]
                continue

            # Patch the static_content with the new Higlass content
            successful_patch, patch_error =  add_viewconf_static_content_to_file(
                connection,
                expset_accession,
                higlass_item_results["item_uuid"],
                static_content_section,
                "tab:processed-files"
            )

            # If we failed to patch, post the error
            if not successful_patch:
                action_logs['failed_to_patch_expset'][expset_accession] = patch_error
                continue

            # It didn't fail, report success and move on to the next view config
            action_logs["success"][expset_accession] = higlass_item_results["item_uuid"]

    # Note if any files were skipped due to missing reference files.
    action.output = {}
    action.description = ""

    if action_logs["missing_reference_files"]:
        action.description += "Missing reference files for {gas}, skipping. ".format(
            gas=", ".join(action_logs["missing_reference_files"].keys())
        )

    # Action has completed. Report success or failure.
    if len(action_logs["success"]) >= number_expsets_to_update:
        action.output["success"] = action_logs["success"]
        action.description = "All {num} ExpSets are updated.".format(num = number_expsets_to_update)
    else:
        action.description += "{number_failures} Processed Files failed. ".format(number_failures = len(action_logs["failed_to_create_viewconf"]) + len(action_logs["failed_to_patch_expset"]) +
        len(action_logs["missing_reference_files"]))
        action.output.update(action_logs)

    action.output["completed_timestamp"] = datetime.utcnow().isoformat()
    action.status = "DONE"
    return action

@check_function()
def check_expsets_otherprocessedfiles_for_new_higlass_items(connection, **kwargs):
    """ Search for Higlass Items from Experiment Set Other Processed Files (aka Supplementary Files) that need to be updated.

        Args:
            connection: The connection to Fourfront.
            **kwargs

        Returns:
            check result object.
    """
    return find_expsets_otherprocessedfiles_requiring_higlass_itmes(
        connection,
        check_name="check_expsets_otherprocessedfiles_for_new_higlass_items",
        action_name="patch_expsets_otherprocessedfiles_for_new_higlass_items",
        search_queries=[],
        find_opfs_missing_higlass=True
    )

@action_function()
def patch_expsets_otherprocessedfiles_for_new_higlass_items(connection, **kwargs):
    """ Create Higlass Items for Files indicated in check_higlass_items_for_new_files.

    Args:
        connection: The connection to Fourfront.
        **kwargs, which may include:
            called_by(optional, string, default=None): uuid of the associated check. If None, use the primary check

    Returns:
        An action object.
    """

    return update_expsets_otherprocessedfiles_for_higlass_items(
        connection,
        called_by = kwargs.get('called_by', None),
        check_name="check_expsets_otherprocessedfiles_for_new_higlass_items",
        action_name="patch_expsets_otherprocessedfiles_for_new_higlass_items",
    )

@check_function(search_queries=[])
def check_expsets_otherprocessedfiles_for_queried_files(connection, **kwargs):
    """ Search for Higlass Items from Experiment Set Other Processed Files (aka Supplementary Files) that match the given query.

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                search_queries(list, optional, default=[]): A list of search queries. All Files found in at least one of the queries will be modified.

        Returns:
            check result object.
    """
    search_queries = kwargs.get('search_queries', [])

    return find_expsets_otherprocessedfiles_requiring_higlass_itmes(
        connection,
        check_name="check_expsets_otherprocessedfiles_for_queried_files",
        action_name="patch_expsets_otherprocessedfiles_for_queried_files",
        search_queries=search_queries,
        find_opfs_missing_higlass=False
    )

@action_function()
def patch_expsets_otherprocessedfiles_for_queried_files(connection, **kwargs):
    """ Update the Higlass Items from Experiment Set Other Processed Files (aka Supplementary Files).

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                called_by(optional, string, default=None): uuid of the associated check. If None, use the primary check

        Returns:
            action object.
    """
    return update_expsets_otherprocessedfiles_for_higlass_items(
        connection,
        called_by = kwargs.get('called_by', None),
        check_name="check_expsets_otherprocessedfiles_for_queried_files",
        action_name="patch_expsets_otherprocessedfiles_for_queried_files",
    )

def find_expsets_otherprocessedfiles_requiring_higlass_itmes(connection, check_name, action_name, search_queries, find_opfs_missing_higlass):
    """ Check to generate Higlass view configs on Fourfront for Experiment Sets Other Processed Files (aka Supplementary Files.)

        Args:
            check_name(string): Name of Foursight check.
            action_name(string): Name of related Foursight action.
            search_queries(list, optional, default=[]): A list of search queries. All Expsets found in at least one of the queries will be modified.
            find_opfs_missing_higlass(boolean, optional, default=True): If True, search_queries is ignored and the check will find Other Processed File groups with missing Higlass Items.

        Returns:
            check results object.
    """

    # Create the initial check
    check = init_check_res(connection, check_name)
    check.full_output = {
        "search_queries":[]
    }
    check.queries = []
    check.action = action_name

    # If no search query was provided and find_opfs_missing_higlass is False, fail
    if not (search_queries or find_opfs_missing_higlass):
        check.summary = check.description = "If find_opfs_missing_higlass is false, Search queries must be provided."
        check.status = 'FAIL'
        check.allow_action = False
        return check

    if find_opfs_missing_higlass:
        search_queries = [
            "&experiments_in_set.other_processed_files.files.higlass_uid%21=No+value",
            "&other_processed_files.files.higlass_uid%21=No+value"
        ]

    # get the fields you need to include
    fields_to_include = ""
    for new_field in (
        "accession",
        "other_processed_files",
        "experiments_in_set",
        "description",
        "lab.uuid",
        "award.uuid",
        "contributing_labs.uuid",
        "description",
        "last_modified.date_modified"
    ):
        fields_to_include += "&field=" + new_field

    expsets_by_accession = {}

    for query in search_queries:
        query = interpolate_query_check_timestamps(connection, query, action_name, check)

        check.full_output["search_queries"].append(query)

        # Add to the base search
        expset_query = "/search/?type=ExperimentSetReplicate" + query + fields_to_include

        # Store results by accession
        search_res = ff_utils.search_metadata(expset_query, key=connection.ff_keys, ff_env=connection.ff_env)
        for expset in search_res:
            expsets_by_accession[ expset["accession"] ] = expset

    # I'll need more specific file information, so get the files, statuses.
    file_query = '/search/?type=File&higlass_uid%21=No+value&field=status&field=accession&limit=all'
    search_res = ff_utils.search_metadata(file_query, key=connection.ff_keys, ff_env=connection.ff_env)
    file_statuses = { res["accession"] : res["status"] for res in search_res if "accession" in res }

    # Get reference files
    reference_files_by_ga = get_reference_files(connection)
    check.full_output['reference_files'] = reference_files_by_ga

    # Create a helper function that finds files with higlass_uid and the genome assembly
    def find_higlass_files(other_processed_files, filegroups_to_update, statuses_lookup, expset_last_modified_date):
        # If find_opfs_missing_higlass is set, find each Other Processed Filegroup without a higlass_view_config
        if find_opfs_missing_higlass:
            def consider_filegroup(fg):
                # If none of the files have higlass uids, do not make a Higlass Item for this group.
                files_with_higlass_uid = [ f for f in fg["files"] if f.get("higlass_uid", None) ]
                if not files_with_higlass_uid:
                    return False

                # If there is no Higlass Item in this group, we'll make one.
                if not fg.get("higlass_view_config", None):
                    return True

                # If the higlass item was created before the expset was last modified, we need to consider the group.
                if expset_last_modified_date:
                    higlass_modified_date = None

                    existing_higlass_item = fg["higlass_view_config"]
                    if existing_higlass_item.get("last_modified", None) and existing_higlass_item["last_modified"].get("date_modified", None):
                        higlass_modified_date = convert_es_timestamp_to_datetime(existing_higlass_item["last_modified"]["date_modified"])

                    # If the ExpSet was modified but not the higlass item, then the Higlass Item is new and the group should be considered.
                    if not higlass_modified_date:
                        return True

                    # If the Higlass Item is older than the ExpSet, it should be considered.
                    if higlass_modified_date < expset_last_modified_date:
                        return True
                else:
                    # If the ExpSet is new, then consider this group.
                    return True

                # No reason to consider this group.
                return False
            groups_to_consider = [ fg for fg in other_processed_files if consider_filegroup(fg) ]
        else:
            groups_to_consider = other_processed_files

        for filegroup in groups_to_consider:
            genome_assembly = None
            title = filegroup["title"]

            # Find every file with a higlass_uid
            for fil in [ f for f in filegroup["files"] if f.get("higlass_uid", None) ]:
                accession = fil["accession"]

                # Create new entry and copy genome assembly and filegroup type
                if not title in filegroups_to_update:
                    filegroups_to_update[title] = {
                        "genome_assembly": fil["genome_assembly"],
                        "files": [],
                        "type": filegroup["type"],
                    }

                # add file accessions to this group
                filegroups_to_update[title]["files"].append({
                    "accession" : accession,
                    "status" : statuses_lookup[accession],
                })
        return

    all_filegroups_to_update = {}
    expsets_to_update = {}
    higlass_view_count = 0

    # For each expset:
    for accession, expset in expsets_by_accession.items():
        filegroups_to_update = {}
        # Look for other processed file groups with higlass_uid . Update the list by accession and file group title.
        expset_titles = set()
        expset_titles_with_higlass = set()

        # Get the last modified date and convert it to a timestamp.
        expset_last_modified_date = None
        if expset.get("last_modified", None) and expset["last_modified"].get("date_modified", None):
            expset_last_modified_date = convert_es_timestamp_to_datetime(expset["last_modified"]["date_modified"])

        if "other_processed_files" in expset:
            find_higlass_files(expset["other_processed_files"], filegroups_to_update, file_statuses, expset_last_modified_date)

            expset_titles = { fg["title"] for fg in expset["other_processed_files"] }

            expset_titles_with_higlass = [ fg["title"] for fg in expset["other_processed_files"] if fg.get("higlass_view_config", None) ]

        # Scan each Experiment in set to look for other processed file groups with higlass_uid .
        experiments_in_set_to_update = {}
        for experiment in expset.get("experiments_in_set", []):
            if "other_processed_files" in experiment:
                find_higlass_files(experiment["other_processed_files"], experiments_in_set_to_update, file_statuses, expset_last_modified_date)

        for title, info in experiments_in_set_to_update.items():
            # Skip the experiment's file if the higlass view has already been generated.
            if find_opfs_missing_higlass and title in expset_titles_with_higlass:
                continue

            # Create the filegroup based on the experiment if:
            # - It doesn't exist in the ExpSet
            # - It does exist in the ExpSet, but the ExpSet didn't have any files to generate higlass uid with.
            if not (title in expset_titles and title in filegroups_to_update):
                higlass_item_uuid = None
                if "higlass_view_config" in info:
                    higlass_item_uuid = info["higlass_view_config"].get("uuid", None)

                filegroups_to_update[title] = {
                    "genome_assembly": info["genome_assembly"],
                    "files": [],
                    "type": info["type"],
                    "higlass_item_uuid": higlass_item_uuid,
                }

            # Add the files to the existing filegroup
            filegroups_to_update[title]["files"] += info["files"]

        # If at least one filegroup needs to be updated, then record the ExpSet and its other_processed_files section.
        if filegroups_to_update:
            filegroups_info = expset.get("other_processed_files", [])

            contributing_labs = [ c["uuid"] for c in expset.get("contributing_labs", []) ]

            expsets_to_update[accession] = {
                "award" : expset["award"]["uuid"],
                "contributing_labs": contributing_labs,
                "lab" : expset["lab"]["uuid"],
                "description" : expset["description"],
                "other_processed_files" : filegroups_info,
            }

            # Replace file description with just the accessions
            for fg in expsets_to_update[accession]["other_processed_files"]:
                accessions = [ f["accession"] for f in fg["files"] ]
                fg["files"] = accessions

            all_filegroups_to_update[accession] = filegroups_to_update
            higlass_view_count += len(filegroups_to_update.keys())

    # check announces success
    check.full_output['filegroups_to_update'] = all_filegroups_to_update
    check.full_output['expsets_to_update'] = expsets_to_update

    if not all_filegroups_to_update:
        # nothing new to generate
        check.summary = check.description = "No new view configs to generate"
        check.status = 'PASS'
    else:
        check.summary = "Ready to generate {file_count} Higlass view configs for {exp_sets} Experiment Set".format(file_count=higlass_view_count, exp_sets=len(expsets_to_update))
        check.description = check.summary + ". See full_output for details."
        check.status = 'WARN'
        check.allow_action = True
    return check

def update_expsets_otherprocessedfiles_for_higlass_items(connection, check_name, action_name, called_by):
    """ Create, Post and Patch HiGlass Items for the given Experiment Sets and their Other Processed Files (aka Supplementary files) entries

        Args:
            connection: The connection to Fourfront.
            check_name(string): Name of Foursight check.
            action_name(string): Name of related Foursight action.
            called_by(string, optional, default=None): uuid of the check this action is associated with.
                If None, use the primary result.

        Returns:
            An action object.
    """
    action = init_action_res(connection, action_name)

    action_logs = {
        'successes': {},
        'failed_to_create_viewconf': {},
        'failed_to_patch_expset': {}
    }

    # get latest results
    gen_check = init_check_res(connection, check_name)
    if called_by:
        gen_check_result = gen_check.get_result_by_uuid(called_by)
    else:
        gen_check_result = gen_check.get_primary_result()

    # make the fourfront auth key (in basic auth format)
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # Get the reference files
    ref_files_by_ga = gen_check_result['full_output'].get('reference_files', {})

    expsets_to_update = gen_check_result['full_output']["expsets_to_update"]
    filegroups_to_update = gen_check_result['full_output']["filegroups_to_update"]

    viewconfs_updated_goal = 0
    number_of_viewconfs_updated = 0

    # For each expset we want to update
    for accession in expsets_to_update:
        # If we've taken more than 270 seconds to complete, break immediately
        if time_expired:
            break

        lab = expsets_to_update[accession]["lab"]
        contributing_labs = expsets_to_update[accession]["contributing_labs"]
        award = expsets_to_update[accession]["award"]
        expset_description = expsets_to_update[accession]["description"]

        # Look in the filegroups we need to update for that ExpSet
        new_viewconfs = {}
        viewconfs_updated_goal += len(filegroups_to_update[accession].keys())
        number_of_posted_viewconfs = 0
        for title, info in filegroups_to_update[accession].items():
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            # Get the reference files for the genome assembly
            reference_files = ref_files_by_ga[ info["genome_assembly"] ]

            # Create the Higlass Viewconf and get the uuid
            data_files = info["files"]

            #- title: <expset accession> - <title of opf)
            higlass_title = "{acc} - {title}".format(acc=accession, title=title)

            #- description: Supplementary files (<description of opf> ) for <accession> (<description of the experiment>): <file accessions involved>
            higlass_desc = "Supplementary Files ({opf_desc}) for {acc} ({exp_desc}): {files}".format(
                opf_desc = title,
                acc = accession,
                exp_desc = expset_description,
                files=", ".join([ f["accession"] for f in data_files ])
            )

            # Create or update a HiglassItem based on these files.
            higlass_item_results = create_or_update_higlass_item(
                connection,
                files={
                    "reference": reference_files,
                    "content": data_files,
                },
                higlass_item={
                    "uuid": info.get("higlass_item_uuid", None),
                    "title": higlass_title,
                    "description": higlass_desc,
                },
                ff_requests_auth={
                    "ff_auth": ff_auth,
                    "headers": headers,
                },
                attributions={
                    "lab": lab,
                    "award": award,
                    "contributing_labs": contributing_labs,
                }
            )

            if higlass_item_results["error"]:
                if accession not in action_logs['failed_to_create_viewconf']:
                    action_logs['failed_to_create_viewconf'][accession] = {}
                if title not in action_logs['failed_to_create_viewconf'][accession]:
                    action_logs['failed_to_create_viewconf'][accession][title] = {}

                action_logs['failed_to_create_viewconf'][accession][title] = post_viewconf_results["error"]
                continue

            # If the filegroup title is not in the ExpSet other_processed_files section, make it now
            matching_title_filegroups = [ fg for fg in expsets_to_update[accession]["other_processed_files"] if fg.get("title", None) == title ]
            if not matching_title_filegroups:
                newfilegroup = deepcopy(info)
                del newfilegroup["genome_assembly"]
                newfilegroup["files"] = []
                newfilegroup["title"] = title
                expsets_to_update[accession]["other_processed_files"].append(newfilegroup)
                matching_title_filegroups = [ newfilegroup, ]

            # Add the higlass_view_config to the filegroup
            matching_title_filegroups[0]["higlass_view_config"] = higlass_item_results["item_uuid"]
            matching_title_filegroups[0]["higlass_view_config"]

            new_viewconfs[title] = higlass_item_results["item_uuid"]
            number_of_posted_viewconfs += 1

        # The other_processed_files section has been updated. Patch the changes.
        try:
            # Make sure all higlass_view_config fields just show the uuid.
            for g in [ group for group in expsets_to_update[accession]["other_processed_files"] if "higlass_view_config" in group ]:
                if isinstance(g["higlass_view_config"], dict):
                    uuid = g["higlass_view_config"]["uuid"]
                    g["higlass_view_config"] = uuid

            ff_utils.patch_metadata(
                {'other_processed_files': expsets_to_update[accession]["other_processed_files"]},
                obj_id=accession,
                key=connection.ff_keys,
                ff_env=connection.ff_env
            )
            number_of_viewconfs_updated += number_of_posted_viewconfs
        except Exception as e:
            if accession not in action_logs['failed_to_patch_expset']:
                action_logs['failed_to_patch_expset'][accession] = {}
            if title not in action_logs['failed_to_patch_expset'][accession]:
                action_logs['failed_to_patch_expset'][accession][title] = {}
            action_logs['failed_to_patch_expset'][accession][title] = str(e)
            continue

        # Success. Note which titles link to which HiGlass view configs.
        if accession not in action_logs['successes']:
            action_logs['successes'][accession] = {}
        action_logs['successes'][accession] = new_viewconfs

    # Report on successes.
    if len(action_logs['successes'].keys()) >= len(expsets_to_update.keys()):
        accession_report = "All"
    else:
        accession_report = "Only"
    accession_report += " {actual} of {goal} ExpSets ({actual_opfs} of {goal_opfs} filegroups) updated".format(
        actual=len(action_logs['successes'].keys()),
        goal=len(expsets_to_update.keys()),
        actual_opfs=number_of_viewconfs_updated,
        goal_opfs=viewconfs_updated_goal,
    )

    action.description = accession_report
    action.status = 'DONE'
    action.output = action_logs
    return action

@check_function(confirm_on_higlass=False, filetype='all', higlass_server=None)
def files_not_registered_with_higlass(connection, **kwargs):
    """
    Used to check registration of files on higlass and also register them
    through the patch_file_higlass_uid action.

    If confirm_on_higlass is True, check each file by making a request to the
    higlass server. Otherwise, just look to see if a higlass_uid is present in
    the metadata.

    The filetype arg allows you to specify which filetypes to operate on.
    Must be one of: 'all', 'bigbed', 'mcool', 'bg', 'bw', 'beddb', 'chromsizes'.
    'chromsizes' and 'beddb' are from the raw files bucket; all other filetypes
    are from the processed files bucket.

    higlass_server may be passed in if you want to use a server other than
    higlass.4dnucleome.org.

    Since 'chromsizes' file defines the coordSystem (assembly) used to register
    other files in higlass, these go first.

    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        A check/action object.
    """
    check = init_check_res(connection, 'files_not_registered_with_higlass')
    check.status = "FAIL"
    check.description = "not able to get data from fourfront"
    # keep track of mcool, bg, and bw files separately
    valid_filetypes = {
        "raw": ['chromsizes', 'beddb'],
        "proc": ['mcool', 'bg', 'bw', 'bed', 'bigbed'],
    }

    all_valid_types = valid_filetypes["raw"] + valid_filetypes["proc"]

    files_to_be_reg = {}
    not_found_upload_key = []
    not_found_s3 = []
    no_genome_assembly = []

    # Make sure the filetype is valid.
    search_all_filetypes = kwargs['filetype'] == 'all'
    if not search_all_filetypes and kwargs['filetype'] not in all_valid_types:
        check.description = check.summary = "Filetype must be one of: %s" % (all_valid_types + ['all'])
        return check

    reg_filetypes = all_valid_types if kwargs['filetype'] == 'all' else [kwargs['filetype']]
    check.action = "patch_file_higlass_uid"

    # can overwrite higlass server, if desired. The default higlass key is always used
    higlass_key = connection.ff_s3.get_higlass_key()
    higlass_server = kwargs['higlass_server'] if kwargs['higlass_server'] else higlass_key['server']

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # Get the query for all file types
    search_queries_by_type = {
        "raw": None,
        "proc": None,
    }

    for file_cat, filetypes in valid_filetypes.items():
        # If the user specified a filetype, only use that one.
        filetypes_to_use = [f for f in filetypes if search_all_filetypes or f == kwargs['filetype']]

        if not filetypes_to_use:
            continue

        # Build a file query string.
        if file_cat == "raw":
            type_filter = '&type=FileReference'
        else:
            type_filter = '&type=FileProcessed' + '&type=FileVistrack'

        # Build a file format filter
        file_format_filter = "?file_format.file_format=" + "&file_format.file_format=".join(filetypes_to_use)

        # Build the query that finds all published files.
        search_query = 'search/' + file_format_filter + type_filter

        # Make sure it's published
        unpublished_statuses = (
            "uploading",
            "to be uploaded by workflow",
            "upload failed",
            "deleted",
        )
        search_query += "&status!=" + "&status!=".join([u.replace(" ","+") for u in unpublished_statuses])

        # exclude read positions because those files are too large for Higlass to render
        search_query += "&file_type!=read+positions"

        # Only request the necessary fields
        for new_field in (
            "accession",
            "genome_assembly",
            "file_format",
            "higlass_uid",
            "uuid",
            "extra_files",
            "upload_key",
        ):
            search_query += "&field=" + new_field

        # Add the query
        search_queries_by_type[file_cat] = search_query

    for file_cat, search_query in search_queries_by_type.items():

        # Skip if there is no search query (most likely it was filtered out)
        if not search_query:
            continue

        # Query all possible files
        possibly_reg = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)

        for procfile in possibly_reg:
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            # Note any file without a genome assembly.
            if 'genome_assembly' not in procfile:
                no_genome_assembly.append(procfile['accession'])
                continue

            # Gather needed information from each file
            file_info = {
                'accession': procfile['accession'],
                'uuid': procfile['uuid'],
                'file_format': procfile['file_format'].get('file_format'),
                'higlass_uid': procfile.get('higlass_uid'),
                'genome_assembly': procfile['genome_assembly']
            }
            file_format = file_info["file_format"]

            if file_format not in files_to_be_reg:
                files_to_be_reg[file_format] = []

            # bg files use an bw file from extra files to register
            # bed files use a beddb file from extra files to regiser
            # don't FAIL if the bg is missing the bw, however
            type2extra = {'bg': 'bw', 'bed': 'beddb'}
            if file_format in type2extra:
                # Get the first extra file of the needed type that has an upload_key and has been published.
                for extra in procfile.get('extra_files', []):
                    if extra['file_format'].get('display_title') == type2extra[file_format] \
                        and 'upload_key' in extra \
                        and extra.get("status", unpublished_statuses[-1]) not in unpublished_statuses:
                        file_info['upload_key'] = extra['upload_key']
                        break
                if 'upload_key' not in file_info:  # bw or beddb file not found
                    continue
            else:
                # mcool and bw files use themselves
                if 'upload_key' in procfile:
                    file_info['upload_key'] = procfile['upload_key']
                else:
                    not_found_upload_key.append(file_info['accession'])
                    continue
            # make sure file exists on s3
            typebucket_by_cat = {
                "raw" : connection.ff_s3.raw_file_bucket,
                "proc" : connection.ff_s3.outfile_bucket,
            }
            if not connection.ff_s3.does_key_exist(file_info['upload_key'], bucket=typebucket_by_cat[file_cat]):
                not_found_s3.append(file_info)
                continue

            # check for higlass_uid and, if confirm_on_higlass is True, check the higlass server
            if file_info.get('higlass_uid'):
                if kwargs['confirm_on_higlass'] is True:
                    higlass_get = higlass_server + '/api/v1/tileset_info/?d=%s' % file_info['higlass_uid']
                    hg_res = requests.get(higlass_get)
                    # Make sure the response completed successfully and did not return an error.
                    if hg_res.status_code >= 400:
                        files_to_be_reg[file_format].append(file_info)
                    elif 'error' in hg_res.json().get(file_info['higlass_uid'], {}):
                        files_to_be_reg[file_format].append(file_info)
            else:
                files_to_be_reg[file_format].append(file_info)

    check.full_output = {'files_not_registered': files_to_be_reg,
                         'files_without_upload_key': not_found_upload_key,
                         'files_not_found_on_s3': not_found_s3,
                         'files_missing_genome_assembly': no_genome_assembly}
    if no_genome_assembly or not_found_upload_key or not_found_s3:
        check.status = "FAIL"
        check.summary = check.description = "Some files cannot be registed. See full_output."
    else:
        check.status = 'PASS'

    file_count = sum([len(files_to_be_reg[ft]) for ft in files_to_be_reg])
    if file_count != 0:
        check.status = 'WARN'
    if check.summary:
        if file_count != 0:
            check.summary += ' %s files ready for registration' % file_count
            check.description += ' %s files ready for registration.' % file_count
        elif check.status == 'PASS':
            check.summary += ' All files are registered.'
            check.description += ' All files are registered.'
        else:
            check.summary += ' No files to register.'
            check.description += ' No files to register.'

        if not kwargs['confirm_on_higlass']:
            check.description += "Run with confirm_on_higlass=True to check against the higlass server"
    else:
        check.summary = ' %s files ready for registration' % file_count
        check.description = check.summary
        if not kwargs['confirm_on_higlass']:
            check.description += "Run with confirm_on_higlass=True to check against the higlass server"


    check.action_message = "Will attempt to patch higlass_uid for %s files." % file_count
    check.allow_action = True
    return check

@action_function(file_accession=None)
def patch_file_higlass_uid(connection, **kwargs):
    """ After running "files_not_registered_with_higlass",
    Try to register files with higlass.

    Args:
        connection: The connection to Fourfront.
        **kwargs, which may include:
            file_accession: Only check this file.

    Returns:
        A check/action object.
    """
    action = init_action_res(connection, 'patch_file_higlass_uid')
    action_logs = {
        'patch_failure': {},
        'patch_success': [],
        'registration_failure': {},
        'registration_success': 0
    }
    # get latest results
    higlass_check = init_check_res(connection, 'files_not_registered_with_higlass')
    if kwargs.get('called_by', None):
        higlass_check_result = higlass_check.get_result_by_uuid(kwargs['called_by'])
    else:
        higlass_check_result = higlass_check.get_primary_result()

    # get the desired server
    higlass_key = connection.ff_s3.get_higlass_key()
    if higlass_check_result['kwargs'].get('higlass_server'):
        higlass_server = higlass_check_result['kwargs']['higlass_server']
    else:
        higlass_server = higlass_key['server']

    # Prepare authentication header
    authentication = (higlass_key['key'], higlass_key['secret'])
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # Files to register is organized by filetype.
    to_be_registered = higlass_check_result.get('full_output', {}).get('files_not_registered')
    for ftype, hits in to_be_registered.items():
        if time_expired:
            break
        for hit in hits:
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            # If a file accession was specified, skip all others
            if kwargs['file_accession'] and hit['accession'] != kwargs['file_accession']:
                continue

            # Based on the filetype, construct a payload to upload to the higlass server.
            payload = {'coordSystem': hit['genome_assembly']}
            if ftype == 'chromsizes':
                payload["filepath"] = connection.ff_s3.raw_file_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'chromsizes-tsv'
                payload['datatype'] = 'chromsizes'
            elif ftype == 'beddb':
                payload["filepath"] = connection.ff_s3.raw_file_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'beddb'
                payload['datatype'] = 'gene-annotation'
            elif ftype == 'mcool':
                payload["filepath"] = connection.ff_s3.outfile_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'cooler'
                payload['datatype'] = 'matrix'
            elif ftype in ['bg', 'bw', 'bigbed']:
                # bigbeds can be registered the same way as bigwigs
                payload["filepath"] = connection.ff_s3.outfile_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'bigwig'
                payload['datatype'] = 'vector'
            elif ftype == 'bed':
                payload["filepath"] = connection.ff_s3.outfile_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'beddb'
                payload['datatype'] = 'bedlike'
            else:
                err_msg = 'No filetype case specified for %s' % ftype
                action_logs['registration_failure'][hit['accession']] = err_msg
                continue
            # register with previous higlass_uid if already there
            if hit.get('higlass_uid'):
                payload['uuid'] = hit['higlass_uid']

            res = requests.post(
                higlass_server + '/api/v1/link_tile/',
                data=json.dumps(payload),
                auth=authentication,
                headers=headers
            )

            # update the metadata file as well, if uid wasn't already present or changed
            if res.status_code == 201:
                action_logs['registration_success'] += 1
                # Get higlass's uuid. This is Fourfront's higlass_uid.
                response_higlass_uid = res.json()['uuid']
                if 'higlass_uid' not in hit or hit['higlass_uid'] != response_higlass_uid:
                    patch_data = {'higlass_uid': response_higlass_uid}
                    try:
                        ff_utils.patch_metadata(patch_data, obj_id=hit['uuid'], key=connection.ff_keys, ff_env=connection.ff_env)
                    except Exception as e:
                        action_logs['patch_failure'][hit['accession']] = "{type}: {message}".format(
                            type = type(e),
                            message = str(e)
                        )
                    else:
                        action_logs['patch_success'].append(hit['accession'])
            else:
                # Add reason for failure. res.json not available on 500 resp
                try:
                    err_msg = res.json().get("error", res.status_code)
                except Exception:
                    err_msg = res.status_code
                action_logs['registration_failure'][hit['accession']] = err_msg
    action.status = 'DONE'
    action.output = action_logs
    return action

@check_function()
def find_cypress_test_items_to_purge(connection, **kwargs):
    """ Looks for all items that are deleted and marked for purging by cypress test.
    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        A check/action object
    """

    check = init_check_res(connection, 'find_cypress_test_items_to_purge')
    check.full_output = {
        'items_to_purge':[]
    }

    # associate the action with the check.
    check.action = 'purge_cypress_items'

    # Search for all Higlass View Config that are deleted and have the deleted_by_cypress_test tag.
    search_query = '/search/?type=Item&status=deleted&tags=deleted_by_cypress_test'
    search_response = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)

    check.full_output['items_to_purge'] = [ s["uuid"] for s in search_response ]

    # Note the number of items ready to purge
    num_viewconfigs = len(check.full_output['items_to_purge'])
    check.status = 'WARN'

    if num_viewconfigs == 0:
        check.summary = check.description = "No new items to purge."
        check.status = 'PASS'
    else:
        check.summary = "Ready to purge %s items" % num_viewconfigs
        check.description = check.summary + ". See full_output for details."
        check.allow_action = True
    return check

@action_function()
def purge_cypress_items(connection, **kwargs):
    """ Using the find_cypress_test_items_to_purge check, deletes the indicated items.
    Args:
        connection: The connection to Fourfront.
        **kwargs

    Returns:
        A check object
    """

    action = init_action_res(connection, 'purge_cypress_items')
    action_logs = {
        'items_purged':[],
        'failed_to_purge':{}
    }

    # get latest results
    gen_check = init_check_res(connection, 'find_cypress_test_items_to_purge')
    if kwargs.get('called_by', None):
        gen_check_result = gen_check.get_result_by_uuid(kwargs['called_by'])
    else:
        gen_check_result = gen_check.get_primary_result()

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # Purge the deleted files.
    for view_conf_uuid in gen_check_result["full_output"]["items_to_purge"]:
        # If we've taken more than 270 seconds to complete, break immediately
        if time.time() - start_time > 270:
            time_expired = True
            break

        purge_response = ff_utils.purge_metadata(view_conf_uuid, key=connection.ff_keys, ff_env=connection.ff_env)
        if purge_response['status'] == 'success':
            action_logs['items_purged'].append(view_conf_uuid)
        else:
            action_logs['failed_to_purge'][view_conf_uuid] = purge_response["comment"]

    action.status = 'DONE'
    action.output = action_logs
    return action

def create_or_update_higlass_item(connection, files, attributions, higlass_item, ff_requests_auth):
    """
    Create a new Higlass viewconfig and update the containing Higlass Item.

    Args:
        connection          : The connection to Fourfront.
        files(dict)         : Info on the files used to create the viewconfig and Item. Also sets Item status.
            reference(list)     : A list of Reference files accessions
            content(list)       : A list of file dicts.
        attributions(dict)  : Higlass Item permission settings using uuids.
            lab(string)
            contributing_labs(list) : A list of contributing lab uuids.
            award(string)
        higlass_item (dict) : Determine whether to create or update the Item and how to present it.
            uuid(string or None)    : Update the Higlass Item with this uuid (or create a new one if None)
            title(string)
            description(string)
        ff_requests_auth(dict)      : Needed information to connect to Fourfront.
            ff_auth(dict)           : Authorization needed to post to Fourfront.
            headers(dict)           : Header information needed to post to Fourfront.

    Returns:
        A dictionary:
            item_uuid(string): The uuid of the new Higlass Item. or None if there was an error.
            error(string): None if the call was successful.
    """
    # start with the reference files and add the target files
    file_accessions = [ f["accession"] for f in files["content"] ]
    to_post = {'files': files["reference"] + file_accessions}

    # post the files to the visualization endpoint
    res = requests.post(
        connection.ff_server + 'add_files_to_higlass_viewconf/',
        data=json.dumps(to_post),
        auth=ff_requests_auth["ff_auth"],
        headers=ff_requests_auth["headers"]
    )

    # Handle the response.
    if res and res.json().get('success', False):
        new_view_config = res.json()['new_viewconfig']

        # Get the new status.
        viewconf_status = get_viewconf_status(files["content"])

        # Set up the fields for the new Higlass Item based on the new viewconf, attributions and description.
        viewconf_description = {
            "genome_assembly": res.json()['new_genome_assembly'],
            "status": viewconf_status,
            "viewconfig": res.json()['new_viewconfig'],
        }

        viewconf_description.update(attributions)
        viewconf_description["description"] = higlass_item.get("description", "")
        viewconf_description["title"] = higlass_item["title"]

        try:
            # If a uuid was given, patch the existing Higlass Item.
            if higlass_item["uuid"]:
                viewconf_res = ff_utils.patch_metadata(
                    viewconf_description,
                    obj_id=higlass_item["uuid"],
                    key=connection.ff_keys,
                    ff_env=connection.ff_env
                )
                return {
                    "item_uuid": higlass_item["uuid"],
                    "error": ""
                }
            else:
                # Post a new Higlass Item.
                viewconf_res = ff_utils.post_metadata(
                    viewconf_description,
                    'higlass-view-configs',
                    key=connection.ff_keys,
                    ff_env=connection.ff_env
                )
                view_conf_uuid = viewconf_res['@graph'][0]['uuid']
                return {
                    "item_uuid": view_conf_uuid,
                    "error": ""
                }
        except Exception as e:
            # Something happened while Patching or Posting the Higlass Item. Note the error.
            return {
                "item_uuid": None,
                "error": str(e)
            }
    else:
        # Fourfront returned a bad status.
        if res:
            return {
                "item_uuid": None,
                "error": res.json()["errors"]
            }

        # We couldn't connect to Fourfront.
        return {
            "item_uuid": None,
            "error": "Could not contact visualization endpoint."
        }

def interpolate_query_check_timestamps(connection, search_query, action_name, result_check):
    """ Search for Foursight check timestamps in the search query
    and replace them with the actual timestamp.

    Args:
        connection              : The connection to Fourfront.
        search_query(string)    : This query may have a substitute key phrase.
        action_name(string)     : Name of the related action.
        result_check(RunResult) : This object can look for the history of other checks.

    Returns:
        The new search_query.
    """

    if "<get_latest_action_completed_date>" in search_query:
        # Get the related action for this check
        action = init_action_res(connection, action_name)
        action_result = action.get_latest_result()

        if not action_result:
            return search_query

        if "completed_timestamp" not in action_result["output"]:
            return search_query

        # Timestamp example:
        # Cut off the timezone and seconds offset.
        completed_timestamp_raw = action_result["output"]["completed_timestamp"]

        index = completed_timestamp_raw.rfind(".")
        if index != -1:
            completed_timestamp_formatted = completed_timestamp_raw[0:index]
        else:
            completed_timestamp_formatted = completed_timestamp_raw

        completed_timestamp_datetime = datetime.strptime(
            completed_timestamp_formatted,
            "%Y-%m-%dT%H:%M:%S"
        )
        # Move time stamp to 1 minute in the future.
        completed_timestamp_datetime += timedelta(minutes=1)

        # Convert to elastic search format, yyyy-mm-dd HH:MM
        es_string = datetime.strftime(completed_timestamp_datetime, "%Y-%m-%d %H:%M")

        # Get the timestamp the action completed.
        completed_timestamp = es_string

        # Replace the key with the timestamp.
        search_query = search_query.replace("<get_latest_action_completed_date>", completed_timestamp)
    return search_query

def convert_es_timestamp_to_datetime(raw):
    """ Convert the ElasticSearch timestamp to a Python Datetime.

    Args:
        raw(string): The ElasticSearch timestamp, as a string.

    Returns:
        A datetime object (or None)
    """
    converted_date = None

    if not raw:
        return converted_date

    index = raw.rfind(".")
    if index != -1:
        formatted_date = raw[0:index]
    else:
        formatted_date = raw

    converted_date = datetime.strptime(
        formatted_date,
        "%Y-%m-%dT%H:%M:%S"
    )
    return converted_date
