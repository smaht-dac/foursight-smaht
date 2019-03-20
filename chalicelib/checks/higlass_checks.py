from __future__ import print_function, unicode_literals
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

def post_viewconf_to_visualization_endpoint(connection, reference_files, files, ff_auth, headers, title="", description=""):
    """
    Given the list of files, contact fourfront and generate a higlass view config.
    Then post the view config.
    Returns the viewconf uuid upon success, or None otherwise.

    Args:
        connection              : The connection to Fourfront.
        reference_files(dict)   : Reference files, stored by genome assembly (see get_reference_files)
        files(list)             : A list of file identifiers.
        ff_auth(dict)           : Authorization needed to post to Fourfront.
        headers(dict)           : Header information needed to post to Fourfront.
        title(string, optional, default = "")       : Higlass view config title.
        description(string, optional, default = "") : Higlass view config description.

    Returns:
        A dictionary:
            view_config_uuid: string referring to the new Higlass view conf uuid if it succeeded. None otherwise.
            error: string describing the error (blank if there is no error.)
    """
    # start with the reference files and add the target files
    to_post = {'files': reference_files + files}

    view_conf_uuid = None
    # post to the visualization endpoint
    ff_endpoint = connection.ff_server + 'add_files_to_higlass_viewconf/'
    res = requests.post(ff_endpoint, data=json.dumps(to_post),
                        auth=ff_auth, headers=headers)

    # Handle the response.
    if res and res.json().get('success', False):
        view_conf = res.json()['new_viewconfig']

        # Post the new view config.
        viewconf_description = {
            "genome_assembly": res.json()['new_genome_assembly'],
            "viewconfig": view_conf
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

def add_viewconf_static_content_to_file(connection, item_uuid, view_conf_uuid, static_content_section, sc_location):
    """
    Add some static content for the item that shows the view config created for it.
    Returns True upon success.

    Args:
        connection          : The connection to Fourfront.
        item_uuid(str)      : Identifier for the item.
        view_conf_uuid(str) : Identifier for the Higlass view config.
        static_content_section(list) : The current static content section for this item.
        sc_location(str)    : Name for the new Static Content's location field.

    Returns:
        boolean. True indicates success.
        string. Contains the error (or an empty string if there is no error.)
    """
    new_view_conf_sc = {
        'location': sc_location,
        'content': view_conf_uuid,
        'description': 'auto_generated_higlass_view_config'
    }
    new_static_content = static_content_section + [new_view_conf_sc]
    try:
        ff_utils.patch_metadata(
            {'static_content': new_static_content},
            obj_id=item_uuid,
            key=connection.ff_keys,
            ff_env=connection.ff_env
        )
    except Exception as e:
        return False, str(e)
    return True, ""

@check_function(file_accession=None)
def check_files_for_higlass_viewconf(connection, **kwargs):
    """
    Check to generate Higlass view configs on Fourfront for appropriate files

    Args:
        connection: The connection to Fourfront.
        **kwargs, which may include:
            file_accession (optional, default=None): Only generate a viewconf for the given file acccession.

    Returns:
        check results object.
    """
    check = init_check_res(connection, 'check_files_for_higlass_viewconf')
    check.full_output = {
        "reference_files" : [],
        "target_files" : {},
    }
    check.action = 'patch_files_for_higlass_viewconf'

    # first, find and cache the reference files
    reference_files_by_ga = get_reference_files(connection)
    check.full_output['reference_files'] = reference_files_by_ga

    # if we don't have two reference files for each genome_assembly, fail
    if any([len(reference_files_by_ga[ga]) != 2 for ga in reference_files_by_ga]):
        check.status = 'FAIL'
        check.summary = check.description = "Could not find two reference files for each genome_assembly. See full_output"
        return check

    target_files_by_ga = {}

    # If the file is specified, use that instead.
    if kwargs['file_accession']:
        accession = kwargs['file_accession']

        file_resource = ff_utils.get_metadata(accession, key=connection.ff_keys, ff_env=connection.ff_env, add_on="frame=embedded")

        genome_assembly = file_resource["genome_assembly"]
        static_content = file_resource.get("static_content", [])

        target_files_by_ga[genome_assembly] = {}
        target_files_by_ga[genome_assembly][accession] = static_content
    else:
        # next, find the files we are interested in (exclude reference files and any with existing Higlass viewconfs.)
        search_query = '/search/?type=File&higlass_uid!=No+value&genome_assembly!=No+value&tags!=higlass_reference&static_content.description!=auto_generated_higlass_view_config&field=accession&field=genome_assembly&field=static_content'
        search_res = ff_utils.search_metadata(search_query, key=connection.ff_keys, ff_env=connection.ff_env)
        for hg_file in search_res:
            # Otherwise add the file to the todo list.
            accession = hg_file["accession"]
            genome_assembly = hg_file["genome_assembly"]
            static_content = hg_file.get("static_content", [])

            if genome_assembly not in target_files_by_ga:
                target_files_by_ga[genome_assembly] = {}

            target_files_by_ga[genome_assembly][accession] = static_content

    check.full_output['target_files'] = target_files_by_ga
    check.status = 'PASS'

    if not target_files_by_ga:
        # nothing new to generate
        check.summary = check.description = "No new view configs to generate"
    else:
        all_files = sum([len(target_files_by_ga[ga]) for ga in target_files_by_ga])
        check.summary = "Ready to generate %s Higlass view configs" % all_files
        check.description = check.summary + ". See full_output for details."
        check.allow_action = True
    return check

@action_function(file_accession=None)
def patch_files_for_higlass_viewconf(connection, **kwargs):
    """ Action that is used with generate_higlass_view_confs_files to actually
    POST new higlass view configs and PATCH the old files.

    Args:
        connection: The connection to Fourfront.
        **kwargs, which may include:
            file_accession(string, optional, default=None): Only generate a viewconf for the given file acccession.

    Returns:
        A check/action object.
    """
    action = init_action_res(connection, 'patch_files_for_higlass_viewconf')
    action_logs = {
        'new_view_confs_by_file': {},
        'failed_to_create_viewconf' : {},
        'failed_to_patch_file' : {},
    }

    # get latest results
    gen_check = init_check_res(connection, 'check_files_for_higlass_viewconf')
    if kwargs.get('called_by', None):
        gen_check_result = gen_check.get_result_by_uuid(kwargs['called_by'])
    else:
        gen_check_result = gen_check.get_primary_result()

    # make the fourfront auth key (in basic auth format)
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json'}

    # pointer to the reference files (by genome_assembly)
    ref_files_by_ga = gen_check_result['full_output'].get('reference_files', {})

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # these are the files we care about
    # loop by genome_assembly
    target_files_by_ga = gen_check_result['full_output'].get('target_files', {})
    for ga in target_files_by_ga:
        if time_expired:
            break

        if ga not in ref_files_by_ga:  # reference files not found
            continue

        ref_files = ref_files_by_ga[ga]

        for file_accession, file_info in target_files_by_ga[ga].items():
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            # If a particular accession was chosen, skip the others
            if kwargs['file_accession'] and file_accession != kwargs['file_accession']:
                continue

            static_content_section = target_files_by_ga[ga][ file_accession ]

            # Post a new Higlass viewconf using the file list
            higlass_title = "{acc} - Higlass Viewconfig".format(acc=file_accession)

            post_viewconf_results = post_viewconf_to_visualization_endpoint(connection, ref_files, [file_accession], ff_auth, headers, higlass_title)

            if post_viewconf_results["error"]:
                action_logs['failed_to_create_viewconf'][file_accession] = post_viewconf_results["error"]
                continue

            # Create a new static content section with the description = "auto_generated_higlass_view_config" and the new viewconf as the content
            # Patch the ExpSet static content
            successful_patch, patch_error =  add_viewconf_static_content_to_file(connection, file_accession, post_viewconf_results["view_config_uuid"],
            static_content_section,
            "tab:higlass")

            if not successful_patch:
                action_logs['failed_to_patch_file'][file_accession] = patch_error
                continue

            action_logs['new_view_confs_by_file'][file_accession] = post_viewconf_results["view_config_uuid"]

    action.status = 'DONE'
    action.output = action_logs

    target_files_by_ga = gen_check_result['full_output'].get('target_files', {})
    file_count = sum([len(target_files_by_ga[ga]) for ga in target_files_by_ga])
    action.progress = "Created Higlass viewconfs for {completed} out of {possible} files".format(
        completed=len(action_logs["new_view_confs_by_file"].keys()),
        possible=file_count
    )
    return action

@check_function(expset_accession=None)
def check_expsets_processedfiles_for_higlass_viewconf(connection, **kwargs):
    """ Check to generate Higlass view configs on Fourfront for Experiment Sets Processed Files (and Processed Files in Experiment Sets.)

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                expset_accession: Only check this expset.

        Returns:
            check results object.
    """

    check = init_check_res(connection, 'check_expsets_processedfiles_for_higlass_viewconf')
    check.full_output = {}
    check.action = 'patch_expsets_processedfiles_for_higlass_viewconf'

    if kwargs['expset_accession']:
        expsets_by_accession = {
            kwargs['expset_accession'] : ff_utils.get_metadata(kwargs['expset_accession'], key=connection.ff_keys, ff_env=connection.ff_env, add_on="frame=embedded")
        }
    else:
        fields_to_include = ""
        for new_field in (
            "accession",
            "static_content",
            "processed_files.accession",
            "processed_files.genome_assembly",
            "processed_files.higlass_uid",
            "experiments_in_set.processed_files.accession",
            "experiments_in_set.processed_files.genome_assembly",
            "experiments_in_set.processed_files.higlass_uid",
            "description",
        ):
            fields_to_include += "&field=" + new_field

        # Include ExpSets whose Processed Files have higlass_uid
        processed_expsets_query = '/search/?type=ExperimentSetReplicate&processed_files.higlass_uid%21=No+value' + fields_to_include
        search_res = ff_utils.search_metadata(processed_expsets_query, key=connection.ff_keys, ff_env=connection.ff_env)

        # store results by accession
        expsets_by_accession = {expset["accession"]: expset for expset in search_res }

        # Include ExpSets whose Experiments contain Processed Files with higlass_uid
        processed_experiments_query = '/search/?type=ExperimentSetReplicate&experiments_in_set.processed_files.higlass_uid%21=No+value' + fields_to_include
        search_res = ff_utils.search_metadata(processed_experiments_query, key=connection.ff_keys, ff_env=connection.ff_env)
        for expset in search_res:
            expsets_by_accession[ expset["accession"] ] = expset

        # Exclude any ExpSets with static content with the description "auto_generated_higlass_view_config"
        static_content_query = '/search/?type=ExperimentSetReplicate&static_content.description=auto_generated_higlass_view_config&field=accession'
        search_res = ff_utils.search_metadata(static_content_query, key=connection.ff_keys, ff_env=connection.ff_env)
        for expset in search_res:
            if expset["accession"] in expsets_by_accession:
                del expsets_by_accession[ expset["accession"] ]

    # Get reference files
    reference_files_by_ga = get_reference_files(connection)
    check.full_output['reference_files'] = reference_files_by_ga

    target_files_by_ga = {}
    higlass_count = 0
    expset_count = 0

    for expset_accession, expset in expsets_by_accession.items():
        # Get all of the processed files
        file_info = gather_processedfiles_for_expset(expset)

        if file_info["error"]:
            continue

        # Add reference files to the list based on the Genome Assembly
        processed_file_genome_assembly = file_info["genome_assembly"]

        if processed_file_genome_assembly not in target_files_by_ga:
            target_files_by_ga[ processed_file_genome_assembly ] = {}
        target_files_by_ga[ processed_file_genome_assembly ][expset_accession] = {
            "static_content" : expset.get("static_content", []),
            "files" : file_info["files"],
            "description": expset["description"],
        }
        higlass_count += 1
        expset_count += 1

    # Generate check response
    check.full_output['target_files'] = target_files_by_ga
    check.status = 'PASS'

    if not target_files_by_ga:
        # nothing new to generate
        check.summary = check.description = "No new view configs to generate"
    else:
        check.summary = "Ready to generate {higlass_count} Higlass view configs for {exp_sets} Experiment Sets".format(higlass_count=higlass_count, exp_sets=expset_count)
        check.description = check.summary + ". See full_output for details."
        check.allow_action = True
    return check

@action_function(expset_accession=None, one_per_genome_assembly=False)
def patch_expsets_processedfiles_for_higlass_viewconf(connection, **kwargs):
    """ Create, Post and Patch HiGlass viewconfig files for the given Experiment Sets.

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                expset_accession(string, optional, default=None): Only generate a viewconf for the given Experiment Set acccession.
                one_per_genome_assembly(boolean, optional, default=False): Only generate one viewconf per genome assembly

        Returns:
            A check/action object.
    """
    action = init_action_res(connection, 'patch_expsets_processedfiles_for_higlass_viewconf')

    action_logs = {
        'successes': {},
        'failed_to_create_viewconf': {},
        'failed_to_patch_expset': {}
    }

    # get latest results
    gen_check = init_check_res(connection, 'check_expsets_processedfiles_for_higlass_viewconf')
    if kwargs.get('called_by', None):
        gen_check_result = gen_check.get_result_by_uuid(kwargs['called_by'])
    else:
        gen_check_result = gen_check.get_primary_result()

    # make the fourfront auth key (in basic auth format)
    ff_auth = (connection.ff_keys['key'], connection.ff_keys['secret'])
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json'}

    # Checks expire after 280 seconds, so keep track of how long this task has lasted.
    start_time = time.time()
    time_expired = False

    # Get the reference files
    ref_files_by_ga = gen_check_result['full_output'].get('reference_files', {})
    target_files = gen_check_result['full_output'].get('target_files', {})
    #For each ExpSet with collected files:
    for ga in target_files:
        if time_expired:
            break

        if ga not in ref_files_by_ga:
            continue
        ref_files = ref_files_by_ga[ga]

        for expset_accession, file_info in target_files[ga].items():
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            # If a specific uuid was desired, skip the others
            if kwargs['expset_accession'] and expset_accession != kwargs['expset_accession']:
                continue

            files_for_viewconf = file_info["files"]
            static_content_section = file_info["static_content"]

            higlass_title = "{acc} Processed Files".format(acc=expset_accession)
            higlass_desc = file_info["description"] + ": " + ", ".join(files_for_viewconf)

            # Post a new Higlass viewconf using the file list
            post_viewconf_results = post_viewconf_to_visualization_endpoint(connection, ref_files, files_for_viewconf, ff_auth, headers, higlass_title, higlass_desc)

            if post_viewconf_results["error"]:
                action_logs['failed_to_create_viewconf'][expset_accession] = post_viewconf_results["error"]
                continue

            # Create a new static content section with the description = "auto_generated_higlass_view_config" and the new viewconf as the content
            # Patch the ExpSet static content
            successful_patch, patch_error =  add_viewconf_static_content_to_file(connection, expset_accession, post_viewconf_results["view_config_uuid"],
            static_content_section,
            "tab:processed-files")

            if not successful_patch:
                action_logs['failed_to_patch_expset'][expset_accession] = patch_error
                continue

            # Report success.
            action_logs['successes'][expset_accession] = post_viewconf_results["view_config_uuid"]

            # If only one per genome assembly, break out of the inner loop.
            if kwargs["one_per_genome_assembly"]:
                break

    action.status = 'DONE'
    action.output = action_logs
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
    unique_accession = {}
    unique_files = []
    for pf in processed_files:
        if pf["accession"] in unique_accession:
            continue
        unique_accession[pf["accession"]] = pf
        unique_files.append(pf["accession"])

    return {
        "error": "",
        "files": unique_files,
        "genome_assembly": processed_files[0]["genome_assembly"]
    }

@check_function(expset_accession=None)
def check_expsets_otherprocessedfiles_for_higlass_viewconf(connection, **kwargs):
    """ Check to generate Higlass view configs on Fourfront for Experiment Sets Other Processed Files (aka Supplementary Files.)

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                expset_accession: Only check this expset.

        Returns:
            check results object.
    """

    check = init_check_res(connection, 'check_expsets_otherprocessedfiles_for_higlass_viewconf')
    check.full_output = {}
    check.action = 'patch_expsets_otherprocessedfiles_for_higlass_viewconf'

    # If an accession was specified, get it
    if kwargs['expset_accession']:
        expsets_by_accession = {
            kwargs['expset_accession'] : ff_utils.get_metadata(kwargs['expset_accession'], key=connection.ff_keys, ff_env=connection.ff_env, add_on="frame=embedded")
        }
    else:
        # Otherwise search for all relevant Experiment Sets
        # get the fields you need to include
        fields_to_include = ""
        for new_field in (
            "accession",
            "other_processed_files",
            "experiments_in_set",
            "description",
        ):
            fields_to_include += "&field=" + new_field

        expects_by_accession = {}

        # Include ExpSets whose Other Processed File groups have higlass_uid
        expset_query = '/search/?type=ExperimentSetReplicate&other_processed_files.files.higlass_uid%21=No+value' + fields_to_include
        search_res = ff_utils.search_metadata(expset_query, key=connection.ff_keys, ff_env=connection.ff_env)

        # store results by accession
        expsets_by_accession = {expset["accession"]: expset for expset in search_res }

        # Include ExpSets whose Experiments have Other Processed File groups with higlass_uid
        expset_query = '/search/?type=ExperimentSetReplicate&experiments_in_set.other_processed_files.files.higlass_uid%21=No+value' + fields_to_include
        search_res = ff_utils.search_metadata(expset_query, key=connection.ff_keys, ff_env=connection.ff_env)
        for expset in search_res:
            expsets_by_accession[ expset["accession"] ] = expset

    # Get reference files
    reference_files_by_ga = get_reference_files(connection)
    check.full_output['reference_files'] = reference_files_by_ga

    # Create a helper function that finds files with higlass_uid and the genome assembly
    def find_higlass_files(other_processed_files, filegroups_to_update, description):
        # For each ExpSet Other Processed Filegroup without a higlass_view_config
        for filegroup in [ fg for fg in other_processed_files if not fg.get("higlass_view_config", None) ]:
            genome_assembly = None
            title = filegroup["title"]
            higlass_file_found = False

            # Find every file with a higlass_uid
            for fil in [ f for f in filegroup["files"] if f.get("higlass_uid", None) ]:
                higlass_file_found = True
                # Create new entry and copy genome assembly and filegroup type
                if not title in filegroups_to_update:
                    filegroups_to_update[title] = {
                        "genome_assembly": fil["genome_assembly"],
                        "files": [],
                        "type": filegroup["type"],
                        "description": description
                    }

                # add file accessions to this group
                filegroups_to_update[title]["files"].append(fil["accession"])
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
        if "other_processed_files" in expset:
            find_higlass_files(expset["other_processed_files"], filegroups_to_update, expset["description"])

            expset_titles = { fg["title"] for fg in expset["other_processed_files"] }

            expset_titles_with_higlass = [ fg["title"] for fg in expset["other_processed_files"] if fg.get("higlass_view_config", None) ]

        # Scan each Experiment in set to look for other processed file groups with higlass_uid .
        experiments_in_set_to_update = {}
        for experiment in expset.get("experiments_in_set", []):
            if "other_processed_files" in experiment:
                find_higlass_files(experiment["other_processed_files"], experiments_in_set_to_update, expset["description"])

        for title, info in experiments_in_set_to_update.items():
            # Skip the experiment's file if the higlass view has already been generated.
            if title in expset_titles_with_higlass:
                continue

            # Create the filegroup based on the experiment if:
            # - It doesn't exist in the ExpSet
            # - It does exist in the ExpSet, but the ExpSet didn't have any files to generate higlass uid with.
            if not (title in expset_titles and title in filegroups_to_update):
                filegroups_to_update[title] = {
                    "genome_assembly": info["genome_assembly"],
                    "files": []
                }

            # Add the files to the existing filegroup
            filegroups_to_update[title]["files"] += info["files"]

        # If at least one filegroup needs to be updated, then record the ExpSet and its other_processed_files section.
        if filegroups_to_update:
            filegroups_info = expset.get("other_processed_files", [])

            expsets_to_update[accession] = filegroups_info

            # Replace file description with just the accessions
            for fg in expsets_to_update[accession]:
                accessions = [ f["accession"] for f in fg["files"] ]
                fg["files"] = accessions

            all_filegroups_to_update[accession] = filegroups_to_update
            higlass_view_count += len(filegroups_to_update.keys())

    # check announces success
    check.full_output['filegroups_to_update'] = all_filegroups_to_update
    check.full_output['expsets_to_update'] = expsets_to_update
    check.status = 'PASS'

    if not all_filegroups_to_update:
        # nothing new to generate
        check.summary = check.description = "No new view configs to generate"
    else:
        check.summary = "Ready to generate {file_count} Higlass view configs for {exp_sets} Experiment Set".format(file_count=higlass_view_count, exp_sets=len(expsets_to_update))
        check.description = check.summary + ". See full_output for details."
        check.allow_action = True
    return check

@action_function(expset_accession=None)
def patch_expsets_otherprocessedfiles_for_higlass_viewconf(connection, **kwargs):
    """ Create, Post and Patch HiGlass viewconfig files for the given Experiment Sets and their Other Processed Files (aka Supplementary files) entries

        Args:
            connection: The connection to Fourfront.
            **kwargs, which may include:
                expset_accession(string, optional, default=None): Only generate a viewconf for the given Experiment Set acccession.

        Returns:
            A check/action object.
    """
    action = init_action_res(connection, 'patch_expsets_otherprocessedfiles_for_higlass_viewconf')

    action_logs = {
        'successes': {},
        'failed_to_create_viewconf': {},
        'failed_to_patch_expset': {}
    }

    # get latest results
    gen_check = init_check_res(connection, 'check_expsets_otherprocessedfiles_for_higlass_viewconf')
    if kwargs.get('called_by', None):
        gen_check_result = gen_check.get_result_by_uuid(kwargs['called_by'])
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

    # For each expset we want to update
    for accession in expsets_to_update:
        # If we've taken more than 270 seconds to complete, break immediately
        if time_expired:
            break

        # If a particular expset was used as an argument, reject the others.
        if kwargs["expset_accession"] and kwargs["expset_accession"] != accession:
            continue

        # Look in the filegroups we need to update for that ExpSet
        new_viewconfs = {}
        for title, info in filegroups_to_update[accession].items():
            # If we've taken more than 270 seconds to complete, break immediately
            if time.time() - start_time > 270:
                time_expired = True
                break

            # Get the reference files for the genome assembly
            reference_files = ref_files_by_ga[ info["genome_assembly"] ]

            # Create the Higlass Viewconf and get the uuid
            data_files = info["files"]
            higlass_title = "{acc} - {title}".format(acc=accession, title=title)
            higlass_desc = info["description"] + " :" + ", ".join(data_files)

            post_viewconf_results =  post_viewconf_to_visualization_endpoint(
                connection,
                reference_files,
                data_files,
                ff_auth,
                headers,
                higlass_title,
                higlass_desc
            )

            if post_viewconf_results["error"]:
                if accession not in action_logs['failed_to_create_viewconf']:
                    action_logs['failed_to_create_viewconf'][accession] = {}
                if title not in action_logs['failed_to_create_viewconf'][accession]:
                    action_logs['failed_to_create_viewconf'][accession][title] = {}

                action_logs['failed_to_create_viewconf'][accession][title] = post_viewconf_results["error"]
                continue

            # If the filegroup title is not in the ExpSet other_processed_files section, make it now
            matching_title_filegroups = [ fg for fg in expsets_to_update[accession] if fg.get("title", None) == title]
            if not matching_title_filegroups:
                newfilegroup = deepcopy(info)
                del newfilegroup["genome_assembly"]
                newfilegroup["files"] = []
                newfilegroup["title"] = title
                expsets_to_update[accession].append(newfilegroup)
                matching_title_filegroups = [ newfilegroup, ]

            # Add the higlass_view_config to the filegroup
            matching_title_filegroups[0]["higlass_view_config"] = post_viewconf_results["view_config_uuid"]
            matching_title_filegroups[0]["higlass_view_config"]

            new_viewconfs[title] = post_viewconf_results["view_config_uuid"]

        # The other_processed_files section has been updated. Patch the changes.
        try:
            # Make sure all higlass_view_config fields just show the uuid.
            for g in expsets_to_update[accession]:
                if isinstance(g["higlass_view_config"], dict):
                    uuid = g["higlass_view_config"]["uuid"]
                    g["higlass_view_config"] = uuid

            ff_utils.patch_metadata(
                {'other_processed_files': expsets_to_update[accession]},
                obj_id=accession,
                key=connection.ff_keys,
                ff_env=connection.ff_env
            )
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
        file_format_filter = "?file_format.file_format=" + filetypes_to_use[0] + "&file_format.file_format=".join(filetypes_to_use[1:])

        # Build the query that finds all published files.
        search_query = 'search/' + file_format_filter + type_filter

        # Make sure it's published
        search_query += '&status!=uploading&status!=to+be+uploaded+by+workflow&status!=upload+failed'

        # Only request the necessary fields
        for new_field in (
            "accession",
            "genome_assembly",
            "file_format",
            "higlass_uuid",
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
                for extra in procfile.get('extra_files', []):
                    if extra['file_format'].get('display_title') == type2extra[file_format] and 'upload_key' in extra:
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
        check.summary = check.description = "Some files cannot be registed. See full_output"
    else:
        check.status = 'PASS'

    file_count = sum([len(files_to_be_reg[ft]) for ft in files_to_be_reg])
    if file_count != 0:
        check.status = 'WARN'
    if check.summary:
        check.summary += '. %s files ready for registration' % file_count
        check.description += '. %s files ready for registration.' % file_count
        if not kwargs['confirm_on_higlass']:
            check.description += "Run with confirm_on_higlass=True to check against the higlass server"
    else:
        check.summary = '%s files ready for registration' % file_count
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
            elif ftype in ['bg', 'bw']:
                payload["filepath"] = connection.ff_s3.outfile_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'bigwig'
                payload['datatype'] = 'vector'
            elif ftype == 'bed':
                payload["filepath"] = connection.ff_s3.outfile_bucket + "/" + hit['upload_key']
                payload['filetype'] = 'beddb'
                payload['datatype'] = 'bedlike'
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
                # Add reason for failure
                action_logs['registration_failure'][hit['accession']] = res.json().get("error", res.status_code)
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
    check.status = 'PASS'

    if num_viewconfigs == 0:
        check.summary = check.description = "No new items to purge."
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