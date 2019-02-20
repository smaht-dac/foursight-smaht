from __future__ import print_function, unicode_literals
from ..utils import (
    check_function,
    init_check_res,
    action_function,
    init_action_res
)
from dcicutils import ff_utils
import re
import requests
import datetime
import json


def stringify(item):
    if isinstance(item, str):
        return item
    elif isinstance(item, list):
        if len(item) == 1:
            return stringify(item[0])
        elif all(isinstance(i, dict) for i in item):
            return str([sorted(i.items()) for i in item])
    elif isinstance(item, dict):
        return str(sorted(item.items()))
    return str(item)


def compare_badges(obj_ids, item_type, badge, ffenv):
    '''
    Compares items that should have a given badge to items that do have the given badge.
    Used for badges that utilize a single message choice.
    Input (first argument) should be a list of item @ids.
    '''
    search_url = 'search/?type={}&badges.badge.@id=/badges/{}/'.format(item_type, badge)
    has_badge = ff_utils.search_metadata(search_url + '&frame=object', ff_env=ffenv)
    needs_badge = []
    badge_ok = []
    remove_badge = {}
    for item in has_badge:
        if item['@id'] in obj_ids:
            # handle differences in badge messages
            badge_ok.append(item['@id'])
        else:
            keep = [badge_dict for badge_dict in item['badges'] if badge not in badge_dict['badge']]
            remove_badge[item['@id']] = keep
    for other_item in obj_ids:
        if other_item not in badge_ok:
            needs_badge.append(other_item)
    return needs_badge, remove_badge, badge_ok


def compare_badges_and_messages(obj_id_dict, item_type, badge, ffenv):
    '''
    Compares items that should have a given badge to items that do have the given badge.
    Also compares badge messages to see if the message is the right one or needs to be updated.
    Input (first argument) should be a dictionary of item's @id and the badge message it should have.
    '''
    search_url = 'search/?type={}&badges.badge.@id=/badges/{}/'.format(item_type, badge)
    has_badge = ff_utils.search_metadata(search_url + '&frame=object', ff_env=ffenv)
    needs_badge = {}
    badge_edit = {}
    badge_ok = []
    remove_badge = {}
    for item in has_badge:
        if item['@id'] in obj_id_dict.keys():
            # handle differences in badge messages
            for a_badge in item['badges']:
                if a_badge['badge'].endswith(badge + '/'):
                    if a_badge['message'] == obj_id_dict[item['@id']]:
                        badge_ok.append(item['@id'])
                    else:
                        a_badge['message'] = obj_id_dict[item['@id']]
                        badge_edit[item['@id']] = item['badges']
        else:
            this_badge = [a_badge for a_badge in item['badges'] if badge in a_badge['badge']][0]
            item['badges'].remove(this_badge)
            remove_badge[item['@id']] = item['badges']
    for key, val in obj_id_dict.items():
        if key not in badge_ok + list(badge_edit.keys()):
            needs_badge[key] = val
    return needs_badge, remove_badge, badge_edit, badge_ok


def patch_badges(full_output, badge_name, output_keys, ffenv, single_message=''):
    '''
    General function for patching badges.
    For badges with single message choice:
    - single_message kwarg should be assigned a string to be used for the badge message;
    - full_output[output_keys[0]] should be a list of item @ids;
    - no badges are edited, they are only added or removed.
    For badges with multiple message options:
    - single_message kwarg should not be used, but left as empty string.
    - full_output[output_keys[0]] should be a list of item @ids and message to patch into badge.
    - badges can also be edited to change the message.
    '''
    patches = {'add_badge_success': [], 'add_badge_failure': [],
               'remove_badge_success': [], 'remove_badge_failure': []}
    badge_id = '/badges/' + badge_name + '/'
    if isinstance(full_output[output_keys[0]], list):
        add_list = full_output[output_keys[0]]
    elif isinstance(full_output[output_keys[0]], dict):
        patches['edit_badge_success'] = []
        patches['edit_badge_failure'] = []
        add_list = full_output[output_keys[0]].keys()
    for add_key in add_list:
        add_result = ff_utils.get_metadata(add_key + '?frame=object', ff_env=ffenv)
        badges = add_result['badges'] if add_result.get('badges') else []
        badges.append({'badge': badge_id, 'message': single_message if single_message else full_output[output_keys[0]][add_key]})
        if [b['badge'] for b in badges].count(badge_id) > 1:
            # print an error message?
            patches['add_badge_failure'].append('{} already has badge'.format(add_key))
            continue
        try:
            response = ff_utils.patch_metadata({"badges": badges}, add_key[1:], ff_env=ffenv)
            if response['status'] == 'success':
                patches['add_badge_success'].append(add_key)
            else:
                patches['add_badge_failure'].append(add_key)
        except Exception:
            patches['add_badge_failure'].append(add_key)
    for remove_key, remove_val in full_output[output_keys[1]].items():
        # delete field if no badges?
        try:
            if remove_val:
                response = ff_utils.patch_metadata({"badges": remove_val}, remove_key, ff_env=ffenv)
            else:
                response = ff_utils.patch_metadata({}, remove_key + '?delete_fields=badges', ff_env=ffenv)
            if response['status'] == 'success':
                patches['remove_badge_success'].append(remove_key)
            else:
                patches['remove_badge_failure'].append(remove_key)
        except Exception:
            patches['remove_badge_failure'].append(remove_key)
    if len(output_keys) > 2:
        for edit_key, edit_val in full_output[output_keys[2]].items():
            try:
                response = ff_utils.patch_metadata({"badges": edit_val}, edit_key, ff_env=ffenv)
                if response['status'] == 'success':
                    patches['edit_badge_success'].append(edit_key)
                else:
                    patches['edit_badge_failure'].append(edit_key)
            except Exception:
                patches['edit_badge_failure'].append(edit_key)
    return patches


@check_function()
def repsets_have_bio_reps(connection, **kwargs):
    check = init_check_res(connection, 'repsets_have_bio_reps')

    results = ff_utils.search_metadata('search/?type=ExperimentSetReplicate&frame=object',
                                       ff_env=connection.ff_env, page_limit=50)

    audits = {'single_experiment': [], 'single_biorep': [], 'biorep_nums': [], 'techrep_nums': []}
    by_exp = {}
    for result in results:
        rep_dict = {}
        exp_audits = []
        if result.get('replicate_exps'):
            rep_dict = {}
            for exp in result['replicate_exps']:
                if exp['bio_rep_no'] in rep_dict.keys():
                    rep_dict[exp['bio_rep_no']].append(exp['tec_rep_no'])
                else:
                    rep_dict[exp['bio_rep_no']] = [exp['tec_rep_no']]
        if rep_dict:
        # check if only 1 experiment present in set
            if len(result['replicate_exps']) == 1:
                audits['single_experiment'].append('{} contains only 1 experiment'.format(result['@id']))
                exp_audits.append('Replicate set contains only a single experiment')
            # check for technical replicates only
            elif len(rep_dict.keys()) == 1:
                audits['single_biorep'].append('{} contains only a single biological replicate'.format(result['@id']))
                exp_audits.append('Replicate set contains only a single biological replicate')
            # check if bio rep numbers not in sequence
            elif sorted(list(rep_dict.keys())) != list(range(min(rep_dict.keys()), max(rep_dict.keys()) + 1)):
                audits['biorep_nums'].append('Biological replicate numbers of {} are not in sequence:'
                                             ' {}'.format(result['@id'], str(sorted(list(rep_dict.keys())))))
                exp_audits.append('Biological replicate numbers are not in sequence')
        # check if tech rep numbers not in sequence
            else:
                for key, val in rep_dict.items():
                    if sorted(val) != list(range(min(val), max(val) + 1)):
                        audits['techrep_nums'].append('Technical replicates of Bio Rep {} in {} are not in '
                                                      'sequence {}'.format(key, result['@id'], str(sorted(val))))
                        exp_audits.append('Technical replicate numbers of biological replicate {}'
                                          ' are not in sequence'.format(key))
        if exp_audits:
            by_exp[result['@id']] = '; '.join(exp_audits)

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(by_exp, 'ExperimentSetReplicate',
                                                                 'replicate-numbers', connection.ff_env)
    check.action = 'patch_badges_for_replicate_numbers'
    if by_exp:
        check.status = 'WARN'
        check.summary = 'Replicate experiment sets found with replicate number issues'
        check.description = '{} replicate experiment sets found with replicate number issues'.format(len(by_exp.keys()))
    else:
        check.status = 'PASS'
        check.summary = 'No replicate experiment sets found with replicate number issues'
        check.description = '0 replicate experiment sets found with replicate number issues'
    check.full_output = {'New replicate sets with replicate number issues': to_add,
                         'Old replicate sets with replicate number issues': ok,
                         'Replicate sets that no longer have replicate number issues': to_remove,
                         'Replicate sets with a replicate_numbers badge that needs editing': to_edit}
    check.brief_output = audits
    if to_add or to_remove or to_edit:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_replicate_numbers(connection, **kwargs):
    action = init_action_res(connection, 'patch_badges_for_replicate_numbers')

    rep_check = init_check_res(connection, 'repsets_have_bio_reps')
    rep_check_result = rep_check.get_result_by_uuid(kwargs['called_by'])

    rep_keys = ['New replicate sets with replicate number issues',
                'Replicate sets that no longer have replicate number issues',
                'Replicate sets with a replicate_numbers badge that needs editing']

    action.output = patch_badges(rep_check_result['full_output'], 'replicate-numbers', rep_keys, connection.ff_env)
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for replicate numbers'
    return action


@check_function()
def tier1_metadata_present(connection, **kwargs):
    check = init_check_res(connection, 'tier1_metadata_present')

    results = ff_utils.search_metadata('search/?biosource.cell_line_tier=Tier+1&type=Biosample',
                                       ff_env=connection.ff_env)
    missing = {}
    msg_dict = {'culture_start_date': 'Tier 1 Biosample missing Culture Start Date',
                # item will fail validation if missing a start date - remove this part of check?
                'culture_duration': 'Tier 1 Biosample missing Culture Duration',
                'morphology_image': 'Tier 1 Biosample missing Morphology Image'}
    for result in results:
        if len(result.get('biosource')) != 1:
            continue
        elif not result.get('cell_culture_details'):
            missing[result['@id']] = 'Tier 1 Biosample missing Cell Culture Details'
        else:
            messages = [val for key, val in msg_dict.items() if not result['cell_culture_details'].get(key)]
            if messages:
                missing[result['@id']] = '; '.join(messages)

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(missing, 'Biosample',
                                                                 'tier1-metadata-missing',
                                                                 connection.ff_env)
    check.action = 'patch_badges_for_tier1_metadata'
    if missing:
        check.status = 'WARN'
        check.summary = 'Tier 1 biosamples found missing required metadata'
        check.description = '{} tier 1 biosamples found missing required metadata'.format(len(missing.keys()))
    else:
        check.status = 'PASS'
        check.summary = 'All Tier 1 biosamples have required metadata'
        check.description = '0 tier 1 biosamples found missing required metadata'
    check.full_output = {'New tier1 biosamples missing required metadata': to_add,
                         'Old tier1 biosamples missing required metadata': ok,
                         'Tier1 biosamples no longer missing required metadata': to_remove,
                         'Biosamples with a tier1_metadata_missing badge that needs editing': to_edit}
    check.brief_output = list(missing.keys())
    if to_add or to_remove or to_edit:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_tier1_metadata(connection, **kwargs):
    action = init_action_res(connection, 'patch_badges_for_tier1_metadata')

    tier1_check = init_check_res(connection, 'tier1_metadata_present')
    tier1_check_result = tier1_check.get_result_by_uuid(kwargs['called_by'])

    tier1keys = ['New tier1 biosamples missing required metadata',
                 'Tier1 biosamples no longer missing required metadata',
                 'Biosamples with a tier1_metadata_missing badge that needs editing']

    action.output = patch_badges(tier1_check_result['full_output'], 'tier1-metadata-missing', tier1keys, connection.ff_env)
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for missing tier1 metadata.'
    return action


@check_function()
def exp_has_raw_files(connection, **kwargs):
    check = init_check_res(connection, 'exp_has_raw_files')
    # search all experiments except microscopy experiments for missing files field
    no_files = ff_utils.search_metadata('search/?type=Experiment&%40type%21=ExperimentMic&files.uuid=No+value',
                                        ff_env=connection.ff_env)
    # also check sequencing experiments whose files items are all uploading/archived/deleted
    bad_status = ff_utils.search_metadata('search/?status=uploading&status=archived&status=deleted&status=upload+failed'
                                          '&type=FileFastq&experiments.uuid%21=No+value',
                                          ff_env=connection.ff_env)
    bad_status_ids = [item['@id'] for item in bad_status]
    exps = list(set([exp['@id'] for fastq in bad_status for exp in
                     fastq.get('experiments') if fastq.get('experiments')]))
    missing_files = [e['@id'] for e in no_files]
    for expt in exps:
        result = ff_utils.get_metadata(expt, ff_env=connection.ff_env)
        if result.get('status') == 'archived':
            continue
        raw_files = False
        if result.get('files'):
            for fastq in result.get('files'):
                if fastq['@id'] not in bad_status_ids:
                    raw_files = True
                    break
        if not raw_files:
            missing_files.append(expt)

    to_add, to_remove, ok = compare_badges(missing_files, 'Experiment', 'no-raw-files', connection.ff_env)

    if missing_files:
        check.status = 'WARN'
        check.summary = 'Experiments missing raw files found'
        check.description = '{} sequencing experiments are missing raw files'.format(len(missing_files))
    else:
        check.status = 'PASS'
        check.summary = 'No experiments missing raw files'
        check.description = '0 sequencing experiments are missing raw files'
    check.action = 'patch_badges_for_raw_files'
    check.full_output = {'Experiments newly missing raw files': to_add,
                         'Old experiments missing raw files': ok,
                         'Experiments no longer missing raw files': to_remove}
    check.brief_output = missing_files
    if to_add or to_remove:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_raw_files(connection, **kwargs):
    action = init_action_res(connection, 'patch_badges_for_raw_files')

    raw_check = init_check_res(connection, 'exp_has_raw_files')
    raw_check_result = raw_check.get_result_by_uuid(kwargs['called_by'])

    raw_keys = ['Experiments newly missing raw files', 'Experiments no longer missing raw files']

    action.output = patch_badges(raw_check_result['full_output'], 'no-raw-files', raw_keys,
                                 connection.ff_env, single_message='Raw files missing')
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching badges successful for experiments missing raw files.'
    return action


@check_function()
def paired_end_info_consistent(connection, **kwargs):
    # check that fastqs with a paired_end number have a paired_with related_file, and vice versa
    check = init_check_res(connection, 'paired_end_info_consistent')

    search1 = 'search/?type=FileFastq&related_files.relationship_type=paired+with&paired_end=No+value'
    search2 = 'search/?type=FileFastq&related_files.relationship_type!=paired+with&paired_end%21=No+value'

    results1 = ff_utils.search_metadata(search1 + '&frame=object', ff_env=connection.ff_env)
    results2 = ff_utils.search_metadata(search2 + '&frame=object', ff_env=connection.ff_env)

    results = {'paired with file missing paired_end number':
               [result1['@id'] for result1 in results1],
               'file with paired_end number missing "paired with" related_file':
               [result2['@id'] for result2 in results2]}
    results_rev = {item: key for key, val in results.items() for item in val}

    to_add, to_remove, to_edit, ok = compare_badges_and_messages(results_rev, 'FileFastq',
                                                                 'paired-ends-consistent',
                                                                 connection.ff_env)

    if [val for val in results.values() if val]:
        check.status = 'WARN'
        check.summary = 'Inconsistencies found in FileFastq paired end info'
        check.description = ('{} files found with a "paired with" related_file but missing a paired_end number; '
                             '{} files found with a paired_end number but missing related_file info'
                             ''.format(len(results['paired with file missing paired_end number']),
                                       len(results['file with paired_end number missing "paired with" related_file'])))
    else:
        check.status = 'PASS'
        check.summary = 'No inconsistencies in FileFastq paired end info'
        check.description = 'All paired end fastq files have both paired end number and "paired with" related_file'
    check.full_output = {'New fastq files with inconsistent paired end info': to_add,
                         'Old fastq files with inconsistent paired end info': ok,
                         'Fastq files with paired end info now consistent': to_remove,
                         'Fastq files with paired end badge that needs editing': to_edit}
    check.brief_output = results
    check.action = 'patch_badges_for_paired_end_consistency'
    if to_add or to_remove or to_edit:
        check.allow_action = True
    return check


@action_function()
def patch_badges_for_paired_end_consistency(connection, **kwargs):
    action = init_action_res(connection, 'patch_badges_for_paired_end_consistency')

    pe_check = init_check_res(connection, 'paired_end_info_consistent')
    pe_check_result = pe_check.get_result_by_uuid(kwargs['called_by'])

    pe_keys = ['New fastq files with inconsistent paired end info',
               'Fastq files with paired end info now consistent',
               'Fastq files with paired end badge that needs editing']

    action.output = patch_badges(pe_check_result['full_output'], 'paired-ends-consistent', pe_keys, connection.ff_env)
    if [action.output[key] for key in list(action.output.keys()) if 'failure' in key and action.output[key]]:
        action.status = 'FAIL'
        action.description = 'Some items failed to patch. See below for details.'
    else:
        action.status = 'DONE'
        action.description = 'Patching successful for paired end fastq files.'
    return action


@check_function()
def consistent_replicate_info(connection, **kwargs):
    check = init_check_res(connection, 'consistent_replicate_info')

    repset_url = 'search/?type=ExperimentSetReplicate&field=experiments_in_set.%40id'
    exp_url = 'search/?type=Experiment&frame=object'
    bio_url = 'search/?type=Experiment&field=biosample'
    repsets = [item for item in ff_utils.search_metadata(repset_url, ff_env=connection.ff_env) if item.get('experiments_in_set')]
    exps = ff_utils.search_metadata(exp_url, ff_env=connection.ff_env)
    biosamples = ff_utils.search_metadata(bio_url, ff_env=connection.ff_env)
    exp_keys = {exp['@id']: exp for exp in exps}
    bio_keys = {bs['@id']: bs['biosample'] for bs in biosamples}
    fields2check = [
        'lab',
        'award',
        'experiment_type',
        'crosslinking_method',
        'crosslinking_time',
        'crosslinking_temperature',
        'digestion_enzyme',
        'enzyme_lot_number',
        'digestion_time',
        'digestion_temperature',
        'tagging_method',
        'ligation_time',
        'ligation_temperature',
        'ligation_volume',
        'biotin_removed',
        'protocol',
        'protocol_variation',
        'follows_sop',
        'average_fragment_size',
        'fragment_size_range',
        'fragmentation_method',
        'fragment_size_selection_method',
        'rna_tag',
        'target_regions',
        'dna_label',
        'labeling_time',
        'antibody',
        'antibody_lot_id',
        'microscopy_technique',
        'imaging_paths',
    ]
    check.full_output = {}
    for repset in repsets:
        info_dict = {}
        exp_list = [item['@id'] for item in repset['experiments_in_set']]
        for field in fields2check:
            vals = [stringify(exp_keys[exp].get(field)) for exp in exp_list]
            if field == 'average_fragment_size' and 'None' not in vals:
                int_vals = [int(val) for val in vals]
                if max(int_vals) - min(int_vals) < 50:
                    continue
            if len(set(vals)) > 1:
                info_dict[field] = vals
        for bfield in ['treatments_summary', 'modifications_summary']:
            bvals = [stringify(bio_keys[exp].get(bfield)) for exp in exp_list]
            if len(set(bvals)) > 1:
                info_dict[bfield] = bvals
        biosource_vals = [stringify([item['@id'] for item in bio_keys[exp].get('biosource')]) for exp in exp_list]
        if len(set(biosource_vals)) > 1:
            info_dict['biosource'] = biosource_vals
        if [True for exp in exp_list if bio_keys[exp].get('cell_culture_details')]:
            for ccfield in ['synchronization_stage', 'differentiation_stage', 'follows_sop']:
                ccvals = [stringify([item['@id'] for item in bio_keys[exp].get('cell_culture_details').get(ccfield)]) for exp in exp_list]
                if len(set(ccvals)) > 1:
                    info_dict[ccfield] = ccvals
        if [True for exp in exp_list if bio_keys[exp].get('biosample_protocols')]:
            bp_vals = [stringify([item['@id'] for item in bio_keys[exp].get('biosample_protocols', [])]) for exp in exp_list]
            if len(set(bp_vals)) > 1:
                info_dict['biosample_protocols'] = bp_vals
        if info_dict:
            check.full_output[repset['@id']] = info_dict
    check.brief_output = {k: list(v.keys()) for k, v in check.full_output.items()}
    if check.full_output:
        check.status = 'WARN'
        check.summary = 'Inconsistent Replicate Info Found'
        check.description = '{} Replicate Experiment Sets found with inconsistent replicate information'.format(len(check.full_output.keys()))
    else:
        check.status = 'PASS'
        check.summary = 'All Replicate Information Consistent'
    return check
