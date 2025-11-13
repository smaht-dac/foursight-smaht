===============
foursight-smaht
===============


----------
Change Log
----------

0.8.17
======
* Update the sample_consistency_check to ensure that all GCC samples have a corresponding TPC sample with matching external_id.
* and report these cases as well as when multiple GCC samples correspond to one TPC sample.
* and still check for consistent properties between matching samples - (only a few key properties for now).

0.8.16
=========
* Adjust schedule for md5run_status check in check_setup.json to hourly_checks until issue with 30_min check schedule is resolved.

0.8.15
=========
* Add 30 min schedule to check_schedules.py
* Update check_setup.json to include higher max_files as default
* bug fix for check_for_new_submissions in audit_checks.py:
  - comparison of numbers was reversed should be last_result_count >= current_result_count
  - added handling for last_result having status CHECK_ERROR
* string formatting bug fix for untagged_donors_with_released_files in wrangler_checks.py:


0.8.14
=========
* Bug fixes to a couple of checks that were expected a field that is not present in results from search_metadata
* in audit_checks.py check_submitted_md5: changed content_md5sum to md5sum
* added constatns and used them for CHECK_PASS, CHECK_WARN
* removed depenency on empty indexing queue for md5run_status check in wfr_checks.py
* increase max_files from 50 to 300 for md5run_status check in wfr_checks.py
* adjusted check_setup.json for md5run_status check to run every 30min instead of 15min

0.8.13
=========
* Added check and associated action to checks/wrangler_checks.py `untagged_donors_with_released_files` that looks for donors that have released files but are missing the 'has_released_files' tag, and adds the tag if missing.
* NOTE: added to the Audit Check group in UI for now and scheduled to run daily in `morning_checks` schedule
* action is initially not cued and will need to be run manually


0.8.12
=========
* Added check to checks/audit_checks.py `check_tissue_sample_properties` that is a weekly check of GCC/TTD-submitted tissue samples to make sure they match particular metadata from corresponding TPC-submitted tissue sample item (with a matching external_id)
* Also checks for more than one GCC/TTD-submitted tissue sample corresponding to one TPC-submitted tissue sample, as they should be one-to-one and would indicate a mislabel


0.8.11
======
* Removed tests/checks/helpers/test_wfr_utils.py (just a single import) which was causing tests to fail with:
  ERROR collecting tests/checks/helpers/test_wfr_utils.py
  import file mismatch: imported module 'test_wfr_utils' has
  this __file__ attribute: /Users/dmichaels/repos/foursight-smaht/tests/checks/test_wfr_utils.py
  which is not the same as the test file we want to collect:
  /Users/dmichaels/repos/foursight-smaht/tests/checks/helpers/test_wfr_utils.py


0.8.10
======
* Make sure that files that don't require lifecycle updates receive an updated `s3_lifecycle_last_checked`` property.
* Pull in latest dependencies (especially dcicutils 8.16.6)


0.8.9
=====
* 2024-10-11/dmichaels
* Updated dcicutils version (8.16.1) for vulnerabilities.


0.8.8
=====
* Support for Python 3.12.
* Changed environment names in chalicelib_smaht/check_setup.json to "<env-name>" as they should
  have been as these get expanded on-the-fly in foursight-core/.../check_utils.py/expand_check_setup.


0.8.7
=====
* Bump Tibanna version (secure AMI update)


0.8.6
=====
* Bump Magma version


0.8.5
=====
* Switch to 15min interval for md5


0.8.4
=====
* Increase md5 run frequency


0.8.3
=====
* Adds an audit check intended to run weekly to detect new submission not made by us


0.8.2
=====
* Adds an audit check for submitted md5 consistency


0.8.1
=====
* Use correct envs in md5 check


0.8.0
=====
* Update foursight-core version for UI fixes WRT environment names;
  and changes to local-check-execution script.
* Use fs_env instead of ff_env in pipeline checks


0.7.2
=====
* Run pipeline and lifecycle checks only on data


0.7.1
=====
* Adjust md5 check setup


0.7.0
=====
* Add md5 check
* Add long running EC2 check


0.6.0
=====
* Added update of a gitinfo.json file in GitHub Actions (.github/workflows/main-publish.yml).
* Update foursight-core with fix to Portal Reindex page (to not show initial deploy).


0.5.0
=====
* Add lifecycle management checks



0.4.0
=====
* Minor UI fix to Ingestion page (foursight-core).


0.3.0
=====
* ?


0.2.0
=====

* New Portal Reindex and Redeploy pages; foursight-core 5.1.0.
* Update poetry to 1.4.2.
* Update dcicutils to 8.0.0.


0.1.0
=====

* Update to Python 3.11.
* Removed get_metadata_for_cases_to_clone from wrangler_checks.py (no cases in SMaHT).
* Added local_check_execution.py.

0.0.1
=====

* Update from foursight-cgap base into foursight-smaht
* Remove old, unused checks
* Change identifiers
* Clean up various small issues
* For foursight-cgap CHANGELOG, see that repository
