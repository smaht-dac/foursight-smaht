===============
foursight-smaht
===============


----------
Change Log
----------

0.8.10
======
* Make sure that files that don't require lifecycle updates receive an updated `s3_lifecycle_last_checked`` property.


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
