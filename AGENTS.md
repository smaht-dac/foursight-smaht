# foursight-smaht — agent memory

Serverless AWS Chalice app that monitors and runs tasks against `smaht-portal`
(https://data.smaht.org). See `README.rst`.

## The one orienting fact

This repo is a **thin SMaHT-specific layer over `foursight-core`** (pinned in
`pyproject.toml`). Nearly everything here subclasses a `*_from_core` base and
overrides SMaHT specifics:

- `chalicelib_smaht/app_utils.py` — `AppUtils(AppUtils_from_core)`, the Chalice
  app wiring; sets prefix, host, favicon, default env.
- `chalicelib_smaht/deploy.py`, `package.py` — `Deploy` / `PackageDeploy`
  subclasses (generate the gitignored `.chalice/config.json`).
- `chalicelib_smaht/buckets.py` — `Buckets` subclass; SMaHT env list + URLs.
- `chalicelib_smaht/checks/helpers/confchecks.py` — re-exports the
  `foursight_core` decorators (`check_function`, `action_function`,
  `CheckResult`, `ActionResult`). **All checks import from here.**

Rule of thumb: the real machinery lives in `foursight-core`; this repo holds the
SMaHT overrides and the actual check/action bodies. When behavior is missing
here, look upstream in `foursight-core`.

## Layout

- `app.py` — Chalice entry point for **local** runs only (`chalice local`); the
  deployed entry point lives in `4dn-cloud-infra`. See the comment at its top.
- `chalicelib_smaht/` — the package (`package_name = 'chalicelib_smaht'`).
  - `check_schedules.py` — cron schedules per stage (`dev`/`prod`) and the
    `@schedule`-decorated lambda entry points + `check_runner`.
  - `check_setup.json` — which checks are scheduled/displayed and their kwargs;
    `.json-local` / `.json-smaht-wolf` are env-specific variants.
  - `vars.py` — constants (`FOURSIGHT_PREFIX`, `CHECK_SETUP_FILE`).
  - `checks/*.py` — check + action bodies, grouped by concern: `audit_checks`,
    `ecs_checks`, `es_checks`, `lifecycle_checks`, `system_checks`,
    `wfr_checks` (metaworkflow runs, via `magma_smaht`), `wrangler_checks`.
    `checks/__init__.py` auto-globs every module into `__all__`.
  - `checks/helpers/` — shared utils (`utils.py`, `wfr_utils.py`,
    `wfrset_utils.py`, `wrangler_utils.py`, `lifecycle_utils.py`, `constants.py`).
  - `scripts/local_check_execution.py` — entry point `local-check-execution`
    (see `[tool.poetry.scripts]`) for running a check locally.
- `.chalice/` — Chalice config + IAM policies (`policy-dev.json`,
  `policy-prod.json`). `config.json` here is a committed base; deploy regenerates it.
- `tests/checks/` — pytest suite + JSON fixtures (`*_testdata.json`).
- `scripts/` — `test_check.py`, `migration.py` (dev helpers).
- `vendor/` — vendored PyYAML wheel (Chalice bundling).
- `docs/` — Sphinx source (ReadTheDocs).

## Commands

- Install: `make build` (installs poetry, then `poetry install`).
- Test: `make test` → `pytest -vv tests`. Config in `pyproject.toml` /
  `.coveragerc` / `conftest.py`.
- Run a check locally: `poetry run local-check-execution`.
- Deps and versions: `pyproject.toml` (+ `poetry.lock`). Do not hand-edit versions.

## CI / config conventions

- CI: `.github/workflows/main-CI.yml` runs `make build && make test` on push/PR
  to `master`, using OIDC AWS creds.
- Other workflows: `main-deploy.yml` (manual prod deploy), `main-publish.yml`
  (PyPI on tag push; rewrites `chalicelib_smaht/gitinfo.json`),
  `main-deploy-docs.yml` (ReadTheDocs webhook).

## Sharp edges (non-obvious, durable)

- **Tests are not hermetic.** They expect an AWS-ish environment. CI sets
  `S3_ENCRYPT_KEY`, `DEV_SECRET`, and a dummy `GLOBAL_ENV_BUCKET`; expect
  failures locally without comparable env/secrets.
- **`ES_HOST` is required at import time.** `app_utils.py` raises if it's unset,
  so importing the package (or running most tooling) needs it.
- **Deployment happens from `4dn-cloud-infra`, not here.** Per `app.py`'s
  comment, the deployed Chalice entry point and real deploy live upstream; this
  repo's local deploy targets are secondary. Note: `make deploy-dev` and
  `main-deploy.yml` invoke `python -m chalicelib.deploy`, while the package here
  is `chalicelib_smaht` — treat those local targets as stale/upstream-driven
  rather than the source of truth.
- **`.chalice/config.json` is regenerated on deploy** by `deploy.py` /
  `package.py`; the committed copy is a base.
- Checks are wired to run only if listed in `check_setup.json` with a schedule
  matching one of the cron groups in `check_schedules.py`.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
