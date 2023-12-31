.PHONY: test

clear-poetry-cache:  # clear poetry/pypi cache. for user to do explicitly, never automatic
	poetry cache clear pypi --all

configure:  # does any pre-requisite installs
	pip install poetry==1.4.2

build:  # builds
	make configure
	poetry install

update:
	poetry update

test:
	pytest -vv tests

publish:
	# New Python based publish script in dcicutils (2023-04-25).
	poetry run publish-to-pypi

publish-for-ga:
	# New Python based publish script in dcicutils (2023-04-25).
	poetry run publish-to-pypi --noconfirm

deploy-dev:
	python -m chalicelib.deploy dev

info:
	@: $(info Here are some 'make' options:)
	   $(info - Use 'make configure' to install poetry, though 'make build' will do it automatically.)
	   $(info - Use 'make build' to install dependencies using poetry.)
	   $(info - Use 'make test' to run tests with the normal options we use on travis)
	   $(info - Use 'make publish' to publish this library manually.)
	   $(info - Use 'make update' to update dependencies)
	   $(info - Use 'make deploy-dev' to deploy the foursight-cgap dev stage in the main account.)
