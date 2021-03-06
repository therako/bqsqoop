.PHONY: develop test coverage clean lint pre-commit upload-package

default: coverage

ci: clean test-setup test-ci lint coverage

develop:
	python setup.py develop
	pip install -r test-requirements.txt
	pip install flake8 restructuredtext_lint
	@echo "#!/bin/bash\nmake pre-commit" > .git/hooks/pre-push
	@chmod a+x .git/hooks/pre-push
	@echo
	@echo "Added pre-push hook! To run manually: make pre-commit"

test-setup:
	python setup.py develop

test:
	tox

test-ci:
	py.test tests

coverage:
	py.test --cov=bqsqoop tests

coverage-html: coverage clean-coverage-html
	coverage html

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

clean-coverage-html:
	rm -rf htmlcov

clean: clean-pyc clean-build clean-coverage-html

lint-rst:
	rst-lint README.rst

lint-pep8:
	flake8 bqsqoop tests

lint: lint-rst lint-pep8

pre-commit: coverage lint

upload-package: test lint clean
	pip install twine wheel
	python setup.py sdist bdist_wheel
	twine upload dist/*

upload-dockerimage: test lint clean
	@bash ./docker/build_and_upload_image.sh
	