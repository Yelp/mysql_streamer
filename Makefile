.PHONY: clean venv-dev test itest build-image compose-prefix

DOCKER_TAG ?= replication-handler-dev-$(USER)

test:
	tox

itest: cook-image
	DOCKER_TAG=$(DOCKER_TAG) tox -e itest

itest_db:
	tox -e itest_db

cook-image:
	docker build -t $(DOCKER_TAG) .

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

venv-dev:
	virtualenv --python=python2.7 ./virtualenv_run
	./virtualenv_run/bin/pip install -i https://pypi.yelpcorp.com/simple/  -r requirements.d/dev.txt -r requirements.txt

install-hooks:
	tox -e pre-commit -- install -f --install-hooks

compose-prefix:
	@echo "DOCKER_TAG=$(DOCKER_TAG) `python -c "from data_pipeline.testing_helpers.containers import Containers; print Containers.compose_prefix()"`"

interactive-streamer: cook-image
	DOCKER_TAG=$(DOCKER_TAG) python interactive_streamer.py
