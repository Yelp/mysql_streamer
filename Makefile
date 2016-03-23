.PHONY: clean venv-dev test itest build-image compose-prefix

DOCKER_TAG ?= replication-handler-dev-$(USER)

test:
	tox

itest: cook-image

cook-image:
	docker build -t $(DOCKER_TAG) .

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

venv-dev:
	virtualenv --python=python2.7 ./virtualenv_run
	./virtualenv_run/bin/pip install -i https://pypi-dev.yelpcorp.com/simple  -r ./requirements-dev.txt

install-hooks:
	tox -e pre-commit -- install -f --install-hooks

compose-prefix:
	@python -c "from data_pipeline.testing_helpers.containers import Containers; print Containers.compose_prefix()"

get-venv-update:
	curl http://yelp.github.io/venv-update/install.txt | bash -s v1.0.0
