.PHONY: clean venv-dev test itest build-image

DOCKER_TAG ?= replication-handler-dev-$(USER)

test:
	tox

itest: build-image

build-image:
	docker build -t $(DOCKER_TAG) .

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

venv-dev:
	virtualenv --python=python2.7 ./virtualenv_run
	./virtualenv_run/bin/pip install -i https://pypi-dev.yelpcorp.com/simple  -r ./requirements-dev.txt

install-hooks:
	tox -e pre-commit -- install -f --install-hooks
