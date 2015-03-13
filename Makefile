.PHONY: clean venv-dev test

test:
	tox

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

venv-dev:
	virtualenv --python=python2.7 ./virtualenv_run;\
	./virtualenv_run/bin/pip install -i https://pypi-dev.yelpcorp.com/simple  -r ./requirements-dev.txt;\
