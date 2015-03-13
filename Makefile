.PHONY: clean venv test

test:
	tox

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

venv:
	virtualenv --python=python2.7 ./virtualenv_run;\
	source ./virtualenv_run/bin/activate;\
	./virtualenv_run/bin/pip install -i https://pypi-dev.yelpcorp.com/simple  -r ./requirements-dev.txt;\
