.PHONY: test clean

test:
	tox

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
