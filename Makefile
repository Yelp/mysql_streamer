.PHONY: test clean

test:
	python -m pytest tests/

tests:
	test

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
