install :
	pip install --upgrade pip &&\
	pip install -e ".[dev]"

lint:
	pylint -v --fail-under=8 src