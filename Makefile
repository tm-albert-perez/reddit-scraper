install :
	pip install --upgrade pip &&\
	pip install -e ".[dev]"

lint:
	pylint --fail-under=8 src