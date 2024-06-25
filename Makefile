install :
	pip install --upgrade pip &&\
	pip install -e ".[dev]"

lint:
	export PYTHONPATH=$PYTHONPATH:$(pwd)/src
	pylint -v --fail-under=8 src