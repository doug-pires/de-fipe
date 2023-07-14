test_config:
	poetry run pytest tests/test_config.py -v --setup-show -s

test_extract:
	poetry run pytest tests/test_extract.py -v --setup-show -s

test_load:
	poetry run pytest tests/test_load.py -v --setup-show -s

test_transform:
	poetry run pytest tests/test_transform.py -v --setup-show -s

test_all:
	poetry run pytest tests -v --setup-show -s