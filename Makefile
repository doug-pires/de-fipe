testconfig:
	poetry run pytest -v tests/test_config.py -v --setup-show -s

testextract:
	poetry run pytest -v tests/test_extract.py -v --setup-show -s