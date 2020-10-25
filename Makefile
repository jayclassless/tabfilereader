
setup::
	@poetry install

env::
	@poetry self --version
	@poetry version
	@poetry env info
	@poetry show --all

clean::
	@rm -rf dist .coverage poetry.lock .mypy_cache .pytest_cache

clean-full:: clean
	@poetry env remove `poetry run which python`

test::
	@poetry run coverage run --module py.test
	@poetry run coverage report

lint::
	-@poetry run tidypy check
	-@poetry run mypy src

build::
	@poetry build

publish::
	@poetry publish
