.PHONY: install install-dev start stop producer processor dashboard dashboard-cumulative test lint

install:
	python -m pip install -e '.[spark]'

install-dev:
	python -m pip install -e '.[spark,dev]'

start:
	./scripts/start_cluster.sh

stop:
	./scripts/stop_cluster.sh

producer:
	cosmic-producer --reset-topics

processor:
	cosmic-processor

dashboard:
	cosmic-dashboard --mode single

dashboard-cumulative:
	cosmic-dashboard --mode cumulative

test:
	pytest -q

lint:
	ruff check src tests
