.PHONY: install seed run test clean

install:
	pip install -r requirements.txt

seed:
	python -m scripts.generate_sample_data

run:
	python -m src.jobs.ingest_lighthouse
	python -m src.jobs.ingest_crux
	python -m src.jobs.ingest_page_logs
	python -m src.jobs.build_core_web_vitals_tables
	python -m src.jobs.detect_regressions
	python -m src.jobs.generate_reports

test:
	pytest -v

clean:
	rm -rf data/processed data/curated reports/*.csv reports/*.md
	rm -rf .pytest_cache spark-warehouse metastore_db derby.log
	find . -name "__pycache__" -type d -exec rm -rf {} +
