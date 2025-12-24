.PHONY: up

up: .init-dirs
	@echo "Checking if Docker is running..."
	@if ! docker info > /dev/null 2>&1; then \
		echo "Docker is not running. Starting Docker Desktop..."; \
		open -a Docker; \
		echo "Waiting for Docker to start..."; \
		while ! docker info > /dev/null 2>&1; do sleep 1; done; \
		echo "Docker started!"; \
	fi
	@echo "Starting up services..."
	docker-compose -f docker/docker-compose.yml up -d

.PHONY: .init-dirs
.init-dirs:
	@echo "Checking for .env file..."
	@if [ ! -f docker/.env ]; then \
		echo "Creating docker/.env from docker/.env.example"; \
		cp docker/.env.example docker/.env; \
	fi
	@echo "Ensuring local data directories exist..."
	@DATA_PATH=$$(grep LOCAL_DATA_PATH docker/.env | cut -d '=' -f2 | sed 's|^~|$(HOME)|'); \
	if [ -z "$$DATA_PATH" ]; then DATA_PATH="$(HOME)/Documents/jimwurst_local_data"; fi; \
	mkdir -p "$$DATA_PATH/linkedin" "$$DATA_PATH/substack" "$$DATA_PATH/apple_health" "$$DATA_PATH/bolt" "$$DATA_PATH/telegram" "$$DATA_PATH/spotify"

.PHONY: setup
setup: .init-dirs
	@echo "Setting up Python virtual environment..."
	@python3 -m venv .venv
	@echo "Upgrading pip..."
	@.venv/bin/pip install --upgrade pip
	@echo "Installing dependencies..."
	@for req in apps/data_ingestion/manual_job/*/requirements.txt; do \
		if [ -f "$$req" ]; then \
			echo "Installing $$req..."; \
			.venv/bin/pip install -r "$$req"; \
		fi \
	done
	@echo "Setup complete. Use 'make ingest-<app>' to run the ingestion scripts."

.PHONY: ingest-apple-health
ingest-apple-health:
	@echo "Running Apple Health ingestion..."
	@.venv/bin/python3 apps/data_ingestion/manual_job/apple_health/ingest.py

.PHONY: ingest-substack
ingest-substack:
	@echo "Running Substack ingestion..."
	@.venv/bin/python3 apps/data_ingestion/manual_job/substack/ingest.py

.PHONY: ingest-linkedin
ingest-linkedin:
	@echo "Running LinkedIn ingestion..."
	@.venv/bin/python3 apps/data_ingestion/manual_job/linkedin/ingest.py

.PHONY: ingest-bolt
ingest-bolt:
	@echo "Running Bolt ingestion..."
	@.venv/bin/python3 apps/data_ingestion/manual_job/bolt/ingest.py

.PHONY: ingest-telegram
ingest-telegram:
	@echo "Running Telegram ingestion..."
	@.venv/bin/python3 apps/data_ingestion/manual_job/telegram/ingest.py

.PHONY: ingest-spotify
ingest-spotify:
	@echo "Running Spotify ingestion..."
	@.venv/bin/python3 apps/data_ingestion/manual_job/spotify/ingest.py

.PHONY: transform-linkedin
transform-linkedin:
	@echo "Running LinkedIn dbt transformations..."
	@cd apps/data_transformation/dbt && ../../../.venv/bin/dbt build --select source:linkedin+ --vars '{"enable_linkedin_models": true}'
