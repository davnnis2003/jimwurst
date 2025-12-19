.PHONY: up

up:
	@echo "Checking if Docker is running..."
	@if ! docker info > /dev/null 2>&1; then \
		echo "Docker is not running. Starting Docker Desktop..."; \
		open -a Docker; \
		echo "Waiting for Docker to start..."; \
		while ! docker info > /dev/null 2>&1; do sleep 1; done; \
		echo "Docker started!"; \
	fi
	@echo "Checking for .env file..."
	@if [ ! -f docker/.env ]; then \
		echo "Creating docker/.env from docker/.env.example"; \
		cp docker/.env.example docker/.env; \
	fi
	@echo "Starting up services..."
	docker-compose -f docker/docker-compose.yml up -d
