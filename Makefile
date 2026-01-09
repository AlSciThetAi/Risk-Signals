# Makefile = shortcuts so you don't have to retype long docker compose commands

COMPOSE = docker compose -f docker/docker-compose.yml
VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

# Create and set up Python virtual environment
venv:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
# Start services in the background
up:
	$(COMPOSE) up -d

# Stop services and remove containers (keeps volumes/data)
down:
	$(COMPOSE) down

# Stop containers without removing them
stop:
	$(COMPOSE) stop

# Start previously stopped containers
start:
	$(COMPOSE) start

# View container status
ps:
	$(COMPOSE) ps

# Tail Postgres logs
logs:
	docker logs -f risk_postgres

# Open a psql session inside the Postgres container
psql:
	docker exec -it risk_postgres psql -U risk -d risk_signals
# Run all SQL migration scripts in order
db-migrate:
	@set -e; \
	for f in $$(ls -1 scripts/sql/*.sql | sort); do \
		echo "Running $$f"; \
		docker exec -i risk_postgres psql -v ON_ERROR_STOP=1 -U risk -d risk_signals < $$f; \
	done

# Ingest USGS earthquake data into bronze.usgs_earthquakes
ingest-usgs-earthquakes:
	$(PYTHON) -m scripts.ingest_usgs_earthquakes

#Run load ref county script
load-ref-county:
	docker exec -i risk_postgres psql -v ON_ERROR_STOP=1 -U risk -d risk_signals -c "DROP VIEW IF EXISTS ref.ref_county_centroid;"
	$(PYTHON) -m scripts.load_ref_county data/ref/cb_2018_us_county_500k.zip

# Run full ETL process
etl: up db-migrate load-ref-county ingest-usgs-earthquakes db-migrate
	@echo "ETL process complete"

#Check python DB connection
check-db-connection:
	$(PYTHON) -m scripts.db_smoke_test