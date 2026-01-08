# Makefile = shortcuts so you don't have to retype long docker compose commands

COMPOSE = docker compose -f docker/docker-compose.yml

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

#Add centroid data to ref.ref_county
db-migrate:
	docker exec -i risk_postgres psql -U risk -d risk_signals < scripts/sql/002_ref_county_centroids.sql