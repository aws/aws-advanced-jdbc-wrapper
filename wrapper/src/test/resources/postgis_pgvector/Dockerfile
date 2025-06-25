FROM postgis/postgis:16-3.4

# Install necessary packages
RUN apt-get update \
    && /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y \
    && apt install -y postgresql-16-pgvector

# Copy initialization scripts
COPY ./docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d/
