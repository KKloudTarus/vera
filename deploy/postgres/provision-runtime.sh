#!/bin/sh
set -eu

: "${VERA_RUNTIME_PASSWORD:?VERA_RUNTIME_PASSWORD is required}"

psql \
  --host postgres \
  --username vera \
  --dbname vera \
  --set ON_ERROR_STOP=1 \
  --set runtime_password="$VERA_RUNTIME_PASSWORD" <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vera_runtime') THEN
        CREATE ROLE vera_runtime NOLOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS;
    END IF;
END
$$;

ALTER ROLE vera_runtime
    LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    PASSWORD :'runtime_password';
GRANT vera_app, vera_trusted, vera_worker TO vera_runtime;
SQL

if [ -z "${VERA_LEGACY_PASSWORD:-}" ]; then
    exit 0
fi

psql \
  --host postgres \
  --username vera \
  --dbname vera \
  --set ON_ERROR_STOP=1 \
  --set legacy_password="$VERA_LEGACY_PASSWORD" <<'SQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vera_legacy') THEN
        CREATE ROLE vera_legacy NOLOGIN NOINHERIT NOSUPERUSER BYPASSRLS;
    END IF;
END
$$;

ALTER ROLE vera_legacy
    LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION BYPASSRLS
    PASSWORD :'legacy_password';
GRANT vera_app TO vera_legacy;
GRANT USAGE ON SCHEMA public TO vera_legacy;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO vera_legacy;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO vera_legacy;
SQL
