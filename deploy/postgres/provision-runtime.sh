#!/bin/sh
set -eu

: "${VERA_RUNTIME_PASSWORD:?VERA_RUNTIME_PASSWORD is required}"
: "${VERA_WORKER_PASSWORD:?VERA_WORKER_PASSWORD is required}"

psql \
  --host postgres \
  --username vera \
  --dbname vera \
  --set ON_ERROR_STOP=1 \
  --set runtime_password="$VERA_RUNTIME_PASSWORD" \
  --set worker_password="$VERA_WORKER_PASSWORD" <<'SQL'
BEGIN;
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vera_runtime') THEN
        CREATE ROLE vera_runtime NOLOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vera_worker_runtime') THEN
        CREATE ROLE vera_worker_runtime NOLOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS;
    END IF;
END
$$;

DO $$
DECLARE role_oid oid := (SELECT oid FROM pg_roles WHERE rolname = 'vera_runtime');
BEGIN
    IF EXISTS (
        SELECT FROM pg_shdepend
        WHERE refclassid = 'pg_authid'::regclass AND refobjid = role_oid
          AND deptype = 'o'
    ) THEN
        RAISE EXCEPTION 'vera_runtime owns database objects';
    END IF;
END
$$;
ALTER ROLE vera_runtime
    LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    PASSWORD :'runtime_password';
REVOKE ALL PRIVILEGES ON DATABASE vera FROM vera_runtime;
REVOKE ALL ON SCHEMA public FROM vera_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vera_runtime;
SELECT format('REVOKE ALL PRIVILEGES (%I) ON TABLE %I.%I FROM vera_runtime',
              attribute.attname, namespace.nspname, relation.relname)
FROM pg_attribute attribute
JOIN pg_class relation ON relation.oid = attribute.attrelid
JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public' AND attribute.attnum > 0
  AND NOT attribute.attisdropped AND attribute.attacl IS NOT NULL \gexec
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM vera_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM vera_runtime;
GRANT CONNECT ON DATABASE vera TO vera_runtime;
SELECT format('REVOKE %I FROM vera_runtime CASCADE', granted.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE member.rolname = 'vera_runtime' \gexec
SELECT format('REVOKE vera_runtime FROM %I CASCADE', member.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE granted.rolname = 'vera_runtime' \gexec

DO $$
DECLARE role_oid oid := (SELECT oid FROM pg_roles WHERE rolname = 'vera_worker_runtime');
BEGIN
    IF EXISTS (
        SELECT FROM pg_shdepend
        WHERE refclassid = 'pg_authid'::regclass AND refobjid = role_oid
          AND deptype = 'o'
    ) THEN
        RAISE EXCEPTION 'vera_worker_runtime owns database objects';
    END IF;
END
$$;
ALTER ROLE vera_worker_runtime
    LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    PASSWORD :'worker_password';
REVOKE ALL PRIVILEGES ON DATABASE vera FROM vera_worker_runtime;
REVOKE ALL ON SCHEMA public FROM vera_worker_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vera_worker_runtime;
SELECT format('REVOKE ALL PRIVILEGES (%I) ON TABLE %I.%I FROM vera_worker_runtime',
              attribute.attname, namespace.nspname, relation.relname)
FROM pg_attribute attribute
JOIN pg_class relation ON relation.oid = attribute.attrelid
JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public' AND attribute.attnum > 0
  AND NOT attribute.attisdropped AND attribute.attacl IS NOT NULL \gexec
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM vera_worker_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM vera_worker_runtime;
GRANT CONNECT ON DATABASE vera TO vera_worker_runtime;
SELECT format('REVOKE %I FROM vera_worker_runtime CASCADE', granted.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE member.rolname = 'vera_worker_runtime' \gexec
SELECT format('REVOKE vera_worker_runtime FROM %I CASCADE', member.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE granted.rolname = 'vera_worker_runtime' \gexec

SELECT format('REVOKE vera_app FROM %I CASCADE', member.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE granted.rolname = 'vera_app' \gexec
SELECT format('REVOKE vera_trusted FROM %I CASCADE', member.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE granted.rolname = 'vera_trusted' \gexec
SELECT format('REVOKE vera_worker FROM %I CASCADE', member.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE granted.rolname = 'vera_worker' \gexec
GRANT vera_app, vera_trusted TO vera_runtime;
GRANT vera_app, vera_trusted, vera_worker TO vera_worker_runtime;
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'vera_legacy') THEN
        GRANT vera_app TO vera_legacy;
    END IF;
END
$$;
COMMIT;
SQL

scaler_enabled=false
if [ -n "${VERA_SCALER_PASSWORD:-}" ]; then
    scaler_enabled=true
fi
psql \
  --host postgres \
  --username vera \
  --dbname vera \
  --set ON_ERROR_STOP=1 \
  --set scaler_enabled="$scaler_enabled" \
  --set scaler_password="${VERA_SCALER_PASSWORD:-}" <<'SQL'
BEGIN;
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vera_scaler_runtime') THEN
        CREATE ROLE vera_scaler_runtime NOLOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS;
    END IF;
END
$$;

DO $$
DECLARE role_oid oid := (SELECT oid FROM pg_roles WHERE rolname = 'vera_scaler_runtime');
BEGIN
    IF EXISTS (
        SELECT FROM pg_shdepend
        WHERE refclassid = 'pg_authid'::regclass AND refobjid = role_oid
          AND deptype = 'o'
    ) THEN
        RAISE EXCEPTION 'vera_scaler_runtime owns database objects';
    END IF;
END
$$;
\if :scaler_enabled
    ALTER ROLE vera_scaler_runtime
        LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
        PASSWORD :'scaler_password';
\else
    ALTER ROLE vera_scaler_runtime
        NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
\endif
SELECT format('REVOKE %I FROM vera_scaler_runtime CASCADE', granted.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE member.rolname = 'vera_scaler_runtime' \gexec
SELECT format('REVOKE vera_scaler_runtime FROM %I CASCADE', member.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE granted.rolname = 'vera_scaler_runtime' \gexec
REVOKE ALL PRIVILEGES ON DATABASE vera FROM vera_scaler_runtime;
REVOKE ALL ON SCHEMA public FROM vera_scaler_runtime;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vera_scaler_runtime;
SELECT format('REVOKE ALL PRIVILEGES (%I) ON TABLE %I.%I FROM vera_scaler_runtime',
              attribute.attname, namespace.nspname, relation.relname)
FROM pg_attribute attribute
JOIN pg_class relation ON relation.oid = attribute.attrelid
JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public' AND attribute.attnum > 0
  AND NOT attribute.attisdropped AND attribute.attacl IS NOT NULL \gexec
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM vera_scaler_runtime;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM vera_scaler_runtime;
\if :scaler_enabled
    GRANT CONNECT ON DATABASE vera TO vera_scaler_runtime;
    GRANT USAGE ON SCHEMA public TO vera_scaler_runtime;
    GRANT SELECT ON ingestion_jobs TO vera_scaler_runtime;
\endif
COMMIT;
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
BEGIN;
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'vera_legacy') THEN
        CREATE ROLE vera_legacy NOLOGIN NOINHERIT NOSUPERUSER BYPASSRLS;
    END IF;
END
$$;

DO $$
DECLARE role_oid oid := (SELECT oid FROM pg_roles WHERE rolname = 'vera_legacy');
BEGIN
    IF EXISTS (
        SELECT FROM pg_shdepend
        WHERE refclassid = 'pg_authid'::regclass AND refobjid = role_oid
          AND deptype = 'o'
    ) THEN
        RAISE EXCEPTION 'vera_legacy owns database objects';
    END IF;
END
$$;
ALTER ROLE vera_legacy
    LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION BYPASSRLS
    PASSWORD :'legacy_password';
SELECT format('REVOKE %I FROM vera_legacy CASCADE', granted.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE member.rolname = 'vera_legacy' \gexec
SELECT format('REVOKE vera_legacy FROM %I CASCADE', member.rolname)
FROM pg_auth_members membership
JOIN pg_roles granted ON granted.oid = membership.roleid
JOIN pg_roles member ON member.oid = membership.member
WHERE granted.rolname = 'vera_legacy' \gexec
REVOKE ALL PRIVILEGES ON DATABASE vera FROM vera_legacy;
REVOKE ALL ON SCHEMA public FROM vera_legacy;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM vera_legacy;
SELECT format('REVOKE ALL PRIVILEGES (%I) ON TABLE %I.%I FROM vera_legacy',
              attribute.attname, namespace.nspname, relation.relname)
FROM pg_attribute attribute
JOIN pg_class relation ON relation.oid = attribute.attrelid
JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
WHERE namespace.nspname = 'public' AND attribute.attnum > 0
  AND NOT attribute.attisdropped AND attribute.attacl IS NOT NULL \gexec
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM vera_legacy;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA public FROM vera_legacy;
GRANT vera_app TO vera_legacy;
GRANT CONNECT ON DATABASE vera TO vera_legacy;
GRANT USAGE ON SCHEMA public TO vera_legacy;
SELECT format('GRANT %s ON TABLE %I.%I TO vera_legacy',
              string_agg(candidate.privilege, ', ' ORDER BY candidate.privilege),
              namespace.nspname, relation.relname)
FROM pg_class relation
JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
CROSS JOIN (VALUES ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE')) candidate(privilege)
WHERE namespace.nspname = 'public' AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND has_table_privilege('vera_app', relation.oid, candidate.privilege)
GROUP BY namespace.nspname, relation.relname
\gexec
SELECT format('GRANT %s ON SEQUENCE %I.%I TO vera_legacy',
              string_agg(candidate.privilege, ', ' ORDER BY candidate.privilege),
              namespace.nspname, relation.relname)
FROM pg_class relation
JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
CROSS JOIN (VALUES ('SELECT'), ('USAGE'), ('UPDATE')) candidate(privilege)
WHERE namespace.nspname = 'public' AND relation.relkind = 'S'
  AND has_sequence_privilege('vera_app', relation.oid, candidate.privilege)
GROUP BY namespace.nspname, relation.relname
\gexec
COMMIT;
SQL
