#!/bin/sh
set -u

root="${VERA_RECOVERY_ROOT:-/state/recovery}"
mkdir -p "$root"
# The evaluator and recovery image use different users but exchange atomic request files here.
chmod 1777 "$root"

purge() {
    token="$1"
    dropdb --if-exists --force "vera_restore_$token" >/dev/null || return
    dropdb --if-exists --force "vera_restore_stage_$token" >/dev/null || return
    dropdb --if-exists --force "vera_restore_previous_$token" >/dev/null || return
    rm -rf "$root/$token"
    printf '%s\n' "$token"
}

purge_all() {
    databases=$(psql --dbname=postgres --quiet --tuples-only --no-align --set ON_ERROR_STOP=1 \
        --command "SELECT datname FROM pg_database WHERE datname ~ '^vera_restore_((stage|previous)_)?[a-f0-9]+$'") || return
    for database in $databases; do
        dropdb --if-exists --force "$database" >/dev/null || return
    done
    for directory in "$root"/[a-f0-9]*; do
        test -d "$directory" || continue
        token=$(basename "$directory")
        case "$token" in
            *[!a-f0-9]*|'') continue ;;
        esac
        rm -rf "$directory" || return
    done
}

backup() {
    token="$1"
    directory="$root/$token"
    dump="$directory/vera.dump"
    clone="vera_restore_$token"
    purge "$token" >/dev/null || return
    mkdir -p "$directory"
    rm -f "$dump"
    pg_dump --format=custom --no-owner --file="$dump" vera || return
    dropdb --if-exists --force "$clone" || return
    createdb --owner=vera "$clone" || return
    pg_restore --exit-on-error --no-owner --dbname="$clone" "$dump" || return
    set -- $(sha256sum "$dump")
    printf '%s %s\n' "$clone" "$1"
}

restore() {
    token="$1"
    dump="$root/$token/vera.dump"
    staging="vera_restore_stage_$token"
    previous="vera_restore_previous_$token"
    staging_marker="$root/$token/staging.sha256"
    test -s "$dump" || return

    database_exists() {
        test "$(psql --dbname=postgres --quiet --tuples-only --no-align --set ON_ERROR_STOP=1 \
            --command "SELECT count(*) FROM pg_database WHERE datname='$1'")" = "1"
    }

    staging_ready() {
        test -s "$staging_marker" || return 1
        read -r expected_sha <"$staging_marker" || return 1
        set -- $(sha256sum "$dump")
        test "$expected_sha" = "$1"
    }

    restore_access() {
        attempt=0
        while test "$attempt" -lt 3; do
            attempt=$((attempt + 1))
            if database_exists vera; then
                psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
                    "ALTER DATABASE vera WITH ALLOW_CONNECTIONS true" >/dev/null 2>&1 || true
            elif database_exists "$staging" && staging_ready; then
                psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
                    "ALTER DATABASE $staging RENAME TO vera; ALTER DATABASE vera WITH ALLOW_CONNECTIONS true" \
                    >/dev/null 2>&1 || true
            fi
            if ! database_exists vera && database_exists "$previous"; then
                psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
                    "ALTER DATABASE $previous RENAME TO vera; ALTER DATABASE vera WITH ALLOW_CONNECTIONS true" \
                    >/dev/null 2>&1 || true
            fi
            if database_exists vera && psql --dbname=vera --set ON_ERROR_STOP=1 \
                --command "SELECT 1" >/dev/null 2>&1; then
                return 0
            fi
            sleep 1
        done
        return 1
    }

    fail_cutover() {
        if ! restore_access; then
            printf '%s\n' "restore cutover failed and database access could not be restored" >&2
        fi
        trap - HUP INT TERM
        return 1
    }
    trap 'restore_access >/dev/null 2>&1 || true; exit 1' HUP INT TERM

    if ! database_exists vera; then
        if database_exists "$staging" && staging_ready; then
            psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
                "ALTER DATABASE $staging RENAME TO vera; ALTER DATABASE vera WITH ALLOW_CONNECTIONS true" \
                || { fail_cutover; return; }
            set -- $(sha256sum "$dump")
            printf '%s\n' "$1"
            trap - HUP INT TERM
            return
        fi
        if database_exists "$previous"; then
            psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
                "ALTER DATABASE $previous RENAME TO vera; ALTER DATABASE vera WITH ALLOW_CONNECTIONS true" \
                || { fail_cutover; return; }
        else
            return 1
        fi
    elif database_exists "$previous"; then
        # Promotion completed but status publication was interrupted. Keep the previous
        # database until the evaluator verifies the restored state and requests purge.
        set -- $(sha256sum "$dump")
        printf '%s\n' "$1"
        if ! restore_access; then
            trap - HUP INT TERM
            return 1
        fi
        trap - HUP INT TERM
        return
    fi

    rm -f "$staging_marker" "$staging_marker.tmp"
    dropdb --if-exists --force "$staging" || return
    createdb --owner=vera "$staging" || return
    pg_restore --exit-on-error --no-owner --dbname="$staging" "$dump" || return
    set -- $(sha256sum "$dump")
    printf '%s\n' "$1" >"$staging_marker.tmp" || return
    mv "$staging_marker.tmp" "$staging_marker" || return
    if ! psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
        "ALTER DATABASE vera WITH ALLOW_CONNECTIONS false"; then
        fail_cutover
        return
    fi
    if ! psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='vera' AND pid <> pg_backend_pid()"; then
        fail_cutover
        return
    fi
    if ! psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
        "ALTER DATABASE vera RENAME TO $previous"; then
        fail_cutover
        return
    fi
    if ! psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
        "ALTER DATABASE $staging RENAME TO vera"; then
        fail_cutover
        return
    fi
    if ! restore_access; then
        fail_cutover
        return
    fi
    set -- $(sha256sum "$dump")
    printf '%s\n' "$1"
    trap - HUP INT TERM
}

cleanup() {
    result=$(psql --dbname=vera --quiet --tuples-only --no-align --set ON_ERROR_STOP=1 <<'SQL'
BEGIN;
SELECT string_agg(format('%I', c.relname), ', ' ORDER BY c.relname) AS mutable_tables
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind IN ('r', 'p')
  AND NOT c.relispartition
  AND c.relname NOT IN ('alembic_version', 'ontology_versions')
\gset
LOCK TABLE :mutable_tables IN ACCESS EXCLUSIVE MODE;
SELECT json_build_object(
    'tables', (
        SELECT json_agg(c.relname ORDER BY c.relname)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND NOT c.relispartition
          AND c.relname NOT IN ('alembic_version', 'ontology_versions')
    ),
    'cost_usd', (SELECT COALESCE(sum(cost_usd), 0) FROM llm_usage),
    'cost_complete', (SELECT COALESCE(bool_and(cost_complete), true) FROM llm_usage)
)::text AS cleanup_result
\gset
TRUNCATE TABLE :mutable_tables RESTART IDENTITY CASCADE;
COMMIT;
\echo :cleanup_result
SQL
    ) || return
    purge_all || return
    printf '%s\n' "$result"
}

run_command() {
    command="$1"
    token="$2"
    case "$command" in
        backup|restore|cleanup|purge) ;;
        *) return 2 ;;
    esac
    case "$token" in
        *[!a-f0-9]*|'') return 2 ;;
    esac
    "$command" "$token"
}

if [ "${1:-}" = "run" ]; then
    test "$#" -eq 3 || exit 2
    run_command "$2" "$3"
    exit
fi
if [ "${1:-}" = "purge-all" ]; then
    test "$#" -eq 1 || exit 2
    purge_all
    exit
fi

command_timeout="${VERA_RECOVERY_COMMAND_TIMEOUT_S:-240}"
case "$command_timeout" in
    *[!0-9]*|' '|''|0) printf 'invalid recovery command timeout\n' >&2; exit 2 ;;
esac

# A SIGKILL can bypass restore's trap after connections are disabled. Re-enable an
# existing active database before replaying the idempotent claimed command.
startup_attempt=0
while ! active_database=$(psql --dbname=postgres --quiet --tuples-only --no-align \
    --set ON_ERROR_STOP=1 --command "SELECT count(*) FROM pg_database WHERE datname='vera'" \
    2>/dev/null); do
    startup_attempt=$((startup_attempt + 1))
    test "$startup_attempt" -lt 30 || exit 1
    sleep 1
done
if test "$active_database" = "1"; then
    psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
        "ALTER DATABASE vera WITH ALLOW_CONNECTIONS true" >/dev/null || exit
fi

# Commands are idempotent by token. Requeue work claimed before a harness restart.
for running in "$root"/*/running; do
    test -e "$running" || break
    exchange=${running%/running}
    if test -e "$exchange/done" || test -e "$exchange/failed"; then
        rm -f "$running"
    else
        mv "$running" "$exchange/request" || true
    fi
done

while true; do
    for request in "$root"/*/request; do
        test -e "$request" || break
        exchange=${request%/request}
        name=$(basename "$exchange")
        command=${name%%.*}
        remainder=${name#*.}
        token=${remainder%%.*}
        nonce=${remainder#*.}
        running="$exchange/running"
        mv "$request" "$running" || continue
        case "$command" in
            backup|restore|cleanup|purge) ;;
            *)
                rm -f "$running"
                printf 'invalid recovery command\n' >"$exchange/failed.tmp"
                mv "$exchange/failed.tmp" "$exchange/failed"
                continue
                ;;
        esac
        case "$token" in
            *[!a-f0-9]*|'')
                rm -f "$running"
                printf 'invalid recovery token\n' >"$exchange/failed.tmp"
                mv "$exchange/failed.tmp" "$exchange/failed"
                continue
                ;;
        esac
        case "$nonce" in
            *[!a-f0-9]*|'')
                rm -f "$running"
                printf 'invalid recovery nonce\n' >"$exchange/failed.tmp"
                mv "$exchange/failed.tmp" "$exchange/failed"
                continue
                ;;
        esac
        if output=$(timeout --kill-after=5 "$command_timeout" sh "$0" run "$command" "$token" 2>&1); then
            rm -f "$running"
            printf '%s\n' "$output" >"$exchange/done.tmp"
            mv "$exchange/done.tmp" "$exchange/done"
        else
            status=$?
            case "$command" in
                backup|cleanup|purge)
                    timeout 30 sh "$0" purge-all >/dev/null 2>&1 || true
                    ;;
            esac
            rm -f "$running"
            printf 'exit_status=%s\n%s\n' "$status" "$output" >"$exchange/failed.tmp"
            mv "$exchange/failed.tmp" "$exchange/failed"
        fi
    done
    sleep 1
done
