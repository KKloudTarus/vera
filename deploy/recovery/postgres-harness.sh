#!/bin/sh
set -u

root="${VERA_RECOVERY_ROOT:-/state/recovery}"
mkdir -p "$root"
# The evaluator and recovery image use different users but exchange atomic request files here.
chmod 1777 "$root"

backup() {
    token="$1"
    directory="$root/$token"
    dump="$directory/vera.dump"
    clone="vera_restore_$token"
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
    test -s "$dump" || return
    psql --dbname=postgres --set ON_ERROR_STOP=1 --command \
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='vera' AND pid <> pg_backend_pid()" || return
    dropdb --if-exists --force vera || return
    createdb --owner=vera vera || return
    pg_restore --exit-on-error --no-owner --dbname=vera "$dump" || return
    set -- $(sha256sum "$dump")
    printf '%s\n' "$1"
}

while true; do
    for request in "$root"/*.request; do
        test -e "$request" || break
        name=$(basename "$request" .request)
        command=${name%%.*}
        token=${name#*.}
        case "$command" in
            backup|restore) ;;
            *)
                printf 'invalid recovery command\n' >"$root/$name.failed"
                rm -f "$request"
                continue
                ;;
        esac
        case "$token" in
            *[!a-f0-9]*|'')
                printf 'invalid recovery token\n' >"$root/$name.failed"
                rm -f "$request"
                continue
                ;;
        esac
        rm -f "$root/$name.done" "$root/$name.failed"
        if output=$($command "$token" 2>&1); then
            printf '%s\n' "$output" >"$root/$name.done.tmp"
            mv "$root/$name.done.tmp" "$root/$name.done"
        else
            printf '%s\n' "$output" >"$root/$name.failed.tmp"
            mv "$root/$name.failed.tmp" "$root/$name.failed"
        fi
        rm -f "$request"
    done
    sleep 1
done
