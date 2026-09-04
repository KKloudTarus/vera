#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(git -C "$script_dir/.." rev-parse --show-toplevel)
output_root=${VERA_EVAL_OUTPUT_ROOT:-"$repo_root/evals/runs"}
case "$output_root" in
    /*) ;;
    *) output_root="$repo_root/$output_root" ;;
esac
runtime_env_file=${VERA_RELEASE_ENV_FILE:-"$repo_root/.env"}
case "$runtime_env_file" in
    /*) ;;
    *) runtime_env_file="$repo_root/$runtime_env_file" ;;
esac
if [ -n "${VERA_RELEASE_ENV_FILE:-}" ] && [ ! -f "$runtime_env_file" ]; then
    printf '%s\n' "release runtime env file does not exist: $runtime_env_file" >&2
    exit 1
fi

if [ "$#" -eq 0 ]; then
    printf '%s\n' "usage: $0 <docker-compose-arguments>" >&2
    exit 2
fi
if [ -n "$(git -C "$repo_root" status --porcelain --untracked-files=all)" ]; then
    printf '%s\n' "release stack requires a clean Git worktree" >&2
    exit 1
fi

: "${COMPOSE_PROJECT_NAME:?set a unique COMPOSE_PROJECT_NAME for this release run}"
: "${VERA_EVAL_SCOPE_ID:?set a unique VERA_EVAL_SCOPE_ID for this release run}"
: "${VERA_RELEASE_APP_IMAGE:?set the candidate application image by immutable digest}"

git_sha=$(git -C "$repo_root" rev-parse --verify HEAD)
if [ "${#git_sha}" -ne 40 ]; then
    printf '%s\n' "release stack could not resolve a full Git SHA" >&2
    exit 1
fi
case "$git_sha" in
    *[!0-9a-f]*)
        printf '%s\n' "release stack could not resolve a full Git SHA" >&2
        exit 1
        ;;
esac

app_image=$VERA_RELEASE_APP_IMAGE
app_image_digest=${app_image##*@}
app_image_digest_hex=${app_image_digest#sha256:}
if [ "$app_image" = "$app_image_digest" ] || [ "$app_image_digest" = "$app_image_digest_hex" ] || [ "${#app_image_digest_hex}" -ne 64 ]; then
    printf '%s\n' "VERA_RELEASE_APP_IMAGE must use repository@sha256:<64 lowercase hex>" >&2
    exit 1
fi
case "$app_image_digest_hex" in
    *[!0-9a-f]*)
        printf '%s\n' "VERA_RELEASE_APP_IMAGE must use repository@sha256:<64 lowercase hex>" >&2
        exit 1
        ;;
esac
if [ "$app_image_digest_hex" = "0000000000000000000000000000000000000000000000000000000000000000" ]; then
    printf '%s\n' "VERA_RELEASE_APP_IMAGE must not use the fail-closed placeholder digest" >&2
    exit 1
fi
database_provision_image="${COMPOSE_PROJECT_NAME}-database-provision:${git_sha}"
prometheus_image="${COMPOSE_PROJECT_NAME}-prometheus:${git_sha}"
recovery_image="${COMPOSE_PROJECT_NAME}-recovery:${git_sha}"
evaluator_image="${COMPOSE_PROJECT_NAME}-evaluator:${git_sha}"

command=$1
shift
case "$command" in
    -*)
        printf '%s\n' "release stack rejects Docker Compose global options" >&2
        exit 2
        ;;
esac
if [ "$command" = "build" ]; then
    if [ "$#" -ne 0 ]; then
        printf '%s\n' "usage: $0 build" >&2
        exit 2
    fi
    archive=$(mktemp "${TMPDIR:-/tmp}/vera-release.XXXXXX.tar")
    trap 'rm -f "$archive"' EXIT HUP INT TERM
    git -C "$repo_root" archive --format=tar --output="$archive" "$git_sha"
    docker pull "$app_image"
    app_build=$(docker run --rm --entrypoint python "$app_image" -c \
        'import json; value=json.load(open("/app/build-metadata.json")); print(f"{value.get('"'"'git_sha'"'"')}:{str(value.get('"'"'git_dirty'"'"')).lower()}")')
    if [ "$app_build" != "$git_sha:false" ]; then
        printf '%s\n' "candidate application image was not built from this clean commit" >&2
        exit 1
    fi
    docker build --file deploy/postgres/Dockerfile \
        --tag "$database_provision_image" - < "$archive"
    docker build --file deploy/observability/Dockerfile \
        --tag "$prometheus_image" - < "$archive"
    docker build --file deploy/recovery/Dockerfile \
        --tag "$recovery_image" - < "$archive"
    docker build \
        --build-arg VERA_BUILD_GIT_SHA="$git_sha" \
        --build-arg VERA_BUILD_GIT_DIRTY=false \
        --file evals/Dockerfile.eval --tag "$evaluator_image" - < "$archive"
    exit 0
fi

for argument in "$command" "$@"; do
    if [ "$argument" = "--build" ] || [ "$argument" = "--no-deps" ]; then
        printf '%s\n' "release stack rejects partial or implicit rebuilds" >&2
        exit 2
    fi
done

if [ "$command" = "up" ]; then
    if [ "$#" -ne 1 ] || { [ "$1" != "-d" ] && [ "$1" != "--detach" ]; }; then
        printf '%s\n' "usage: $0 up -d" >&2
        exit 2
    fi
    set -- --no-build --force-recreate -d
fi

context_root=$(mktemp -d "${TMPDIR:-/tmp}/vera-release-context.XXXXXX")
trap 'rm -rf "$context_root"' EXIT HUP INT TERM
git -C "$repo_root" archive --format=tar "$git_sha" | tar -xf - -C "$context_root"

export VERA_APP_IMAGE="$app_image"
export VERA_RELEASE_APP_IMAGE_DIGEST="$app_image_digest"
export VERA_DB_PROVISION_IMAGE="$database_provision_image"
export VERA_PROMETHEUS_IMAGE="$prometheus_image"
export VERA_RECOVERY_IMAGE="$recovery_image"
export VERA_EVALUATOR_IMAGE="$evaluator_image"
export VERA_EVALUATOR_USER="$(id -u):$(id -g)"
export VERA_BUILD_GIT_SHA="$git_sha"
export VERA_BUILD_GIT_DIRTY=false
export VERA_EVAL_OUTPUT_ROOT="$output_root"
export VERA_RUNTIME_ENV_FILE="$runtime_env_file"

compose() {
    if [ -f "$runtime_env_file" ]; then
        docker compose --env-file "$runtime_env_file" \
            --project-directory "$context_root" \
            -f "$context_root/docker-compose.yml" \
            -f "$context_root/evals/docker-compose.eval.yml" \
            --profile app --profile eval "$@"
    else
        docker compose \
            --project-directory "$context_root" \
            -f "$context_root/docker-compose.yml" \
            -f "$context_root/evals/docker-compose.eval.yml" \
            --profile app --profile eval "$@"
    fi
}

if [ "$command" = "up" ]; then
    mkdir -p "$output_root"
    if [ ! -w "$output_root" ]; then
        printf '%s\n' "release output directory is not writable: $output_root" >&2
        exit 1
    fi
    rm -f "$output_root/stack-attestation.json"
fi
compose "$command" "$@"

if [ "$command" = "up" ]; then
    if ! compose exec -T evaluator test -w /output; then
        printf '%s\n' "evaluator cannot write to the release output directory" >&2
        exit 1
    fi
    app_image_id=$(docker image inspect --format '{{.Id}}' "$app_image")
    evaluator_image_id=$(docker image inspect --format '{{.Id}}' "$evaluator_image")
    database_provision_image_id=$(docker image inspect --format '{{.Id}}' "$database_provision_image")
    prometheus_image_id=$(docker image inspect --format '{{.Id}}' "$prometheus_image")
    recovery_image_id=$(docker image inspect --format '{{.Id}}' "$recovery_image")

    verify_service_image() {
        service=$1
        expected=$2
        container_id=$(compose ps --all -q "$service")
        if [ -z "$container_id" ]; then
            printf '%s\n' "release service is missing: $service" >&2
            exit 1
        fi
        actual=$(docker inspect --format '{{.Image}}' "$container_id")
        if [ "$actual" != "$expected" ]; then
            printf '%s\n' "release service image mismatch: $service" >&2
            exit 1
        fi
    }

    for service in migrate api worker mcp rollout-state-init rollout-controller; do
        verify_service_image "$service" "$app_image_id"
    done
    verify_service_image database-provision "$database_provision_image_id"
    verify_service_image prometheus "$prometheus_image_id"
    verify_service_image recovery-harness "$recovery_image_id"
    for service in evaluator-state-init evaluator; do
        verify_service_image "$service" "$evaluator_image_id"
    done

    attestation="$output_root/stack-attestation.json"
    temporary_attestation="$attestation.tmp"
    printf '%s\n' \
        '{' \
        '  "schema_version": "1.0",' \
        "  \"git_sha\": \"$git_sha\"," \
        "  \"app_image_ref\": \"$app_image\"," \
        "  \"app_image_digest\": \"$app_image_digest\"," \
        "  \"app_image_id\": \"$app_image_id\"," \
        "  \"database_provision_image_id\": \"$database_provision_image_id\"," \
        "  \"prometheus_image_id\": \"$prometheus_image_id\"," \
        "  \"recovery_image_id\": \"$recovery_image_id\"," \
        "  \"evaluator_image_id\": \"$evaluator_image_id\"," \
        '  "verified_services": ["migrate", "api", "worker", "mcp", "database-provision", "prometheus", "recovery-harness", "rollout-state-init", "rollout-controller", "evaluator-state-init", "evaluator"]' \
        '}' > "$temporary_attestation"
    chmod 0644 "$temporary_attestation"
    mv "$temporary_attestation" "$attestation"
fi
