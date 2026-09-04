{{- define "vera.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "vera.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "vera.labels" -}}
app.kubernetes.io/name: {{ include "vera.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/* Component service names, so env references stay in one place. */}}
{{- define "vera.postgresHost" -}}{{ include "vera.fullname" . }}-postgres{{- end -}}
{{- define "vera.valkeyHost" -}}{{ include "vera.fullname" . }}-valkey{{- end -}}
{{- define "vera.minioHost" -}}{{ include "vera.fullname" . }}-minio{{- end -}}
{{- define "vera.falkordbHost" -}}{{ include "vera.fullname" . }}-falkordb{{- end -}}
{{- define "vera.neo4jHost" -}}{{ include "vera.fullname" . }}-neo4j{{- end -}}
{{- define "vera.apiName" -}}{{ include "vera.fullname" . }}-api{{- end -}}
{{- define "vera.mcpName" -}}{{ include "vera.fullname" . }}-mcp{{- end -}}
{{- define "vera.workerName" -}}{{ include "vera.fullname" . }}-worker{{- end -}}

{{/* Database DSNs are split so runtime pods never receive worker or schema-owner credentials. */}}
{{- define "vera.adminDsn" -}}
postgresql+asyncpg://{{ .Values.postgres.user }}:{{ .Values.postgres.password | urlquery | replace "+" "%20" }}@{{ include "vera.postgresHost" . }}:5432/{{ .Values.postgres.database }}
{{- end -}}

{{- define "vera.runtimeDsn" -}}
postgresql+asyncpg://vera_runtime:{{ .Values.postgres.runtimePassword | urlquery | replace "+" "%20" }}@{{ include "vera.postgresHost" . }}:5432/{{ .Values.postgres.database }}
{{- end -}}

{{- define "vera.workerDsn" -}}
postgresql+asyncpg://vera_worker_runtime:{{ .Values.postgres.workerPassword | urlquery | replace "+" "%20" }}@{{ include "vera.postgresHost" . }}:5432/{{ .Values.postgres.database }}
{{- end -}}

{{- define "vera.image" -}}
{{- $digest := required "image.digest must be an immutable sha256 digest" .Values.image.digest -}}
{{- if not (regexMatch "^sha256:[a-f0-9]{64}$" $digest) -}}
{{- fail "image.digest must be an immutable sha256 digest" -}}
{{- end -}}
{{- if eq $digest "sha256:0000000000000000000000000000000000000000000000000000000000000000" -}}
{{- fail "image.digest must replace the fail-closed placeholder" -}}
{{- end -}}
{{ .Values.image.repository }}@{{ $digest }}
{{- end -}}

{{/* App pods run as the image's non-root vera user. kubelet needs the numeric UID to
     confirm non-root, since the image's USER is a name. */}}
{{- define "vera.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: {{ .Values.image.runAsUser }}
{{- end -}}

{{/* Non-secret configuration is common; credentials are injected per process below. */}}
{{- define "vera.envFrom" -}}
- configMapRef:
    name: {{ include "vera.fullname" . }}-config
{{- end -}}

{{- define "vera.applicationSecretEnv" -}}
- name: VERA_OBJECTSTORE__ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-secrets
      key: VERA_OBJECTSTORE__ACCESS_KEY
- name: VERA_OBJECTSTORE__SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-secrets
      key: VERA_OBJECTSTORE__SECRET_KEY
{{- if eq .Values.graph.backend "neo4j" }}
- name: VERA_NEO4J__PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-secrets
      key: VERA_NEO4J__PASSWORD
{{- end }}
- name: VERA_MEMORY__OPENAI_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-secrets
      key: VERA_MEMORY__OPENAI_API_KEY
      optional: true
- name: VERA_VOYAGE__API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-secrets
      key: VERA_VOYAGE__API_KEY
      optional: true
{{- end -}}

{{- define "vera.mcpSecretEnv" -}}
- name: VERA_MCP__JWT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-secrets
      key: VERA_MCP__JWT_SECRET
      optional: true
{{- end -}}

{{- define "vera.bootstrapSecretEnv" -}}
- name: VERA_BOOTSTRAP__ADMIN_API_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-secrets
      key: VERA_BOOTSTRAP__ADMIN_API_KEY
      optional: true
{{- end -}}

{{- define "vera.runtimeDatabaseEnv" -}}
- name: VERA_DB__DSN
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-database-runtime
      key: VERA_DB__DSN
{{- end -}}

{{- define "vera.workerDatabaseEnv" -}}
- name: VERA_DB__DSN
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-database-worker
      key: VERA_DB__DSN
{{- end -}}

{{- define "vera.adminDatabaseEnv" -}}
- name: VERA_DB__DSN
  valueFrom:
    secretKeyRef:
      name: {{ include "vera.fullname" . }}-database-admin
      key: VERA_DB__DSN
{{- end -}}

{{- define "vera.schemaRevision" -}}d4e5f6a7b8c9{{- end -}}

{{/* Hold app pods until the database accepts connections. Runs non-root so it satisfies
     the app pods' runAsNonRoot policy (pg_isready needs no privileges). */}}
{{- define "vera.waitForPostgres" -}}
- name: wait-for-postgres
  image: {{ .Values.postgres.image }}
  securityContext:
    runAsNonRoot: true
    runAsUser: 65534
    allowPrivilegeEscalation: false
  command:
    - sh
    - -c
    - until pg_isready -h {{ include "vera.postgresHost" . }} -U {{ .Values.postgres.user }}; do echo "waiting for postgres"; sleep 2; done
{{- end -}}

{{/* Fresh-install Jobs are ordinary resources and may finish after Deployments are created.
     Connect through the workload's own login and wait for both the exact schema revision and
     runtime-login provisioning before application code starts. */}}
{{- define "vera.waitForRuntimeDatabase" -}}
{{- $root := .root -}}
- name: wait-for-runtime-database
  image: {{ include "vera.image" $root }}
  imagePullPolicy: {{ $root.Values.image.pullPolicy }}
  securityContext:
    runAsNonRoot: true
    runAsUser: {{ $root.Values.image.runAsUser }}
    allowPrivilegeEscalation: false
  env:
    - name: VERA_DB__DSN
      valueFrom:
        secretKeyRef:
          name: {{ include "vera.fullname" $root }}-database-{{ .database }}
          key: VERA_DB__DSN
    - name: VERA_DATABASE_ROLE
      value: {{ .role | quote }}
    - name: VERA_SCHEMA_REVISION
      value: {{ include "vera.schemaRevision" $root | quote }}
    - name: VERA_DEPLOYMENT_NAME
      value: {{ include "vera.fullname" $root | quote }}
    - name: VERA_RELEASE_REVISION
      value: {{ $root.Release.Revision | quote }}
  command: ["python", "-c"]
  args:
    - |
      import asyncio
      import os
      from sqlalchemy import text
      from sqlalchemy.ext.asyncio import create_async_engine
      from sqlalchemy.pool import NullPool

      async def main():
          role = os.environ["VERA_DATABASE_ROLE"]
          if role not in {"vera_app", "vera_worker"}:
              raise RuntimeError("invalid database readiness role")
          engine = create_async_engine(
              os.environ["VERA_DB__DSN"],
              poolclass=NullPool,
              connect_args={"timeout": 5},
          )
          try:
              while True:
                  try:
                      async with asyncio.timeout(10):
                          async with engine.begin() as connection:
                              await connection.execute(text(f"SET ROLE {role}"))
                              row = (
                                  await connection.execute(
                                      text(
                                          "SELECT version_num, "
                                          "to_regrole('vera_runtime') IS NOT NULL AND "
                                          "to_regrole('vera_worker_runtime') IS NOT NULL, "
                                          "current_setting('vera.provisioned_release', true), "
                                          "current_setting('vera.provisioned_revision', true) "
                                          "FROM alembic_version"
                                      )
                                  )
                              ).one()
                          if row == (
                              os.environ["VERA_SCHEMA_REVISION"],
                              True,
                              os.environ["VERA_DEPLOYMENT_NAME"],
                              os.environ["VERA_RELEASE_REVISION"],
                          ):
                              return
                  except Exception:
                      pass
                  print("waiting for database migration and runtime roles", flush=True)
                  await asyncio.sleep(2)
          finally:
              await engine.dispose()

      asyncio.run(main())
{{- end -}}
