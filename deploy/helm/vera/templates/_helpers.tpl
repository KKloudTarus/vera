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

{{/* The DSN carries a password, so it lives in the Secret. */}}
{{- define "vera.dsn" -}}
postgresql+asyncpg://{{ .Values.postgres.user }}:{{ .Values.postgres.password }}@{{ include "vera.postgresHost" . }}:5432/{{ .Values.postgres.database }}
{{- end -}}

{{- define "vera.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{/* App containers read all VERA_* from the ConfigMap and Secret. */}}
{{- define "vera.envFrom" -}}
- configMapRef:
    name: {{ include "vera.fullname" . }}-config
- secretRef:
    name: {{ include "vera.fullname" . }}-secrets
{{- end -}}

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
