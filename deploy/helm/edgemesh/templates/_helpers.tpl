{{/* Common naming and label helpers. */}}

{{- define "edgemesh.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "edgemesh.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "edgemesh.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "edgemesh.labels" -}}
helm.sh/chart: {{ include "edgemesh.chart" . }}
{{ include "edgemesh.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: edgemesh
{{- end -}}

{{- define "edgemesh.selectorLabels" -}}
app.kubernetes.io/name: {{ include "edgemesh.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "edgemesh.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "edgemesh.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Fully-qualified image references. */}}
{{- define "edgemesh.image" -}}
{{- $registry := .root.Values.image.registry -}}
{{- $tag := default .root.Chart.AppVersion .spec.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .spec.repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .spec.repository $tag -}}
{{- end -}}
{{- end -}}

{{/* Headless service name giving control pods stable DNS for Raft. */}}
{{- define "edgemesh.controlHeadless" -}}
{{- printf "%s-control-headless" (include "edgemesh.fullname" .) -}}
{{- end -}}

{{- define "edgemesh.controlFullname" -}}
{{- printf "%s-control" (include "edgemesh.fullname" .) -}}
{{- end -}}

{{- define "edgemesh.edgeFullname" -}}
{{- printf "%s-edge" (include "edgemesh.fullname" .) -}}
{{- end -}}

{{- define "edgemesh.originFullname" -}}
{{- printf "%s-origin" (include "edgemesh.fullname" .) -}}
{{- end -}}

{{/*
Raft peer list.

Each control pod gets stable DNS through the headless service, which is what
lets Raft addresses be computed rather than discovered. Membership is static in
V1, so the list is fixed at install time.
*/}}
{{- define "edgemesh.raftPeers" -}}
{{- $ctl := include "edgemesh.controlFullname" . -}}
{{- $svc := include "edgemesh.controlHeadless" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := .Values.control.ports.raft -}}
{{- range $i := until (int .Values.control.replicaCount) }}
    - id: {{ printf "%s-%d" $ctl $i }}
      address: {{ printf "%s-%d.%s.%s.svc.cluster.local:%d" $ctl $i $svc $ns (int $port) }}
{{- end }}
{{- end -}}

{{/* Edge-facing control endpoints. */}}
{{- define "edgemesh.controlEndpoints" -}}
{{- $ctl := include "edgemesh.controlFullname" . -}}
{{- $svc := include "edgemesh.controlHeadless" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := .Values.control.ports.edge -}}
{{- range $i := until (int .Values.control.replicaCount) }}
    - {{ printf "%s-%d.%s.%s.svc.cluster.local:%d" $ctl $i $svc $ns (int $port) }}
{{- end }}
{{- end -}}

{{/* Shared security stanza rendered into both node configurations. */}}
{{- define "edgemesh.securityConfig" -}}
security:
{{- if .Values.security.mtls.enabled }}
  enabled: true
  ca_file: /etc/edgemesh/tls/ca.crt
  cert_file: /etc/edgemesh/tls/tls.crt
  key_file: /etc/edgemesh/tls/tls.key
{{- else }}
  enabled: false
{{- end }}
{{- end -}}

{{- define "edgemesh.telemetryConfig" -}}
telemetry:
  address: 0.0.0.0:{{ .port }}
  log_level: {{ .root.Values.telemetry.logLevel | quote }}
  log_format: {{ .root.Values.telemetry.logFormat | quote }}
  trace_sample_ratio: {{ .root.Values.telemetry.traceSampleRatio }}
{{- if .root.Values.telemetry.otlpEndpoint }}
  otlp_endpoint: {{ .root.Values.telemetry.otlpEndpoint | quote }}
  otlp_insecure: {{ .root.Values.telemetry.otlpInsecure }}
{{- end }}
  enable_pprof: {{ .root.Values.telemetry.enablePprof }}
{{- end -}}
