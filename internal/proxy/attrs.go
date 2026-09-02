package proxy

import "go.opentelemetry.io/otel/attribute"

// attrString builds a string span attribute. These keep the attribute package
// out of the handler's import list for a handful of call sites.
func attrString(k, v string) attribute.KeyValue { return attribute.String(k, v) }

// attrInt builds an integer span attribute.
func attrInt(k string, v int) attribute.KeyValue { return attribute.Int(k, v) }
