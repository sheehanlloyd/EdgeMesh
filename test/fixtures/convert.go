// Package fixtures validates the example documents shipped with the repository.
//
// An example that does not apply cleanly is worse than no example: it teaches
// the wrong shape and fails at the moment someone is least equipped to debug
// it. These tests feed every shipped document through the same decode and
// validation path the admin API uses.
package fixtures

import "encoding/json"

// yamlToJSON re-encodes a parsed YAML document as JSON.
//
// YAML is what an operator writes by hand; JSON is what protojson decodes. The
// CLI performs the same conversion, so doing it here means the fixtures are
// validated through the real path rather than a parallel one.
func yamlToJSON(doc map[string]any) ([]byte, error) {
	return json.Marshal(normalize(doc))
}

// normalize converts YAML's map[any]any into map[string]any, which JSON
// requires. yaml.v3 usually produces string keys already, but nested documents
// from other sources may not.
func normalize(v any) any {
	switch value := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(value))
		for k, item := range value {
			out[k] = normalize(item)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(value))
		for k, item := range value {
			if key, ok := k.(string); ok {
				out[key] = normalize(item)
			}
		}
		return out
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = normalize(item)
		}
		return out
	default:
		return v
	}
}
