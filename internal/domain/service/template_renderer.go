package service

import "strings"

// Render substitutes {{key}} placeholders in tmpl with the matching value from vars.
// Unknown placeholders are left as-is so callers can inspect missing variables.
func Render(tmpl string, vars map[string]string) string {
	if len(vars) == 0 {
		return tmpl
	}
	result := tmpl
	for k, v := range vars {
		result = strings.ReplaceAll(result, "{{"+k+"}}", v)
	}
	return result
}
