// Package api contains DTOs generated from shared-contracts (SDD). Do not hand-edit
// the generated files; regenerate with the commands below.
//
// notification.gen.go, from the OpenAPI contract:
//
//	oapi-codegen -generate types -package api -o api/notification.gen.go \
//	  ../design-software-shared-contracts/openapi/notification.yaml
//
// event_envelope.gen.go, from the domain event envelope JSON Schema:
//
//	go-jsonschema -p api -t --capitalization ID -o api/event_envelope.gen.go \
//	  ../design-software-shared-contracts/events/event-envelope.schema.json
//
// NOTE (2026-08-15): shared-contracts/events/event-envelope.schema.json currently has
// invalid JSON (the "pattern" regex uses a bare `\.` instead of `\\.`, an invalid JSON
// escape — confirmed with `python3 -c "import json; json.load(open(...))"`). go-jsonschema
// cannot parse it as-is. event_envelope.gen.go here was generated from a locally-patched
// copy (only that one escape fixed) — regenerate the same way once the bug is fixed
// upstream in shared-contracts. This is a read-only dependency for notification-service;
// the fix belongs in the shared-contracts repo, not here.
package api
