package model

import "errors"

// ErrNotFound is returned by the repository when no row matches the requested id.
var ErrNotFound = errors.New("notification not found")

// ErrPermanentFailure wraps errors that must not be retried (invalid envelope, unsupported
// event_type). The AMQP consumer checks errors.Is(err, ErrPermanentFailure) to decide
// whether to Nack immediately or apply exponential backoff (HU-NOTIF-004).
var ErrPermanentFailure = errors.New("permanent failure: no retry")
