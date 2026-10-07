package schedulebit

import (
	"regexp"
	"strings"
)

// Anything shaped like one of the platform's secrets: API keys (sbk_),
// operator keys (sbo_), viewer tokens (sbv_), Anthropic keys (sk-ant-) and
// webhook secrets (whsec_).
var secret = regexp.MustCompile(`\b(sbk|sbo|sbv|whsec)_[A-Za-z0-9_\-]+|\bsk-ant-[A-Za-z0-9_\-]+`)

var bearer = regexp.MustCompile(`(?i)(authorization:?\s*"?\s*bearer\s+)[^\s"]+`)

// Redact replaces every secret in s with its prefix and "…redacted", so a key
// never reaches an error, a log line or a test's output.
func Redact(s string) string {
	s = secret.ReplaceAllStringFunc(s, func(m string) string {
		if strings.HasPrefix(m, "sk-ant-") {
			return "sk-ant-…redacted"
		}
		return m[:strings.Index(m, "_")] + "_…redacted"
	})
	return bearer.ReplaceAllString(s, "${1}…redacted")
}

type redactedError struct{ err error }

func (e redactedError) Error() string { return Redact(e.err.Error()) }
func (e redactedError) Unwrap() error { return e.err }

func redactErr(err error) error {
	if err == nil {
		return nil
	}
	return redactedError{err}
}
