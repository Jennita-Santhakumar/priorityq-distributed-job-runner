// Package apierror provides a shared HTTP error envelope for JSON responses.
package apierror

import (
	"encoding/json"
	"net/http"
)

type Envelope struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func Write(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Envelope{Error: message, Code: code})
}
