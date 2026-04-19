package api

import (
	"encoding/json"
	"log"
	"net/http"
)

// recoveryMiddleware catches panics, logs them, and returns a 500.
func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("panic recovered: %v", rec)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(ErrorResponse{
					Error: "internal server error",
					Code:  "INTERNAL",
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
