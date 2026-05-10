package server

import (
	"fmt"
	"io"
	"net/http"

	"github.com/gabrigio30/raft-kv/kvstore"
)

// Server exposes the KV store over HTTP.
type Server struct {
	client *kvstore.Client
	mux *http.ServeMux
}

// NewServer creates a Server wrapping the given client.
func NewServer(client *kvstore.Client) *Server {
	s := &Server{
		client: client,
		mux: http.NewServeMux(),
	}
	s.mux.HandleFunc("PUT /keys/{key}", s.handlePut)
	s.mux.HandleFunc("GET /keys/{key}", s.handleGet)
	s.mux.HandleFunc("DELETE /keys/{key}", s.handleDelete)
	return s
}

// ListenAndServe starts the HTTP server on addr.
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s.mux)
}


func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	if err := s.client.Put(key, string(body)); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, err := s.client.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	fmt.Fprint(w, val)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := s.client.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}