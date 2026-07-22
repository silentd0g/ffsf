package wsserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWsHandlerInvalidHandshakeDoesNotPanic(t *testing.T) {
	server := &WsServer{}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()

	server.WsHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: got %d, want %d", response.Code, http.StatusBadRequest)
	}
}
