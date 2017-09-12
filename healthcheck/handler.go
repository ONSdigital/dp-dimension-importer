package healthcheck

import (
	"net/http"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
	"sync"
	"context"
)

const (
	gracefulShutdownMsg = "graceful shutdown of healthcheck endpoint complete"
)

var httpServer *server.Server
var serverErrors chan error
var once sync.Once

// NewHandler create and run the healthcheck API endpoint.
func NewHandler(bindAddr string, errorChan chan error) {
	once.Do(func() {
		serverErrors = errorChan
		router := mux.NewRouter()
		router.Path("/healthcheck").HandlerFunc(handle)

		httpServer = server.New(bindAddr, router)
		// Disable this here to allow main to manage graceful shutdown of the entire app.
		httpServer.HandleOSSignals = false

		go func() {
			log.Debug("Starting healthcheck endpoint...", nil)
			if err := httpServer.ListenAndServe(); err != nil {
				log.ErrorC("healthcheck server returned error", err, nil)
				serverErrors <- err
			}
		}()
	})
}

func Close(ctx context.Context) {
	httpServer.Shutdown(ctx)
	log.Info(gracefulShutdownMsg, nil)
}

// TODO Implement actual healthcheck.
func handle(w http.ResponseWriter, r *http.Request) {
	log.Debug("Healthcheck endpoint.", nil)
	w.WriteHeader(200)
}
