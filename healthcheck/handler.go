package healthcheck

import (
	"context"
	"net/http"
	"sync"

	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

var (
	httpServer   *server.Server
	serverErrors chan error
	once         sync.Once
	logger       = logging.Logger{"healthcheck.Handler"}
)

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
				logger.ErrorC("healthcheck server returned error", err, nil)
				serverErrors <- err
			}
		}()
	})
}

// Close properly shutdown the http server
func Close(ctx context.Context) error {
	if err := httpServer.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "httpServer.Shutdown returned an error")
	}
	logger.Info("graceful shutdown of healthcheck endpoint complete", nil)
	return nil
}

// TODO Implement actual healthcheck.
func handle(w http.ResponseWriter, r *http.Request) {
	logger.Info("Healthcheck endpoint.", nil)
	w.WriteHeader(200)
}
