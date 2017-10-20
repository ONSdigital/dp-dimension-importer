package healthcheck

import (
	"context"
	"net/http"
	"sync"

	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
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

func Close(ctx context.Context) {
	httpServer.Shutdown(ctx)
	logger.Info("graceful shutdown of healthcheck endpoint complete", nil)
}

// TODO Implement actual healthcheck.
func handle(w http.ResponseWriter, r *http.Request) {
	logger.Info("Healthcheck endpoint.", nil)
	w.WriteHeader(200)
}
