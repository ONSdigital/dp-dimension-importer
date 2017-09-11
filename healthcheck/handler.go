package healthcheck

import (
	"net/http"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
	"sync"
	"context"
	"time"
)


const (
	gracefulShutdownMsg = "Graceful shutdown of healthcheck endpoint complete"
)

var httpServer *server.Server
var once sync.Once

// NewHandler create and run the healthcheck API endpoint.
func NewHandler(bindAddr string, shutdownTimeout time.Duration) {
	once.Do(func() {
		router := mux.NewRouter()
		router.Path("/healthcheck").HandlerFunc(handle)

		httpServer = server.New(bindAddr, router)
		// Disable this here to allow main to manage graceful shutdown of the entire app.
		httpServer.HandleOSSignals = false
		httpServer.DefaultShutdownTimeout = shutdownTimeout

		go func() {
			log.Debug("Starting healthcheck endpoint...", nil)
			if err := httpServer.ListenAndServe(); err != nil {
				log.ErrorC("healthcheck server returned error", err, nil)
			}
		}()
	})
}

func Close(ctx context.Context) {
	httpServer.Shutdown(ctx)
	log.Info(gracefulShutdownMsg, nil)
}

func handle(w http.ResponseWriter, r *http.Request) {
	log.Debug("Healthcheck endpoint.", nil)
	w.WriteHeader(200)
}
