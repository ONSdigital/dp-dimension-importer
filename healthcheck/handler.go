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
	gracefulShutdownMsg = "Graceful shutdown of healthcheck endpoint complete"
)

var httpServer *server.Server
var once sync.Once

func NewHandler() {
	once.Do(func() {
		router := mux.NewRouter()
		router.Path("/healthcheck").HandlerFunc(handle)

		httpServer = server.New(":22000", router)
		httpServer.HandleOSSignals = false

		go func() {
			log.Debug("Starting http server...", nil)
			if err := httpServer.ListenAndServe(); err != nil {
				log.ErrorC("Sever returned error", err, nil)
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
