package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := loadConfig()

	consumer, err := NewBuildConsumer(cfg)
	if err != nil {
		log.Fatalf("failed to initialize build consumer: %v", err)
	}
	defer consumer.Close()

	go func() {
		if err := consumer.Start(); err != nil {
			log.Fatalf("build consumer stopped: %v", err)
		}
	}()

	server := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}),
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server stopped: %v", err)
		}
	}()

	log.Printf("build worker listening on :%s and consuming %s", cfg.HTTPPort, cfg.ApplicationBuildQueue)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
