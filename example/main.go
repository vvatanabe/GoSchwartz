package main

import (
	"context"

	"os"
	"os/signal"
	"syscall"

	"log"

	"time"

	"github.com/vvatanabe/GoSchwartz/schwartz"
)

func main2() {

}

func main() {

	s := schwartz.Schwartz{}
	s.CanDO(HelloWorker{})

	go func() {
		if err := s.Work(nil); err != nil {
			log.Println(err)
		}
	}()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)

	<-sigint

	log.Println("Received a signal of graceful shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		log.Printf("Failed to graceful shutdown: %v\n", err)
	}

	log.Println("Completed graceful shutdown")

}

type HelloWorker struct {
}

func (w HelloWorker) Work(job schwartz.Job) {

}
