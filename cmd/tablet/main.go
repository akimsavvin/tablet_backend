package main

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/akimsavvin/tablet_backend/internal/dto"
	"github.com/akimsavvin/tablet_backend/internal/services"
	"github.com/akimsavvin/tablet_backend/internal/storage"
	"github.com/go-chi/chi/v5"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	kafkaUrl := os.Getenv("KAFKA_URL")

	if kafkaUrl == "" {
		log.Fatal("KAFKA_URL is not set")
	}

	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Producer.RequiredAcks = sarama.WaitForAll
	kafkaCfg.Producer.Retry.Max = 5
	kafkaCfg.Producer.Return.Successes = true

	mongoClient := createMongoClient()

	tabletStorage := storage.NewTabletMongoStorage(mongoClient.Database("tablets_backend"))
	ts, err := services.NewTabletService(tabletStorage, kafkaUrl, kafkaCfg)
	if err != nil {
		log.Fatalf("Could not create tablet service due to error: %s", err.Error())
	}

	defer ts.Close()

	r := chi.NewRouter()
	setupRouting(r, ts)

	log.Print("Starting http server")
	go http.ListenAndServe(":4000", r)
	log.Print("Server is now listening on port 4000")

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	<-stop

	// Disconnect from MongoDB
	mongoClient.Disconnect(context.Background())
	// Close tablets service
	ts.Close()
}

func createMongoClient() *mongo.Client {
	log.Print("Creating mongo client")

	mongoURI := os.Getenv("MONGO_URI")

	if mongoURI == "" {
		log.Fatal("MONGO_URI is not set")
	}

	mongoClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Could not connect to mongo due to error: %s", err.Error())
	}

	log.Print("Created mongo client")

	return mongoClient
}

func setupRouting(r *chi.Mux, ts *services.TabletService) {
	r.Post("/api/tablets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		createDTO := &dto.CreateTabletDTO{}
		defer r.Body.Close()

		err := json.NewDecoder(r.Body).Decode(createDTO)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		tablet, err := ts.Create(r.Context(), createDTO)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		jsonTablet, err := json.Marshal(tablet)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		w.Write(jsonTablet)
	})
}
