package services

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/akimsavvin/tablet_backend/internal/dto"
	"github.com/akimsavvin/tablet_backend/internal/models"
	"github.com/google/uuid"
)

type TabletRepo interface {
	GetAll(ctx context.Context) ([]*models.Tablet, error)
	Insert(ctx context.Context, ts *models.Tablet) error
}

type TabletService struct {
	repo          TabletRepo
	kafkaProducer sarama.SyncProducer
}

func NewTabletService(repo TabletRepo, kafkaUrl string, kafkaCfg *sarama.Config) (*TabletService, error) {
	log.Print("Creating tablet service")

	log.Print("Creating kafka producer")
	producer, err := sarama.NewSyncProducer([]string{kafkaUrl}, kafkaCfg)
	if err != nil {
		return nil, err
	}

	log.Print("Created kafka producer")

	ts := &TabletService{repo, producer}

	err = ts.watch()
	if err != nil {
		return nil, err
	}

	log.Print("Created tablet service")

	return ts, nil
}

func (ts *TabletService) sendKafkaMessage(tablet *models.Tablet) error {
	jsonTablet, err := json.Marshal(tablet)
	if err != nil {
		log.Panic(err)
	}

	kafkaMessage := &sarama.ProducerMessage{
		Topic: "TabletsSchedule",
		Key:   sarama.StringEncoder(uuid.NewString()),
		Value: sarama.ByteEncoder(jsonTablet),
	}

	part, offset, err := ts.kafkaProducer.SendMessage(kafkaMessage)

	if err != nil {
		log.Printf("Could not send kafka message: %s", err.Error())
		return err
	} else {
		log.Printf("Sent kafka message in %d partition, offset: %d", part, offset)
	}

	return nil
}

func (ts *TabletService) watchForTablet(tablet *models.Tablet) {
	log.Printf("Watching for tablet with id %s", tablet.Id)

	for {
		now := time.Now()

		if tablet.UseHour == now.Hour() && tablet.UseMinute == now.Minute() {
			err := ts.sendKafkaMessage(tablet)
			if err != nil {
				log.Panicf("Sttoped watching for tablet '%s' due to error: %s", tablet.Name, err.Error())
			}
		}

		time.Sleep(time.Minute)
	}
}

func (ts *TabletService) watch() error {
	log.Print("Starting watching for existing tablets")

	ctx, cancefFunc := context.WithTimeout(context.Background(), time.Second*5)
	defer cancefFunc()

	tablets, err := ts.repo.GetAll(ctx)
	if err != nil {
		log.Printf("Could not get existing tablets for watching due to error: %s", err.Error())
		return err
	}

	for _, tablet := range tablets {
		go ts.watchForTablet(tablet)
	}

	log.Print("Started wathing for existing tablets")

	return nil
}

func (ts *TabletService) Create(ctx context.Context, createDTO *dto.CreateTabletDTO) (*models.Tablet, error) {
	tablet := models.NewTablet(
		uuid.NewString(),
		createDTO.UserTelegramID,
		createDTO.Name,
		createDTO.UseHour,
		createDTO.UseMinute,
	)

	err := ts.repo.Insert(ctx, tablet)
	if err != nil {
		return nil, err
	}

	go ts.watchForTablet(tablet)

	return tablet, nil
}

func (ts *TabletService) Close() {
	ts.kafkaProducer.Close()
}
