package storage

import (
	"context"
	"log"

	"github.com/akimsavvin/tablet_backend/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type TabletMongoStorage struct {
	c *mongo.Collection
}

func NewTabletMongoStorage(db *mongo.Database) *TabletMongoStorage {
	log.Print("Created tablet mongo storage")
	return &TabletMongoStorage{db.Collection("tablets")}
}

func (s *TabletMongoStorage) GetAll(ctx context.Context) ([]*models.Tablet, error) {
	cur, err := s.c.Find(ctx, bson.D{})
	if err != nil {
		log.Printf("Could not get all tablets due to error: %s", err.Error())
		return nil, err
	}

	defer cur.Close(ctx)

	var res []*models.Tablet

	for cur.Next(ctx) {
		tablet := &models.Tablet{}
		if err := cur.Decode(tablet); err != nil {
			log.Printf("Could not get all tablets due to decoding error: %s", err.Error())
			return nil, err
		}
		res = append(res, tablet)
	}

	if err := cur.Err(); err != nil {
		log.Printf("Could not get all tablets due to cursor error: %s", err.Error())
		return nil, err
	}

	return res, nil
}

func (s *TabletMongoStorage) Insert(ctx context.Context, t *models.Tablet) error {
	_, err := s.c.InsertOne(ctx, t)
	if err != nil {
		log.Printf("Could not insert tablet due to error: %s", err.Error())
		return err
	}

	log.Printf("Inserted tablet with id %s", t.Id)

	return nil
}
