package repositories

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"backend/config"
	"backend/models"
)

type ChatRepository struct {
	collection *mongo.Collection
}

func NewChatRepository() *ChatRepository {
	return &ChatRepository{
		collection: config.GetCollection("messages"),
	}
}

// SaveMessage inserts a new chat message into the database
func (r *ChatRepository) SaveMessage(msg *models.DBMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ensure timestamp is set
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	_, err := r.collection.InsertOne(ctx, msg)
	return err
}

// GetMessagesByMatch retrieves chat history for a specific match, sorted newest first
func (r *ChatRepository) GetMessagesByMatch(matchID string, limit int64) ([]models.DBMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if limit == 0 {
		limit = 50 // Default limit
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "timestamp", Value: -1}}). // Newest first
		SetLimit(limit)

	cursor, err := r.collection.Find(ctx, bson.M{"match_id": matchID}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var messages []models.DBMessage
	if err := cursor.All(ctx, &messages); err != nil {
		return nil, err
	}

	return messages, nil
}

// CreateIndex creates indexes for the messages collection
func (r *ChatRepository) CreateIndex() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := r.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "match_id", Value: 1}, {Key: "timestamp", Value: -1}},
	})
	return err
}
