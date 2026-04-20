package repositories

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"backend/config"
	"backend/models"
)

type TokenRepository struct {
	collection *mongo.Collection
}

func NewTokenRepository() *TokenRepository {
	return &TokenRepository{
		collection: config.GetCollection("refresh_tokens"),
	}
}

func (r *TokenRepository) Store(token *models.StoredRefreshToken) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	token.CreatedAt = time.Now()
	_, err := r.collection.InsertOne(ctx, token)
	return err
}

func (r *TokenRepository) FindByToken(token string) (*models.StoredRefreshToken, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var t models.StoredRefreshToken
	err := r.collection.FindOne(ctx, bson.M{"token": token}).Decode(&t)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

func (r *TokenRepository) RevokeToken(token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := r.collection.UpdateOne(ctx, bson.M{"token": token}, bson.M{"$set": bson.M{"revoked": true}})
	return err
}

func (r *TokenRepository) DeleteExpired() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := r.collection.DeleteMany(ctx, bson.M{
		"$or": bson.A{
			bson.M{"expires_at": bson.M{"$lt": time.Now()}},
			bson.M{"revoked": true},
		},
	})
	return err
}

func (r *TokenRepository) CreateIndex() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := r.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "token", Value: 1}},
	})
	return err
}
