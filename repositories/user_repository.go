package repositories

import (
	"context"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"backend/config"
	"backend/models"
)

type UserRepository struct {
	collection *mongo.Collection
}

func NewUserRepository() *UserRepository {
	return &UserRepository{
		collection: config.GetCollection("users"),
	}
}

func (r *UserRepository) Create(user *models.DBUser) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	user.CreatedAt = time.Now()
	user.UpdatedAt = time.Now()

	result, err := r.collection.InsertOne(ctx, user)
	if err != nil {
		return err
	}

	user.ID = result.InsertedID.(bson.ObjectID)
	return nil
}

func (r *UserRepository) FindByID(id string) (*models.DBUser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	objectID, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	var user models.DBUser
	err = r.collection.FindOne(ctx, bson.M{"_id": objectID}).Decode(&user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

func (r *UserRepository) FindByEmail(email string) (*models.DBUser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var user models.DBUser
	err := r.collection.FindOne(ctx, bson.M{"email": strings.ToLower(email)}).Decode(&user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

func (r *UserRepository) FindByUsername(username string) (*models.DBUser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var user models.DBUser
	err := r.collection.FindOne(ctx, bson.M{"username": strings.ToLower(username)}).Decode(&user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

func (r *UserRepository) UpdateProfile(id string, updates bson.M) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	objectID, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return err
	}

	updates["updated_at"] = time.Now()

	_, err = r.collection.UpdateOne(ctx, bson.M{"_id": objectID}, bson.M{"$set": updates})
	return err
}

func (r *UserRepository) UpdateProvider(id, provider, providerID string) error {
	return r.UpdateProfile(id, bson.M{"provider": provider, "provider_id": providerID})
}

func (r *UserRepository) SetOnlineStatus(id string, isOnline bool) error {
	return r.UpdateProfile(id, bson.M{"is_online": isOnline, "last_seen": time.Now()})
}

func (r *UserRepository) FindByOAuthProvider(provider, providerID string) (*models.DBUser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var user models.DBUser
	err := r.collection.FindOne(ctx, bson.M{"provider": provider, "provider_id": providerID}).Decode(&user)
	if err != nil {
		return nil, err
	}

	return &user, nil
}

func (r *UserRepository) CreateIndex() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "email", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "username", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "provider", Value: 1}, {Key: "provider_id", Value: 1}}, Options: options.Index().SetSparse(true)},
	}

	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	return err
}
