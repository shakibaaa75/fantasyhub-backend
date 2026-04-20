package config

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

var Client *mongo.Client
var Database *mongo.Database

func ConnectDB() error {
	uri := os.Getenv("MONGODB_URI")
	dbName := os.Getenv("DB_NAME")

	if uri == "" {
		return fmt.Errorf("MONGODB_URI not set in .env")
	}
	if dbName == "" {
		dbName = "fantisy"
	}

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)

	// TLS config required for MongoDB Atlas
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	opts := options.Client().
		ApplyURI(uri).
		SetServerAPIOptions(serverAPI).
		SetTLSConfig(tlsConfig).
		SetConnectTimeout(15 * time.Second).
		SetServerSelectionTimeout(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := mongo.Connect(opts)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	Client = client
	Database = client.Database(dbName)

	log.Println("✅ Connected to MongoDB!")
	return nil
}

func DisconnectDB() error {
	if Client == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := Client.Disconnect(ctx); err != nil {
		return fmt.Errorf("failed to disconnect: %w", err)
	}

	log.Println("✅ Disconnected from MongoDB")
	return nil
}

func GetCollection(name string) *mongo.Collection {
	return Database.Collection(name)
}
