package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Storage struct {
	client   *s3.Client
	bucket   string
	basePath string
}

func NewS3Storage(ctx context.Context, bucket, basePath string) (*S3Storage, error) {
	if bucket == "" {
		return nil, fmt.Errorf("S3_BUCKET not configured")
	}

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	return &S3Storage{
		client:   s3.NewFromConfig(awsCfg),
		bucket:   bucket,
		basePath: basePath,
	}, nil
}

func (s *S3Storage) Upload(ctx context.Context, filePath, s3Key string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s3Key),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("upload to S3: %w", err)
	}

	return nil
}

func (s *S3Storage) BuildS3Key(eventInfo *EventInfo, filename string) string {
	basePath := s.basePath
	if basePath == "" {
		basePath = "raw_greyhounds_data"
	}
	return filepath.Join(basePath, "PRO", eventInfo.Year, eventInfo.Month, eventInfo.Day, eventInfo.EventID, filename)
}