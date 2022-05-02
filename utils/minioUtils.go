package utils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioSessionConf struct {
	host   string
	port   string
	id     string
	secret string
}

type MinioSession struct {
	conf   MinioSessionConf
	client *minio.Client
	mu     sync.Mutex
}

type ObjectInfo struct {
	Key          string
	ContentType  string
	LastModified time.Time
}

var once sync.Once
var session MinioSession

func getSessionConf() MinioSessionConf {
	session.conf.id = os.Getenv("MINIO_ROOT_USER")
	session.conf.secret = os.Getenv("MINIO_ROOT_PASSWORD")
	session.conf.host = os.Getenv("MINIO_HOST")
	session.conf.port = os.Getenv("MINIO_PORT")
	return session.conf
}

func CreateMinioSession() *MinioSession {
	once.Do(func() {
		var err error
		_conf := getSessionConf()
		endpoint := fmt.Sprintf("%s:%s", _conf.host, _conf.port)
		session.client, err = minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(_conf.id, _conf.secret, ""),
			Secure: false,
		})
		if err != nil {
			log.Println(err)
		}
	})
	return &session
}

func (session *MinioSession) PutObject(buckectName string, objName string, content []byte, contentType string) bool {
	if session.client == nil {
		return false
	}
	session.mu.Lock()
	defer session.mu.Unlock()

	reader := bytes.NewReader(content)
	n, err := session.client.PutObject(context.Background(), buckectName, objName, reader, reader.Size(), minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		log.Println(err)
	}
	if n.Size != reader.Size() {
		log.Println("Put length error")
		return false
	}
	return true
}

func (session *MinioSession) GetObject(bucketName string, objName string) (*minio.Object, error) {
	if session.client == nil {
		return nil, errors.New("Client is nil")
	}
	session.mu.Lock()
	defer session.mu.Unlock()

	obj, err := session.client.GetObject(context.Background(), bucketName, objName, minio.GetObjectOptions{})
	if err != nil {
		log.Println(err)
	}
	return obj, err

}

func (session *MinioSession) DeleteObject(bucketName string, objName string) error {
	if session.client == nil {
		return errors.New("Client is nil")
	}
	session.mu.Lock()
	defer session.mu.Unlock()

	err := session.client.RemoveObject(context.Background(), bucketName, objName, minio.RemoveObjectOptions{
		GovernanceBypass: true,
	})
	if err != nil {
		log.Println(err)
	}
	return err
}

func (session *MinioSession) ListObjects(bucketName string) ([]ObjectInfo, error) {
	if session.client == nil {
		return nil, errors.New("Client is nil")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objectInfos := make([]ObjectInfo, 0, 10)
	objectInfoCh := session.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		WithMetadata: true,
		Recursive:    true,
	})

	for objectInfo := range objectInfoCh {
		if objectInfo.Err != nil {
			log.Println(objectInfo.Err)
			continue
		}
		objectInfos = append(objectInfos, ObjectInfo{
			Key:          objectInfo.Key,
			ContentType:  objectInfo.UserMetadata["content-type"],
			LastModified: objectInfo.LastModified,
		})
	}
	return objectInfos, nil
}
