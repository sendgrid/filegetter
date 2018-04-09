package getter

import (
	"io"
	"log"
	"os"

	"github.com/minio/minio-go"
)

// Source allows us to have a type safe return value specifying if a file was remote or local
type Source string

const (
	// Local signifies we are using a local file source
	Local Source = "local"
	//Remote signifies we are using a remote file source
	Remote Source = "remote"
)

// MessageGetter allows us to get a ReadCloser, the source (remote/local), or an error when attempting to get
// a message from either local or remote storage
type MessageGetter interface {
	GetMessage(localPath, host, bucket, key string) (io.ReadCloser, Source, error)
}

// Getter contains unexported fields allowing the local or remote fetching of files
type Getter struct {
	useRemoteFS          bool
	isRemoteFSCanaryHost bool
	accessKey            string
	accessSecret         string

	remoteFetcher RemoteFetcher
	localFetcher  LocalFetcher
}

// New creates a instatialized Getter that can get files locally or remotely.
// useRemoteFS tells us if the service is configured to use the remote file system.
// accessKey and accessSecret are authentication parts for the remote file system.
func New(useRemoteFS, accessKey, accessSecret string) *Getter {
	return &Getter{
		useRemoteFS:   useRemoteFS,
		accessKey:     accessKey,
		accessSecret:  accessSecret,
		remoteFetcher: &minioWrapper{},
		localFetcher:  &osFile{},
	}
}

// GetMessage will reach out to s3 or use the local file system to retrieve an email message
func (g *Getter) GetMessage(localPath, host, bucket, key string) (io.ReadCloser, Source, error) {
	// validation against host, bucket, and key elided for brefity
	if g.useRemoteFS {
		// we have everything we need to do remote fs stuff
		fh, err := g.remoteFetcher.FetchRemoteMessage(g.accessKey, g.accessSecret, host, bucket, key)
		if err == nil {
			// early return
			return fh, Remote, nil
		}

		log.Println("falling back to local source - %v", err)
	}
	fh, err := g.localFetcher.Open(localPath)
	if err != nil { /* return wrapped error */
	}

	return fh, Local, nil
}

type RemoteFetcher interface {
	FetchRemoteMessage(accessKey, accessSecret, host, bucket, key string) (io.ReadCloser, error)
}

type minioWrapper struct{}

func (_ *minioWrapper) FetchRemoteMessage(accessKey, accessSecret, host, bucket, key string) (io.ReadCloser, error) {
	client, err := minio.NewV2(host, accessKey, accessSecret, false)
	if err != nil { /* return wrapped error */
	}

	obj, err := client.GetObject(bucket, key)
	if err != nil { /* return wrapped error */
	}
	_, err = obj.Stat()
	if err != nil { /* return wrapped error */
	}

	return obj, nil
}

type LocalFetcher interface {
	Open(localPath string) (io.ReadCloser, error)
}

type osFile struct{}

func (f *osFile) Open(localPath string) (io.ReadCloser, error) {
	return os.Open(localPath)
}
