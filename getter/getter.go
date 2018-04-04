package getter

import (
	"io"
	"log"
	"os"

	"github.com/minio/minio-go"
	"github.com/pkg/errors"
)

const (
	// SourceLocal signifies if a file or error came from local disk
	SourceLocal = "local"
	// SourceRemote signifies if a file or error came from the remote disk
	SourceRemote = "remote"
)

// FileGetter allows us to get a ReadCloser, the source (remote/local), or an error when attempting to get
// a file from either local or remote storage
type FileGetter interface {
	GetFile(localPath, host, bucket, key string) (io.ReadCloser, string, error)
}

// Getter contains unexported fields allowing the local or remote fetching of files
type Getter struct {
	logger       *log.Logger
	useRemoteFS  bool
	accessKey    string
	accessSecret string

	remoteGetter remoteGetter
	localGetter  localGetter
}

// New creates a instatialized Getter that can get files locally or remotely.
// useRemoteFS tells us if the service is configured to use the remote file system.
// accessKey and accessSecret are authentication parts for the remote file system.
func New(logger *log.Logger, useRemoteFS bool, accessKey, accessSecret string) *Getter {
	return &Getter{
		logger:       logger,
		useRemoteFS:  useRemoteFS,
		accessKey:    accessKey,
		accessSecret: accessSecret,
		remoteGetter: &minioWrapper{},
		localGetter:  &osFile{},
	}
}

// GetFile will reach out to s3 or use the local file system to retrieve an email file
func (g *Getter) GetFile(localPath, host, bucket, key string) (io.ReadCloser, string, error) {
	if g.useRemoteFS && host != "" && key != "" && bucket != "" {
		// we have everything we need to do remote fs stuff
		fh, err := g.remoteGetter.GetRemoteFile(g.accessKey, g.accessSecret, host, bucket, key)
		if err == nil {
			return fh, SourceRemote, nil
		}

		g.logger.Printf("falling back to local source - %v", err)
	} else if g.useRemoteFS {
		// we want to do remote fs stuff, but host, bucket, or key are messed up
		g.logger.Printf(`falling back to local source - missing fields. "host":%q, "bucket":%q, "key":%q`, host, bucket, key)
	}

	fh, err := g.localGetter.Open(localPath)
	if err != nil {
		return nil, SourceLocal, err
	}

	return fh, SourceLocal, nil
}

type remoteGetter interface {
	GetRemoteFile(accessKey, accessSecret, host, bucket, key string) (io.ReadCloser, error)
}

// minioWrapper adheres to the remoteGetter interface
type minioWrapper struct{}

// GetRemoteFile returns a remote file
func (*minioWrapper) GetRemoteFile(accessKey, accessSecret, host, bucket, key string) (io.ReadCloser, error) {
	client, err := minio.NewV2(host, accessKey, accessSecret, false)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get remote fs client")
	}

	obj, err := client.GetObject(bucket, key)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get remote object")
	}
	_, err = obj.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get remote file info")
	}

	return obj, nil
}

type localGetter interface {
	Open(localPath string) (io.ReadCloser, error)
}

// osFile adheres to the localGetter interface
type osFile struct{}

// Open opens a local file
func (f *osFile) Open(localPath string) (io.ReadCloser, error) {
	return os.Open(localPath)
}
