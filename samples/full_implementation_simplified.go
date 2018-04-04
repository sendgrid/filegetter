package getter

import (
	"io"
	"log"
	"os"

	"github.com/minio/minio-go"
)

// MessageGetter allows us to get a ReadCloser, the source (remote/local), or an error when attempting to get
// a message from either local or remote storage
type MessageGetter interface {
	GetMessage(localPath, host, bucket, key string) (io.ReadCloser, string, error)
}

// Getter contains unexported fields allowing the local or remote fetching of files
type Getter struct {
	useRemoteFS          bool
	isRemoteFSCanaryHost bool
	accessKey            string
	accessSecret         string

	remoteGetter RemoteGetter
	localGetter  LocalGetter
}

// New creates a instatialized Getter that can get files locally or remotely.
// useRemoteFS tells us if the service is configured to use the remote file system.
// accessKey and accessSecret are authentication parts for the remote file system.
func New(useRemoteFS, accessKey, accessSecret string) *Getter {
	return &Getter{
		useRemoteFS:  useRemoteFS,
		accessKey:    accessKey,
		accessSecret: accessSecret,
		remoteGetter: &minioWrapper{},
		localGetter:  &osFile{},
	}
}

// GetMessage will reach out to s3 or use the local file system to retrieve an email message
func (g *Getter) GetMessage(localPath, host, bucket, key string) (io.ReadCloser, string, error) {
	// validation against host, bucket, and key elided
	if g.useRemoteFS {
		// we have everything we need to do remote fs stuff
		fh, err := g.remoteGetter.GetRemoteMessage(g.accessKey, g.accessSecret, host, bucket, key)
		if err == nil {
			// early return
			return fh, "remote", nil
		}

		log.Println("falling back to local source - %v", err)
	}
	fh, err := g.localGetter.Open(localPath)
	if err != nil { /* return wrapped error */
	}

	return fh, "local", nil
}

type RemoteGetter interface {
	GetRemoteMessage(accessKey, accessSecret, host, bucket, key string) (io.ReadCloser, error)
}

type minioWrapper struct{}

func (_ *minioWrapper) GetRemoteMessage(accessKey, accessSecret, host, bucket, key string) (io.ReadCloser, error) {
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

type LocalGetter interface {
	Open(localPath string) (io.ReadCloser, error)
}

type osFile struct{}

func (f *osFile) Open(localPath string) (io.ReadCloser, error) {
	return os.Open(localPath)
}
