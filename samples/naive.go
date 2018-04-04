package samples

import (
	"io"
	"log"
	"os"

	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
)

const (
	// SourceLocal signifies we are using a local file source
	SourceLocal = "local"
	//SourceRemote signifies we are using a remote file source
	SourceRemote = "remote"
)

// Getter contains unexported properties for accessing local and remote files
type Getter struct {
	logger       *log.Logger
	useRemoteFS  bool
	accessKey    string
	accessSecret string
}

// New instantiates a Getter
func New(l *log.Logger, useRemoteFS bool, accessKey, accessSecret string) *Getter {
	return &Getter{
		logger:       l,
		useRemoteFS:  useRemoteFS,
		accessKey:    accessKey,
		accessSecret: accessSecret,
	}
}

// GetFile takes in the parameters needed to do both local and remote file getting
func (g *Getter) GetFile(localPath, host, bucket, key string) (io.ReadCloser, string, error) {
	if g.useRemoteFS && host != "" && key != "" && bucket != "" {
		// we have everything we need to do remote fs stuff
		var err error
		var client *minio.Client
		var obj *minio.Object

		client, err = minio.NewV2(host, g.accessKey, g.accessSecret, false)
		if err != nil {
			err = errors.Wrap(err, "unable to get remote fs client")
		} else {
			obj, err = client.GetObject(bucket, key)
			if err != nil {
				err = errors.Wrap(err, "unable to get remote object")
			} else {
				_, err = obj.Stat()
				if err != nil {
					err = errors.Wrap(err, "unable to get remote file info")
				} else {
					return obj, SourceRemote, nil
				}
			}
		}
		// if we get here, we are falling back to local disk

	} else if g.useRemoteFS {
		// we want to do remote fs stuff, but host, bucket, or key are messed up
		g.logger.Println("falling back to local source - missing fields")
	}

	fh, err := os.Open(localPath)
	if err != nil {
		return nil, "", err
	}

	return fh, SourceLocal, nil
}
