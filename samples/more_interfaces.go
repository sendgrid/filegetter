package samples

import (
	"io"
	"os"

	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
)

// RemoteGetter will return the remote file
type RemoteGetter interface {
	GetRemoteFile(accessKey, accessSecret, host, bucket, key string) (io.ReadCloser, error)
}

// minioWrapper will adhere to the RemoteGetter interface
type minioWrapper struct{}

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

// LocalGetter will return the local file
type LocalGetter interface {
	Open(localPath string) (io.ReadCloser, error)
}

// osFile will adhere to the LocalGetter interface
type osFile struct{}

func (f *osFile) Open(localPath string) (io.ReadCloser, error) {
	return os.Open(localPath)
}
