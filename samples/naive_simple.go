package samples

import (
	"io"
	"os"

	minio "github.com/minio/minio-go"
)

// Getter contains unexported properties for accessing local and remote files
type Getter struct {
	useRemoteFS  bool
	accessKey    string
	accessSecret string
}

// New instantiates a Getter
func New(useRemoteFS bool, accessKey, accessSecret string) *Getter {
	return &Getter{
		useRemoteFS:  useRemoteFS,
		accessKey:    accessKey,
		accessSecret: accessSecret,
	}
}

// GetFile takes in the parameters needed to do both local and remote file getting
func (g *Getter) GetFile(localPath, host, bucket, key string) (io.ReadCloser, string, error) {
	// ensure we have the info we need to do remote file system stuff
	if g.useRemoteFS && host != "" && key != "" && bucket != "" {
		var localFallback bool
		client, err := minio.NewV2(host, g.accessKey, g.accessSecret, false)
		if err != nil {
			// handle err
			localFallback = true
		}
		obj, err := client.GetObject(bucket, key)
		if err != nil {
			// handle err
			localFallback = true
		}
		_, err = obj.Stat()
		if err != nil {
			// handle err
			localFallback = true
		}
		if !localFallback {
			return obj, SourceRemote, nil
		}
		// if we get here, we are falling back to local disk
	}

	fh, err := os.Open(localPath)
	if err != nil {
		return nil, "", err
	}

	return fh, SourceLocal, nil
}
