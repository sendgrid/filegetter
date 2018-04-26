import (
	"io"
	"os"

	minio "github.com/minio/minio-go"
)

type Source string

const (
	// Local signifies we are using a local file source
	Local Source = "local"
	//Remote signifies we are using a remote file source
	Remote Source = "remote"
)

// FetchFile takes in the parameters needed to do both local and remote file getting
func (g *Getter) FetchFile(localPath, host, bucket, key string) (io.ReadCloser, Source, error) {
	// ensure we have the info we need to do remote file system stuff
	// validation around host, key, and bucket elided for brevity
	if g.useRemoteFS {
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
			return obj, Remote, nil
		}
		// if we get here, we are falling back to local disk
	}

	fh, err := os.Open(localPath)
	if err != nil {
		return nil, "", err
	}

	return fh, Local, nil
}
