import "io"

type RemoteFetcher interface {
	GetRemoteMessage(accessKey, accessSecret, host, bucket, key string) (io.ReadCloser, error)
}

// minioWrapper will adhere to the RemoteFetcher interface
type minioWrapper struct{}

type LocalFetcher interface {
	Open(localPath string) (io.ReadCloser, error)
}

// osFile will adhere to the LocalFetcher interface
type osFile struct{}
