// example setup of how you might consume from the getter package
package main

import (
	"log"
	"os"

	"github.com/sendgrid/filegetter/getter"
)

func main() {
	jobs := make(chan Job)
	logger := log.New(os.Stdout, "", log.LstdFlags)
	// Read config, determine if we are working in local or remote file getting mode
	useRemoteFS := true
	key := "key"
	secret := "secret"
	// Call the file getter, use the results to do something
	fileFetcher := getter.New(logger, useRemoteFS, key, secret)

	for job := range jobs {
		data, source, err := fileFetcher.GetFile(job.FilePath, job.Host, job.Bucket, job.Key)
		if err != nil {
			// handle err
		}
		// so something productive with the file data and the source (ie, local or remote)
		_ = data
		_ = source
	}
}

// Job represents a unit of work we will have to perform.
// Details may or may not have host, bucket, and key data.
// All jobs should have a FilePath
type Job struct {
	FilePath string
	Host     string
	Bucket   string
	Key      string
}
