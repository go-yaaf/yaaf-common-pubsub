package gcsfilestore

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	fs "github.com/go-yaaf/yaaf-common/files"
)

/**
 * IMPORTANT: To use GCS from local machine you must first set up authentication by creating service account
 * and setting environment variable.
 * See https://cloud.google.com/storage/docs/reference/libraries#client-libraries-install-go for more details
 */

// region IFileStore implementation ------------------------------------------------------------------------------------

// GcsFileStore is a concrete implementation of Google Cloud Storage
type GcsFileStore struct {
	uri      string
	path     string
	gsClient *storage.Client
	context  context.Context
}

// NewGcsFileStore factory method
func NewGcsFileStore(uri string) fs.IFileStore {
	ctx := context.Background()
	cli, _ := storage.NewClient(ctx)

	path := uri

	if Url, err := url.Parse(uri); err == nil {
		path = Url.Path
	}

	return &GcsFileStore{
		uri:      uri,
		path:     path,
		gsClient: cli,
		context:  ctx,
	}
}

// URI returns the resource URI with schema
// Schema can be: file, gcs, http etc
func (f *GcsFileStore) URI() string {
	return f.uri
}

// List files in the file store
func (f *GcsFileStore) List(filter string) ([]fs.IFile, error) {

	result := make([]fs.IFile, 0)
	cb := func(filePath string) {
		result = append(result, NewGcsFile(filePath))
	}

	err := f.Apply(filter, cb)
	return result, err
}

// Apply action on files in the file store
func (f *GcsFileStore) Apply(filter string, action func(string)) error {

	// Get bucket and prefix from path
	bucket, prefix, err := parseUri(f.uri)
	if err != nil {
		return err
	}

	rgx, erx := regexp.Compile(filter)
	if erx != nil {
		if len(filter) > 0 {
			return erx
		}
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: fmt.Sprintf("%s/", prefix)})
	for {
		if attrs, er := it.Next(); er != nil {
			if er == iterator.Done {
				break
			} else {
				return er
			}
		} else {
			if attrs.Size > 0 {
				filePath := fmt.Sprintf("gcs://%s/%s", bucket, attrs.Name)
				if rgx == nil {
					action(filePath)
				} else {
					if rgx.MatchString(filePath) {
						action(filePath)
					}
				}
			}
		}
	}
	return nil
}

// Exists test for resource existence
func (f *GcsFileStore) Exists(uri string) (result bool) {
	if strings.HasPrefix(uri, "gcs://") || strings.HasPrefix(uri, "gs://") {
		return NewGcsFile(uri).Exists()
	} else {
		return NewGcsFile(fs.CombineUri(f.uri, uri)).Exists()
	}
}

// Delete resource
func (f *GcsFileStore) Delete(uri string) (err error) {
	if strings.HasPrefix(uri, "gcs://") || strings.HasPrefix(uri, "gs://") {
		return NewGcsFile(uri).Delete()
	} else {
		return NewGcsFile(fs.CombineUri(f.uri, uri)).Delete()
	}
}

// Close release associated resources, if any
func (f *GcsFileStore) Close() error {
	if f.gsClient != nil {
		return f.gsClient.Close()
	}
	return nil
}

// endregion
