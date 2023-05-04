package gcsfilestore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"

	fs "github.com/go-yaaf/yaaf-common/files"
)

/**
 * IMPORTANT: To use GCS from local machine you must first set up authentication by creating service account
 * and setting environment variable.
 * See https://cloud.google.com/storage/docs/reference/libraries#client-libraries-install-go for more details
 */

// region IFile implementation -----------------------------------------------------------------------------------------

// GcsFile is a concrete implementation of Google Cloud Storage file
type GcsFile struct {
	uri      string
	path     string
	gsClient *storage.Client
	context  context.Context

	reader *storage.Reader
	writer *storage.Writer
}

func NewGcsFile(uri string) fs.IFile {
	ctx := context.Background()
	return &GcsFile{uri: uri, context: ctx}
}

// URI returns the resource URI with schema
// Schema can be: file, gcs, http etc
func (t *GcsFile) URI() string {
	return t.uri
}

// Close client to free resources
func (t *GcsFile) Close() (err error) {
	if t.gsClient != nil {
		return t.gsClient.Close()
	} else {
		return nil
	}
}

// Read implements io.Reader interface
func (t *GcsFile) Read(p []byte) (int, error) {
	// ensure client
	if er := t.ensureClient(); er != nil {
		return 0, er
	}

	if t.reader != nil {
		return t.reader.Read(p)
	}

	// If reader is not open
	if bucket, object, err := parseUri(t.uri); err != nil {
		return 0, err
	} else {
		t.reader, err = t.gsClient.Bucket(bucket).Object(object).NewReader(t.context)
		if err != nil {
			return 0, err
		}
		return t.reader.Read(p)
	}
}

// Write implements io.Writer interface
func (t *GcsFile) Write(p []byte) (int, error) {
	// ensure client
	if er := t.ensureClient(); er != nil {
		return 0, er
	}

	if t.writer != nil {
		return t.writer.Write(p)
	}

	// If reader is not open
	if bucket, object, err := parseUri(t.uri); err != nil {
		return 0, err
	} else {
		t.writer = t.gsClient.Bucket(bucket).Object(object).NewWriter(t.context)
		if t.writer == nil {
			return 0, fmt.Errorf("could not create writer")
		}
		return t.writer.Write(p)
	}
}

// ReadAll read resource content to a byte array in a single call
func (t *GcsFile) ReadAll() (b []byte, err error) {
	// ensure client
	if er := t.ensureClient(); er != nil {
		return nil, er
	}

	// Get bucket and object from path
	bucket, object, err := parseUri(t.uri)
	if err != nil {
		return nil, err
	}

	rc, err := t.gsClient.Bucket(bucket).Object(object).NewReader(t.context)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// WriteAll write content to a resource in a single call
func (t *GcsFile) WriteAll(b []byte) (n int, err error) {
	// ensure client
	if er := t.ensureClient(); er != nil {
		return 0, er
	}

	// Get bucket and object from path
	bucket, object, err := parseUri(t.uri)
	if err != nil {
		return 0, err
	}

	wc := t.gsClient.Bucket(bucket).Object(object).NewWriter(t.context)

	r := bytes.NewReader(b)
	if written, er := io.Copy(wc, r); er != nil {
		return int(written), er
	}
	if err := wc.Close(); err != nil {
		return 0, err
	}
	return 0, nil
}

// Exists test if file exists
func (t *GcsFile) Exists() (result bool) {
	// ensure client
	if er := t.ensureClient(); er != nil {
		return false
	}

	// Get bucket and object from path
	bucket, object, err := parseUri(t.uri)
	if err != nil {
		return false
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return false
	}

	defer client.Close()
	_, err = client.Bucket(bucket).Object(object).Attrs(ctx)
	return err == nil
}

// Rename change the resource name using pattern.
// The pattern can be a file or keeping parts from the original file using template ({{path}}, {{file}}, {{ext}})
func (t *GcsFile) Rename(pattern string) (string, error) {
	// ensure client
	if er := t.ensureClient(); er != nil {
		return "", er
	}

	// Get bucket and object from path
	bucket, object, err := parseUri(t.uri)
	if err != nil {
		return "", err
	}

	// create new path based on pattern
	_, newPath, newFile, newExt, er := fs.ParseUri(pattern)
	if er != nil {
		return "", er
	}
	newUri := pattern
	newUri = strings.ReplaceAll(newUri, "{{path}}", newPath)
	newUri = strings.ReplaceAll(newUri, "{{file}}", newFile)
	newUri = strings.ReplaceAll(newUri, "{{ext}}", newExt)

	// Get bucket and object from path
	newBucket, newObject, err := parseUri(newUri)
	if err != nil {
		return "", err
	}

	src := t.gsClient.Bucket(bucket).Object(object)
	dst := t.gsClient.Bucket(newBucket).Object(newObject)

	if _, err = dst.CopierFrom(src).Run(t.context); err != nil {
		return "", fmt.Errorf("Object(%q).CopierFrom(%q).Run: %v", newObject, object, err)
	}
	if err = src.Delete(t.context); err != nil {
		return "", fmt.Errorf("Object(%q).Delete: %v", object, err)
	}

	t.uri = newUri
	return newUri, nil
}

// Delete a file
func (t *GcsFile) Delete() (err error) {
	// ensure client
	if er := t.ensureClient(); er != nil {
		return er
	}

	// Get bucket and object from path
	bucket, object, err := parseUri(t.uri)
	if err != nil {
		return err
	}

	if err := t.gsClient.Bucket(bucket).Object(object).Delete(t.context); err != nil {
		return err
	} else {
		return nil
	}
}

// Copy file content to a writer
func (t *GcsFile) Copy(wc io.WriteCloser) (written int64, err error) {
	// ensure client
	if er := t.ensureClient(); er != nil {
		return 0, er
	}

	if bucket, object, er := parseUri(t.uri); err != nil {
		return 0, er
	} else {
		if reader, er := t.gsClient.Bucket(bucket).Object(object).NewReader(t.context); er != nil {
			return 0, er
		} else {
			written, err = io.Copy(wc, reader)
			_ = reader.Close()
			_ = wc.Close()
			return written, err
		}
	}
}

// endregion

// region PRIVATE methods ----------------------------------------------------------------------------------------------

// GetReader return a reader interface
func (t *GcsFile) getReader() (rc io.ReadCloser, err error) {
	if bucket, object, er := parseUri(t.uri); err != nil {
		return nil, er
	} else {
		return t.gsClient.Bucket(bucket).Object(object).NewReader(t.context)
	}
}

// GetWriter return a writer interface
func (t *GcsFile) getWriter() (wc io.WriteCloser, err error) {
	if bucket, object, er := parseUri(t.uri); err != nil {
		return nil, er
	} else {
		return t.gsClient.Bucket(bucket).Object(object).NewWriter(t.context), nil
	}
}

// Ensure Google Storage Client in initialized
func (t *GcsFile) ensureClient() error {
	if t.gsClient != nil {
		return nil
	} else {
		if cli, err := storage.NewClient(t.context); err != nil {
			return err
		} else {
			t.gsClient = cli
			return nil
		}
	}
}

// endregion

// Parse GCS uri to extract bucket and file object
func parseUri(uri string) (bucket string, object string, err error) {
	// Get bucket and object from path
	if Url, er := url.Parse(uri); er != nil {
		return "", "", er
	} else {
		return Url.Host, Url.Path[1:], nil
	}
}
