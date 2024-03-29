// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package blobstorage

import (
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test"))

	t.Run("parse all metadata", func(t *testing.T) {
		m.Properties = map[string]string{
			"storageAccount":    "account",
			"storageAccessKey":  "key",
			"container":         "test",
			"getBlobRetryCount": "5",
			"decodeBase64":      "true",
		}
		meta, err := blobStorage.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, "test", meta.Container)
		assert.Equal(t, "account", meta.StorageAccount)
		assert.Equal(t, "key", meta.StorageAccessKey)
		assert.Equal(t, true, meta.DecodeBase64)
		assert.Equal(t, 5, meta.GetBlobRetryCount)
		assert.Equal(t, azblob.PublicAccessNone, meta.PublicAccessLevel)
	})

	t.Run("parse metadata with publicAccessLevel = blob", func(t *testing.T) {
		m.Properties = map[string]string{
			"publicAccessLevel": "blob",
		}
		meta, err := blobStorage.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, azblob.PublicAccessBlob, meta.PublicAccessLevel)
	})

	t.Run("parse metadata with publicAccessLevel = container", func(t *testing.T) {
		m.Properties = map[string]string{
			"publicAccessLevel": "container",
		}
		meta, err := blobStorage.parseMetadata(m)
		assert.Nil(t, err)
		assert.Equal(t, azblob.PublicAccessContainer, meta.PublicAccessLevel)
	})

	t.Run("parse metadata with invalid publicAccessLevel", func(t *testing.T) {
		m.Properties = map[string]string{
			"publicAccessLevel": "invalid",
		}
		_, err := blobStorage.parseMetadata(m)
		assert.Error(t, err)
	})
}

func TestGetOption(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test"))

	t.Run("return error if blobName is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := blobStorage.get(&r)
		if assert.Error(t, err) {
			assert.Equal(t, ErrMissingBlobName, err)
		}
	})
}

func TestDeleteOption(t *testing.T) {
	blobStorage := NewAzureBlobStorage(logger.NewLogger("test"))

	t.Run("return error if blobName is missing", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		_, err := blobStorage.delete(&r)
		if assert.Error(t, err) {
			assert.Equal(t, ErrMissingBlobName, err)
		}
	})

	t.Run("return error for invalid deleteSnapshots", func(t *testing.T) {
		r := bindings.InvokeRequest{}
		r.Metadata = map[string]string{
			"blobName":        "foo",
			"deleteSnapshots": "invalid",
		}
		_, err := blobStorage.delete(&r)
		assert.Error(t, err)
	})
}
