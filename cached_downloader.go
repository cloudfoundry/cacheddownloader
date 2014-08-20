package cacheddownloader

import (
	"io"
	"net/url"
)

type CachedDownloader interface {
	Fetch(url *url.URL, cacheKey string) (io.ReadCloser, error)
}

type CachingInfoType struct {
	ETag         string
	LastModified string
}
