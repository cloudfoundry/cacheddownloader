package cacheddownloader

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"sync"
	"time"
)

// called after a new object has entered the cache.
// it is assumed that `path` will be removed, if a new path is returned.
// a noop transformer returns the given path and its detected size.
type CacheTransformer func(source, destination string) (newSize int64, wellCrap error)

type CachedDownloader interface {
	Fetch(url *url.URL, cacheKey string, transformer CacheTransformer) (*CachedFile, error)
}

func NoopTransform(source, destination string) (int64, error) {
	err := os.Rename(source, destination)
	if err != nil {
		return 0, err
	}

	fi, err := os.Stat(destination)
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}

type CachingInfoType struct {
	ETag         string
	LastModified string
}

type cachedDownloader struct {
	downloader   *Downloader
	uncachedPath string
	cache        *FileCache

	lock       *sync.Mutex
	inProgress map[string]chan struct{}
}

func (c CachingInfoType) isCacheable() bool {
	return c.ETag != "" || c.LastModified != ""
}

func (c CachingInfoType) Equal(other CachingInfoType) bool {
	return c.ETag == other.ETag && c.LastModified == other.LastModified
}

func New(cachedPath string, uncachedPath string, maxSizeInBytes int64, downloadTimeout time.Duration, maxConcurrentDownloads int) *cachedDownloader {
	os.RemoveAll(cachedPath)
	os.MkdirAll(cachedPath, 0770)
	return &cachedDownloader{
		downloader:   NewDownloader(downloadTimeout, maxConcurrentDownloads),
		uncachedPath: uncachedPath,
		cache:        NewCache(cachedPath, maxSizeInBytes),
		lock:         &sync.Mutex{},
		inProgress:   map[string]chan struct{}{},
	}
}

func (c *cachedDownloader) Fetch(url *url.URL, cacheKey string, transformer CacheTransformer) (*CachedFile, error) {
	if cacheKey == "" {
		return c.fetchUncachedFile(url, transformer)
	}

	cacheKey = fmt.Sprintf("%x", md5.Sum([]byte(cacheKey)))
	return c.fetchCachedFile(url, cacheKey, transformer)
}

func (c *cachedDownloader) fetchUncachedFile(url *url.URL, transformer CacheTransformer) (*CachedFile, error) {
	download, _, err := c.populateCache(url, "uncached", CachingInfoType{}, transformer)
	if err != nil {
		return nil, err
	}

	return tempFileRemoveOnClose(download.path)
}

func (c *cachedDownloader) fetchCachedFile(url *url.URL, cacheKey string, transformer CacheTransformer) (*CachedFile, error) {
	rateLimiter := c.acquireLimiter(cacheKey)
	defer c.releaseLimiter(cacheKey, rateLimiter)

	// lookup cache entry
	currentReader, currentCachingInfo, getErr := c.cache.Get(cacheKey)

	// download (short circuits if endpoint respects etag/etc.)
	download, cacheIsWarm, err := c.populateCache(url, cacheKey, currentCachingInfo, transformer)
	if err != nil {
		if currentReader != nil {
			currentReader.Close()
		}
		return nil, err
	}

	// nothing had to be downloaded; return the cached entry
	if cacheIsWarm {
		return currentReader, getErr
	}

	// current cache is not fresh; disregard it
	if currentReader != nil {
		currentReader.Close()
	}

	// fetch uncached data
	var newReader *CachedFile
	if download.cachingInfo.isCacheable() {
		newReader, err = c.cache.Add(cacheKey, download.path, download.size, download.cachingInfo)
		if err == NotEnoughSpace {
			return tempFileRemoveOnClose(download.path)
		}
	} else {
		c.cache.Remove(cacheKey)
		newReader, err = tempFileRemoveOnClose(download.path)
	}

	// return newly fetched file
	return newReader, err
}

func (c *cachedDownloader) acquireLimiter(cacheKey string) chan struct{} {
	for {
		c.lock.Lock()
		rateLimiter := c.inProgress[cacheKey]
		if rateLimiter == nil {
			rateLimiter = make(chan struct{})
			c.inProgress[cacheKey] = rateLimiter
			c.lock.Unlock()
			return rateLimiter
		}
		c.lock.Unlock()
		<-rateLimiter
	}
}

func (c *cachedDownloader) releaseLimiter(cacheKey string, limiter chan struct{}) {
	c.lock.Lock()
	delete(c.inProgress, cacheKey)
	close(limiter)
	c.lock.Unlock()
}

func tempFileRemoveOnClose(path string) (*CachedFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return NewFileCloser(f, func(path string) {
		os.RemoveAll(path)
	}), nil
}

type download struct {
	path        string
	size        int64
	cachingInfo CachingInfoType
}

func (c *cachedDownloader) populateCache(
	url *url.URL,
	name string,
	cachingInfo CachingInfoType,
	transformer CacheTransformer,
) (download, bool, error) {
	filename, cachingInfo, err := c.downloader.Download(url, func() (*os.File, error) {
		return ioutil.TempFile(c.uncachedPath, name+"-")
	}, cachingInfo)
	if err != nil {
		return download{}, false, err
	}

	if filename == "" {
		return download{}, true, nil
	}

	cachedFile, err := ioutil.TempFile(c.uncachedPath, "transformed")
	if err != nil {
		return download{}, false, err
	}

	err = cachedFile.Close()
	if err != nil {
		return download{}, false, err
	}

	cachedSize, err := transformer(filename, cachedFile.Name())
	if err != nil {
		// os.Remove(filename)
		return download{}, false, err
	}

	return download{
		path:        cachedFile.Name(),
		size:        cachedSize,
		cachingInfo: cachingInfo,
	}, false, nil
}
