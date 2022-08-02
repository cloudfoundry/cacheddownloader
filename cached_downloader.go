package cacheddownloader

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"code.cloudfoundry.org/lager"
)

// called after a new object has entered the cache.
// it is assumed that `path` will be removed, if a new path is returned.
// a noop transformer returns the given path and its detected size.
type CacheTransformer func(source, destination string) (newSize int64, err error)

//go:generate counterfeiter -o cacheddownloaderfakes/fake_cached_downloader.go . CachedDownloader

// CachedDownloader is responsible for downloading and caching files and maintaining reference counts for each cache entry.
// Entries in the cache with no active references are ejected from the cache when new space is needed.
type CachedDownloader interface {
	// Fetch downloads the file at the given URL and stores it in the cache with the given cacheKey.
	// If cacheKey is empty, the file will not be saved in the cache.
	//
	// Fetch returns a stream that can be used to read the contents of the downloaded file. While this stream is active (i.e., not yet closed),
	// the associated cache entry will be considered in use and will not be ejected from the cache.
	Fetch(logger lager.Logger, urlToFetch *url.URL, cacheKey string, checksum ChecksumInfoType, cancelChan <-chan struct{}) (stream io.ReadCloser, size int64, err error)

	// FetchAsDirectory downloads the tarfile pointed to by the given URL, expands the tarfile into a directory, and returns the path of that directory as well as the total number of bytes downloaded.
	FetchAsDirectory(logger lager.Logger, urlToFetch *url.URL, cacheKey string, checksum ChecksumInfoType, cancelChan <-chan struct{}) (dirPath string, size int64, err error)

	// CloseDirectory decrements the usage counter for the given cacheKey/directoryPath pair.
	// It should be called when the directory returned by FetchAsDirectory is no longer in use.
	// In this way, FetchAsDirectory and CloseDirectory should be treated as a pair of operations,
	// and a process that calls FetchAsDirectory should make sure a corresponding CloseDirectory is eventually called.
	CloseDirectory(logger lager.Logger, cacheKey, directoryPath string) error

	// SaveState writes the current state of the cache metadata to a file so that it can be recovered
	// later. This should be called on process shutdown.
	SaveState(logger lager.Logger) error

	// RecoverState checks to see if a state file exists (from a previous SaveState call), and restores
	// the cache state from that information if such a file exists. This should be called on startup.
	RecoverState(logger lager.Logger) error
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

type ChecksumInfoType struct {
	Algorithm string
	Value     string
}

type cachedDownloader struct {
	downloader    *Downloader
	uncachedPath  string
	cache         *FileCache
	transformer   CacheTransformer
	cacheLocation string

	lock       *sync.Mutex
	inProgress map[string]chan struct{}
}

func (c CachingInfoType) isCacheable() bool {
	return c.ETag != "" || c.LastModified != ""
}

func (c CachingInfoType) Equal(other CachingInfoType) bool {
	return c.ETag == other.ETag && c.LastModified == other.LastModified
}

// A transformer function can be used to do post-download
// processing on the file before it is stored in the cache.
func New(
	downloader *Downloader,
	cache *FileCache,
	transformer CacheTransformer,
) *cachedDownloader {
	os.MkdirAll(cache.CachedPath, 0770)
	return &cachedDownloader{
		cache:         cache,
		cacheLocation: filepath.Join(cache.CachedPath, "saved_cache.json"),
		uncachedPath:  createTempCachedDir(cache.CachedPath),
		downloader:    downloader,
		transformer:   transformer,

		lock:       &sync.Mutex{},
		inProgress: map[string]chan struct{}{},
	}
}

func createTempCachedDir(path string) string {
	workDir := filepath.Join(path, "temp")
	os.RemoveAll(workDir)
	os.MkdirAll(workDir, 0755)
	return workDir
}

func (c *cachedDownloader) SaveState(logger lager.Logger) error {
	logger = logger.Session("save-state")
	logger.Debug("marshalling-cache")
	json, err := json.Marshal(c.cache)
	if err != nil {
		logger.Debug("unable-to-marshall-cache")
		return err
	}

	return ioutil.WriteFile(c.cacheLocation, json, 0600)
}

func (c *cachedDownloader) RecoverState(logger lager.Logger) error {
	logger = logger.Session("recover-state")
	logger.Debug("recover-state-started")
	defer logger.Debug("recover-state-ended")

	logger.Debug("opening-cache-location", lager.Data{"cache-location": c.cacheLocation})
	file, err := os.Open(c.cacheLocation)
	if err != nil && !os.IsNotExist(err) {
		logger.Debug("failed-to-open-cache-location", lager.Data{"cache-location": c.cacheLocation})
		return err
	}

	defer file.Close()

	if err == nil {
		// parse the file only if it exists
		logger.Debug("decoding-file")
		err = json.NewDecoder(file).Decode(c.cache)
		file.Close()
	}

	// set the inuse count to 0 since all containers will be recreated
	logger.Debug("reset-in-use-counts")
	for _, entry := range c.cache.Entries {
		// inuseCount starts at 1 (i.e. 1 == no references to the entry)
		entry.directoryInUseCount = 0
		entry.fileInUseCount = 0
	}

	// delete files that aren't in the cache. **note** if there is no
	// saved_cache.json, then all files will be deleted
	trackedFiles := map[string]struct{}{}

	for _, entry := range c.cache.Entries {
		trackedFiles[entry.FilePath] = struct{}{}
		trackedFiles[entry.ExpandedDirectoryPath] = struct{}{}
	}

	logger.Debug("read-cached-path-dir")
	files, err := ioutil.ReadDir(c.cache.CachedPath)
	if err != nil && !os.IsNotExist(err) {
		logger.Debug("failed-to-read-cached-path-dir")
		return err
	}

	logger.Debug("removing-files")
	for _, file := range files {
		path := filepath.Join(c.cache.CachedPath, file.Name())
		if _, ok := trackedFiles[path]; ok {
			logger.Debug("ignoring-tracked-file", lager.Data{"path": path})
			continue
		}
		logger.Debug("removing-file", lager.Data{"path": path})
		err = os.RemoveAll(path)
		if err != nil {
			logger.Debug("error-removing-file", lager.Data{"path": path})
			return err
		}
	}

	// free some disk space in case the maxSizeInBytes was changed
	logger.Debug("making-room")
	c.cache.makeRoom(logger, 0, "")

	logger.Debug("making-directory", lager.Data{"uncachedPath": c.uncachedPath})
	if err := os.Mkdir(c.uncachedPath, 0755); err != nil {
		logger.Debug("failed-to-make-directory", lager.Data{"uncachedPath": c.uncachedPath})
		return err
	}

	return err
}

func (c *cachedDownloader) CloseDirectory(logger lager.Logger, cacheKey, directoryPath string) error {
	logger = logger.Session("close-directory", lager.Data{"cache-key": cacheKey, "directory-path": directoryPath})
	logger.Debug("close-directory-started")
	defer logger.Debug("close-directory-ended")

	cacheKey = fmt.Sprintf("%x", md5.Sum([]byte(cacheKey)))
	return c.cache.CloseDirectory(logger, cacheKey, directoryPath)
}

func (c *cachedDownloader) Fetch(logger lager.Logger, url *url.URL, cacheKey string, checksum ChecksumInfoType, cancelChan <-chan struct{}) (io.ReadCloser, int64, error) {
	logger = logger.Session("fetch", lager.Data{"url": url, "cache-key": cacheKey})
	logger.Debug("fetch-started")
	defer logger.Debug("fetch-ended")

	if cacheKey == "" {
		logger.Debug("uncached-fetch")
		return c.fetchUncachedFile(logger, url, checksum, cancelChan)
	}

	cacheKey = fmt.Sprintf("%x", md5.Sum([]byte(cacheKey)))
	logger.Debug("cached-fetch")
	return c.fetchCachedFile(logger, url, cacheKey, checksum, cancelChan)
}

func (c *cachedDownloader) fetchUncachedFile(logger lager.Logger, url *url.URL, checksum ChecksumInfoType, cancelChan <-chan struct{}) (*CachedFile, int64, error) {
	logger = logger.Session("fetch-uncached-file", lager.Data{"url": url})
	logger.Debug("fetch-uncached-file-started")
	defer logger.Debug("fetch-uncached-file-ended")

	logger.Debug("populating-cache")
	download, _, size, err := c.populateCache(logger, url, "uncached", CachingInfoType{}, checksum, c.transformer, cancelChan)
	if err != nil {
		logger.Debug("error-populating-cache", lager.Data{"error": err})
		return nil, 0, err
	}

	logger.Debug("generating-file-closer")
	file, err := tempFileRemoveOnClose(download.path)
	return file, size, err
}

func (c *cachedDownloader) fetchCachedFile(logger lager.Logger, url *url.URL, cacheKey string, checksum ChecksumInfoType, cancelChan <-chan struct{}) (*CachedFile, int64, error) {
	logger = logger.Session("fetch-cached-file", lager.Data{"url": url, "cache-key": cacheKey})
	logger.Debug("fetch-cached-file-started")
	defer logger.Debug("fetch-cached-file-ended")

	logger.Debug("generating-rate-limiter")
	rateLimiter, err := c.acquireLimiter(logger, cacheKey, cancelChan)
	if err != nil {
		logger.Debug("error-generating-rate-limiter", lager.Data{"error": err})
		return nil, 0, err
	}
	defer c.releaseLimiter(cacheKey, rateLimiter)

	// lookup cache entry
	logger.Debug("getting-cachekey", lager.Data{"cache-key": cacheKey})
	currentReader, currentCachingInfo, getErr := c.cache.Get(logger, cacheKey)

	// download (short circuits if endpoint respects etag/etc.)
	logger.Debug("populating-cache")
	download, cacheIsWarm, size, err := c.populateCache(logger, url, cacheKey, currentCachingInfo, checksum, c.transformer, cancelChan)
	if err != nil {
		logger.Debug("error-populating-cache", lager.Data{"error": err})
		if currentReader != nil {
			logger.Debug("closing-current-reader")
			currentReader.Close()
		}
		return nil, 0, err
	}

	// nothing had to be downloaded; return the cached entry
	logger.Debug("check-if-cache-is-warm", lager.Data{"cache-is-warm": cacheIsWarm})
	if cacheIsWarm {
		logger.Info("file-found-in-cache", lager.Data{"cache_key": cacheKey, "size": size})
		return currentReader, 0, getErr
	}

	// current cache is not fresh; disregard it
	if currentReader != nil {
		logger.Debug("closing-current-reader")
		currentReader.Close()
	}

	// fetch uncached data
	var newReader *CachedFile
	logger.Debug("check-file-cachablity", lager.Data{"cache-key": cacheKey, "path": download.path, "cacheable": download.cachingInfo.isCacheable()})
	if download.cachingInfo.isCacheable() {
		logger.Debug("cachable_add-cache-entry", lager.Data{"cache-key": cacheKey, "path": download.path})
		newReader, err = c.cache.Add(logger, cacheKey, download.path, download.size, download.cachingInfo)
	} else {
		logger.Debug("uncacheable_remove-cache-key", lager.Data{"cache-key": cacheKey, "path": download.path})
		c.cache.Remove(logger, cacheKey)
		newReader, err = tempFileRemoveOnClose(download.path)
	}

	// return newly fetched file
	return newReader, size, err
}

func (c *cachedDownloader) FetchAsDirectory(logger lager.Logger, url *url.URL, cacheKey string, checksum ChecksumInfoType, cancelChan <-chan struct{}) (string, int64, error) {
	logger = logger.Session("fetch-as-directory", lager.Data{"url": url, "cache-key": cacheKey})
	logger.Debug("fetch-as-directory-started")
	defer logger.Debug("fetch-as-directory-ended")

	logger.Debug("check-missing-cache-key", lager.Data{"cache-key": cacheKey})
	if cacheKey == "" {
		logger.Debug("missing-cache-key")
		return "", 0, MissingCacheKeyErr
	}

	cacheKey = fmt.Sprintf("%x", md5.Sum([]byte(cacheKey)))
	return c.fetchCachedDirectory(logger, url, cacheKey, checksum, cancelChan)
}

func (c *cachedDownloader) fetchCachedDirectory(logger lager.Logger, url *url.URL, cacheKey string, checksum ChecksumInfoType, cancelChan <-chan struct{}) (string, int64, error) {
	logger = logger.Session("fetch-cached-directory", lager.Data{"url": url, "cache-key": cacheKey})
	logger.Debug("fetch-cached-directory-started")
	defer logger.Debug("fetch-cached-directory-ended")

	logger.Debug("generating-rate-limiter")
	rateLimiter, err := c.acquireLimiter(logger, cacheKey, cancelChan)
	if err != nil {
		logger.Debug("error-generating-rate-limiter", lager.Data{"error": err})
		return "", 0, err
	}
	defer c.releaseLimiter(cacheKey, rateLimiter)

	// lookup cache entry
	currentDirectory, currentCachingInfo, getErr := c.cache.GetDirectory(logger, cacheKey)

	// download (short circuits if endpoint respects etag/etc.)
	logger.Debug("populate-cache-entry", lager.Data{"url": url, "cache-key": cacheKey})
	download, cacheIsWarm, size, err := c.populateCache(logger, url, cacheKey, currentCachingInfo, checksum, TarTransform, cancelChan)
	if err != nil {
		logger.Debug("error-download-cache-entry", lager.Data{"url": url, "cache-key": cacheKey, "current-directory": currentDirectory})
		if currentDirectory != "" {
			c.cache.CloseDirectory(logger, cacheKey, currentDirectory)
		}
		return "", 0, err
	}

	// nothing had to be downloaded; return the cached entry
	if cacheIsWarm {
		logger.Info("directory-found-in-cache", lager.Data{"cache-key": cacheKey, "size": size})
		return currentDirectory, 0, getErr
	}

	// current cache is not fresh; disregard it
	if currentDirectory != "" {
		logger.Debug("closing-current-directory", lager.Data{"current-directory": currentDirectory})
		c.cache.CloseDirectory(logger, cacheKey, currentDirectory)
	}

	// fetch uncached data
	var newDirectory string
	if download.cachingInfo.isCacheable() {
		logger.Debug("adding-cachable-directory", lager.Data{"cache-key": cacheKey, "download-path": download.path})
		newDirectory, err = c.cache.AddDirectory(logger, cacheKey, download.path, download.size, download.cachingInfo)
		// return newly fetched directory
		return newDirectory, size, err
	}

	logger.Debug("removing-cache-key", lager.Data{"cache-key": cacheKey})
	c.cache.Remove(logger, cacheKey)
	return "", 0, MissingCacheHeadersErr
}

func (c *cachedDownloader) acquireLimiter(logger lager.Logger, cacheKey string, cancelChan <-chan struct{}) (chan struct{}, error) {
	logger = logger.Session("aquire-limiter", lager.Data{"cache-key": cacheKey})
	logger.Debug("aquire-limiter-started")
	defer logger.Debug("aquire-limiter-ended")

	startTime := time.Now()
	defer func() {
		logger.Info("completed", lager.Data{"duration-ns": time.Now().Sub(startTime)})
	}()

	for {
		c.lock.Lock()
		rateLimiter := c.inProgress[cacheKey]
		if rateLimiter == nil {
			rateLimiter = make(chan struct{})
			c.inProgress[cacheKey] = rateLimiter
			logger.Debug("rate-limiter-locked")
			c.lock.Unlock()
			return rateLimiter, nil
		}
		c.lock.Unlock()

		select {
		case <-rateLimiter:
			logger.Debug("rate-limiter-unlocked")
		case <-cancelChan:
			logger.Debug("cancellation-received")
			return nil, NewDownloadCancelledError("acquire-limiter", time.Now().Sub(startTime), NoBytesReceived, nil)
		}
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

// Currently populateCache takes a transformer due to the fact that a fetchCachedDirectory
// uses only a TarTransformer, which overwrites what is currently set. This way one transformer
// can be used to call Fetch and FetchAsDirectory
func (c *cachedDownloader) populateCache(
	logger lager.Logger,
	url *url.URL,
	name string,
	cachingInfo CachingInfoType,
	checksum ChecksumInfoType,
	transformer CacheTransformer,
	cancelChan <-chan struct{},
) (download, bool, int64, error) {
	logger = logger.Session("populate-cache", lager.Data{"url": url, "name": name})
	logger.Debug("populdate-cache-started")
	defer logger.Debug("populdate-cache-ended")

	filename, cachingInfo, err := c.downloader.Download(logger, url, func() (*os.File, error) {
		return ioutil.TempFile(c.uncachedPath, name+"-")
	}, cachingInfo, checksum, cancelChan)
	if err != nil {
		return download{}, false, 0, err
	}

	if filename == "" {
		return download{}, true, 0, nil
	}

	fileInfo, err := os.Stat(filename)
	if err != nil {
		return download{}, false, 0, err
	}
	defer os.Remove(filename)

	logger.Debug("create-temp-file", lager.Data{"uncached-path": c.uncachedPath})
	cachedFile, err := ioutil.TempFile(c.uncachedPath, "transformed")
	if err != nil {
		return download{}, false, 0, err
	}

	err = cachedFile.Close()
	if err != nil {
		return download{}, false, 0, err
	}

	logger.Debug("tranform-cached-file", lager.Data{"source": filename, "desintation": cachedFile.Name()})
	cachedSize, err := transformer(filename, cachedFile.Name())
	if err != nil {
		return download{}, false, 0, err
	}

	return download{
		path:        cachedFile.Name(),
		size:        cachedSize,
		cachingInfo: cachingInfo,
	}, false, fileInfo.Size(), nil
}
