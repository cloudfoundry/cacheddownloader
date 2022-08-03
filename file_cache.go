package cacheddownloader

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"code.cloudfoundry.org/archiver/compressor"
	"code.cloudfoundry.org/archiver/extractor"
	"code.cloudfoundry.org/lager"
)

var (
	lock                   = &sync.Mutex{}
	EntryNotFound          = errors.New("Entry Not Found")
	AlreadyClosed          = errors.New("Already closed directory")
	MissingCacheKeyErr     = errors.New("Not cacheable directory: cache key is missing")
	MissingCacheHeadersErr = errors.New("Not cacheable directory: ETag and Last-Modified were missing from response")
)

type FileCache struct {
	Logger         lager.Logger
	CachedPath     string
	maxSizeInBytes int64
	Entries        map[string]*FileCacheEntry
	OldEntries     map[string]*FileCacheEntry
	Seq            uint64
}

type FileCacheEntry struct {
	Logger                lager.Logger
	Size                  int64
	Access                time.Time
	CachingInfo           CachingInfoType
	FilePath              string
	ExpandedDirectoryPath string
	directoryInUseCount   int
	fileInUseCount        int
}

func NewCache(dir string, maxSizeInBytes int64) *FileCache {
	return &FileCache{
		CachedPath:     dir,
		maxSizeInBytes: maxSizeInBytes,
		Entries:        map[string]*FileCacheEntry{},
		OldEntries:     map[string]*FileCacheEntry{},
		Seq:            0,
	}
}

func newFileCacheEntry(cachePath string, size int64, cachingInfo CachingInfoType) *FileCacheEntry {
	return &FileCacheEntry{
		Size:                  size,
		FilePath:              cachePath,
		Access:                time.Now(),
		CachingInfo:           cachingInfo,
		ExpandedDirectoryPath: "",
	}
}

func (e *FileCacheEntry) inUse(logger lager.Logger) bool {
	logger = logger.Session("file-cache-entry.in-use")
	logger.Debug("inUse", lager.Data{"directoryInUseCount": e.directoryInUseCount, "fileInUseCount": e.fileInUseCount})
	return e.directoryInUseCount > 0 || e.fileInUseCount > 0
}

func (e *FileCacheEntry) decrementUse(logger lager.Logger) {
	logger = logger.Session("file-cache-entry.decrement-use")
	e.decrementFileInUseCount(logger)
	e.decrementDirectoryInUseCount(logger)
}

func (e *FileCacheEntry) incrementDirectoryInUseCount(logger lager.Logger) {
	logger = logger.Session("file-cache-entry.increment-directory-in-use-count")
	logger.Debug("incrementDirectoryInUseCount", lager.Data{"before-increment-directoryInUseCount": e.directoryInUseCount})
	e.directoryInUseCount++
}

func (e *FileCacheEntry) decrementDirectoryInUseCount(logger lager.Logger) {
	logger = logger.Session("file-cache-entry.decrement-directory-in-use-count")
	logger.Debug("decrementing-count", lager.Data{"directory-in-use-count": e.directoryInUseCount})
	e.directoryInUseCount--

	// Delete the directory if the tarball is the only asset
	// being used or if the directory has been removed (in use count -1)
	logger.Debug("check-for-potential-directory-delete", lager.Data{"file-in-use-count": e.fileInUseCount, "directory-in-use-count": e.directoryInUseCount, "size": e.Size})
	if e.directoryInUseCount < 0 || (e.directoryInUseCount == 0 && e.fileInUseCount > 0) {
		logger.Debug("delete-directory")
		err := os.RemoveAll(e.ExpandedDirectoryPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Unable to delete cached directory", err)
		}
		e.ExpandedDirectoryPath = ""

		if e.fileInUseCount > 0 {
			e.Size = e.Size / 2
		}
	}
}

func (e *FileCacheEntry) incrementFileInUseCount(logger lager.Logger) {
	logger = logger.Session("file-cache-entry.increment-file-in-use-count")
	logger.Debug("incrementFileInUseCount", lager.Data{"before-increment-fileInUseCount": e.fileInUseCount})
	e.fileInUseCount++
}

func (e *FileCacheEntry) decrementFileInUseCount(logger lager.Logger) {
	logger = logger.Session("file-cache-entry.decrement-file-in-use-count")
	e.fileInUseCount--

	// Delete the file if the file is not being used and there is
	// a directory or if the file has been removed (in use count -1)
	logger.Debug("check-for-potential-file-delete", lager.Data{"file-in-use-count": e.fileInUseCount, "directory-in-use-count": e.directoryInUseCount, "size": e.Size})
	if e.fileInUseCount < 0 || (e.fileInUseCount == 0 && e.directoryInUseCount > 0) {
		logger.Debug("delete-file")
		err := os.RemoveAll(e.FilePath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Unable to delete cached file", err)
		}

		if e.directoryInUseCount > 0 {
			e.Size = e.Size / 2
		}
	}
}

func (e *FileCacheEntry) fileDoesNotExist() bool {
	_, err := os.Stat(e.FilePath)
	return os.IsNotExist(err)
}

func (e *FileCacheEntry) dirDoesNotExist() bool {
	if e.ExpandedDirectoryPath == "" {
		return true
	}
	_, err := os.Stat(e.ExpandedDirectoryPath)
	return os.IsNotExist(err)
}

// Can we change this to be an io.ReadCloser return
func (e *FileCacheEntry) readCloser(logger lager.Logger) (*CachedFile, error) {
	logger = logger.Session("file-cache-entry.read-closer")
	var f *os.File
	var err error

	if e.fileDoesNotExist() {
		logger.Debug("creating-file", lager.Data{"file": e.FilePath})

		f, err = os.Create(e.FilePath)
		if err != nil {
			return nil, err
		}

		logger.Debug("write-tar", lager.Data{"directory-path": e.ExpandedDirectoryPath, "file": f})
		err = compressor.WriteTar(e.ExpandedDirectoryPath+"/", f)
		if err != nil {
			return nil, err
		}

		// If the directory is not used remove it
		if e.directoryInUseCount == 0 {
			logger.Debug("directory-not-in-use", lager.Data{"directory-use-count": e.directoryInUseCount, "directory-path": e.ExpandedDirectoryPath})

			err = os.RemoveAll(e.ExpandedDirectoryPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Unable to remove cached directory", err)
			}
		} else {
			logger.Debug("expanding-size", lager.Data{"old-size": e.Size, "new-size": e.Size * 2})
			// Double the size to account for both assets
			e.Size = e.Size * 2
		}
	} else {
		logger.Debug("file-already-exists", lager.Data{"file": e.FilePath})

		f, err = os.Open(e.FilePath)
		if err != nil {
			return nil, err
		}
	}

	e.incrementFileInUseCount(logger)

	logger.Debug("rewind-file", lager.Data{"file": e.FilePath})
	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		return nil, err
	}

	readCloser := NewFileCloser(f, func(filePath string) {
		lock.Lock()
		e.decrementFileInUseCount(logger)
		lock.Unlock()
	})

	return readCloser, nil
}

func (e *FileCacheEntry) expandedDirectory(logger lager.Logger) (string, error) {
	logger = logger.Session("file-cache-entry.expanded-directory")

	// if it has not been extracted before expand it!
	if e.dirDoesNotExist() {
		logger.Debug("directory-does-not-exist")

		e.ExpandedDirectoryPath = e.FilePath + ".d"
		err := extractTarToDirectory(logger, e.FilePath, e.ExpandedDirectoryPath)
		if err != nil {
			return "", err
		}

		// If the file is not in use, we can delete it
		if e.fileInUseCount == 0 {
			logger.Debug("deleting-file", lager.Data{"file-in-use-count": e.fileInUseCount, "file-path": e.FilePath})

			err = os.RemoveAll(e.FilePath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Unable to delete the cached file", err)
			}
		}
	}

	e.incrementDirectoryInUseCount(logger)

	return e.ExpandedDirectoryPath, nil
}

func (c *FileCache) CloseDirectory(logger lager.Logger, cacheKey, dirPath string) error {
	logger = logger.Session("file-cache.close-directory", lager.Data{"cache-key": cacheKey, "dir_path": dirPath})
	lock.Lock()
	defer lock.Unlock()

	logger.Info("starting")
	defer logger.Info("finished")

	entry := c.Entries[cacheKey]
	if entry != nil && entry.ExpandedDirectoryPath == dirPath {
		logger.Debug("entry-detected")
		logger.Debug("does-old-entries-contain-this", lager.Data{"possible-old-entry": c.OldEntries[cacheKey+dirPath]})
		if entry.directoryInUseCount == 0 {
			// We don't think anybody is using this so throw an error
			return AlreadyClosed
		}

		entry.decrementDirectoryInUseCount(logger)
		return nil
	}

	// Key didn't match anything in the current cache, so
	// check and clean up old entries
	logger.Debug("add-old-entries", lager.Data{"cache-key": cacheKey, "dir_path": dirPath})
	entry = c.OldEntries[cacheKey+dirPath]
	if entry == nil {
		return EntryNotFound
	}

	entry.decrementDirectoryInUseCount(logger)
	if !entry.inUse(logger) {
		// done with this old entry, so clean it up
		logger.Debug("removing-old-entries", lager.Data{"cacheKey": cacheKey + dirPath})
		delete(c.OldEntries, cacheKey+dirPath)
	}
	return nil
}

func (c *FileCache) Add(logger lager.Logger, cacheKey, sourcePath string, size int64, cachingInfo CachingInfoType) (*CachedFile, error) {
	logger = logger.Session("file-cache.add", lager.Data{"cache-key": cacheKey, "source_path": sourcePath, "size": size})
	lock.Lock()
	defer lock.Unlock()

	logger.Info("starting")
	defer logger.Info("finished")

	oldEntry := c.Entries[cacheKey]

	logger.Debug("make-room")
	c.makeRoom(logger, size, "")

	c.Seq++
	uniqueName := fmt.Sprintf("%s-%d-%d", cacheKey, time.Now().UnixNano(), c.Seq)
	cachePath := filepath.Join(c.CachedPath, uniqueName)

	logger.Debug("move-file", lager.Data{"source_path": sourcePath, "cache_path": cachePath})
	err := os.Rename(sourcePath, cachePath)
	if err != nil {
		return nil, err
	}

	logger.Debug("create-new-file-cache-entry", lager.Data{"cache_path": cachePath, "size": size, "caching_info": cachingInfo})
	newEntry := newFileCacheEntry(cachePath, size, cachingInfo)
	c.Entries[cacheKey] = newEntry

	logger.Debug("clean-old-entries-if-present")
	if oldEntry != nil {
		logger.Debug("removing-old-entries", lager.Data{"oldEntry": oldEntry})
		oldEntry.decrementUse(logger)
		c.updateOldEntries(logger, cacheKey, oldEntry)
	}
	return newEntry.readCloser(logger)
}

func (c *FileCache) AddDirectory(logger lager.Logger, cacheKey, sourcePath string, size int64, cachingInfo CachingInfoType) (string, error) {
	logger = logger.Session("file-cache.add-directory", lager.Data{"cache-key": cacheKey, "source_path": sourcePath, "size": size})
	lock.Lock()
	defer lock.Unlock()

	logger.Info("starting")
	defer logger.Info("finished")

	oldEntry := c.Entries[cacheKey]

	logger.Debug("make-room")
	c.makeRoom(logger, size, "")

	c.Seq++
	uniqueName := fmt.Sprintf("%s-%d-%d", cacheKey, time.Now().UnixNano(), c.Seq)
	cachePath := filepath.Join(c.CachedPath, uniqueName)

	logger.Debug("move-file", lager.Data{"source_path": sourcePath, "cache_path": cachePath})
	err := os.Rename(sourcePath, cachePath)
	if err != nil {
		return "", err
	}

	logger.Debug("create-new-file-cache-entry", lager.Data{"cache_path": cachePath, "size": size, "caching_info": cachingInfo})
	newEntry := newFileCacheEntry(cachePath, size, cachingInfo)
	c.Entries[cacheKey] = newEntry

	logger.Debug("clean-old-entries-if-present")
	if oldEntry != nil {
		logger.Debug("removing-old-entries", lager.Data{"oldEntry": oldEntry})
		oldEntry.decrementUse(logger)
		c.updateOldEntries(logger, cacheKey, oldEntry)
	}
	return newEntry.expandedDirectory(logger)
}

func (c *FileCache) Get(logger lager.Logger, cacheKey string) (*CachedFile, CachingInfoType, error) {
	logger = logger.Session("file-cache.get", lager.Data{"cache-key": cacheKey})
	lock.Lock()
	defer lock.Unlock()

	logger.Info("starting")
	defer logger.Info("finished")

	entry := c.Entries[cacheKey]
	if entry == nil {
		logger.Debug("no-cache-entry-found")
		return nil, CachingInfoType{}, EntryNotFound
	}

	logger.Debug("check-if-file-exists")
	if entry.fileDoesNotExist() {
		logger.Debug("file-does-not-exist")
		c.makeRoom(logger, entry.Size, cacheKey)
	}

	entry.Access = time.Now()

	logger.Debug("create-file-handle")
	readCloser, err := entry.readCloser(logger)
	if err != nil {
		logger.Debug("unable-to-generate-file-handle")
		return nil, CachingInfoType{}, err
	}

	return readCloser, entry.CachingInfo, nil
}

func (c *FileCache) GetDirectory(logger lager.Logger, cacheKey string) (string, CachingInfoType, error) {
	logger = logger.Session("file-cache.get-directory", lager.Data{"cache-key": cacheKey})
	lock.Lock()
	defer lock.Unlock()

	logger.Info("starting")
	defer logger.Info("finished")

	entry := c.Entries[cacheKey]
	if entry == nil {
		return "", CachingInfoType{}, EntryNotFound
	}

	// Was it expanded before
	logger.Debug("check-if-directory-exists")
	if entry.dirDoesNotExist() {
		// Do we have enough room to double the size?
		c.makeRoom(logger, entry.Size, cacheKey)
		entry.Size = entry.Size * 2
	}

	entry.Access = time.Now()
	logger.Debug("expand-directory")
	dir, err := entry.expandedDirectory(logger)
	if err != nil {
		return "", CachingInfoType{}, err
	}

	return dir, entry.CachingInfo, nil
}

func (c *FileCache) Remove(logger lager.Logger, cacheKey string) {
	logger = logger.Session("file-cache.remove", lager.Data{"cache-key": cacheKey})

	lock.Lock()
	logger.Info("starting")
	c.remove(logger, cacheKey)
	lock.Unlock()
	logger.Info("finished")
}

func (c *FileCache) remove(logger lager.Logger, cacheKey string) {
	logger = logger.Session("file-cache.inner-remove", lager.Data{"cache-key": cacheKey})
	entry := c.Entries[cacheKey]
	if entry != nil {
		logger.Debug("entry-is-present", lager.Data{"entry": entry})
		entry.decrementUse(logger)
		c.updateOldEntries(logger, cacheKey, entry)
		delete(c.Entries, cacheKey)
	}
}

func (c *FileCache) updateOldEntries(logger lager.Logger, cacheKey string, entry *FileCacheEntry) {
	logger = logger.Session("update-old-entries", lager.Data{"cache-key": cacheKey, "entry": entry})

	if entry == nil {
		logger.Debug("provided-entry-is-nil")
		return
	}

	if !entry.inUse(logger) && entry.ExpandedDirectoryPath != "" {
		// put it in the oldEntries Cache since somebody may still be using the directory
		logger.Debug("saving-old-entry", lager.Data{"entry": entry, "in-use": entry.inUse(logger), "expanded-directory-path": entry.ExpandedDirectoryPath, "cache-key": cacheKey})
		c.OldEntries[cacheKey+entry.ExpandedDirectoryPath] = entry
	} else {
		// We need to remove it from oldEntries
		lookupKey := cacheKey + entry.ExpandedDirectoryPath
		logger.Debug("remove-old-entry", lager.Data{"entry": entry, "in-use": entry.inUse(logger), "expanded-directory-path": entry.ExpandedDirectoryPath, "cache-key": cacheKey, "key-is-present": c.OldEntries[lookupKey]})
		delete(c.OldEntries, lookupKey)
	}
}

func (c *FileCache) makeRoom(logger lager.Logger, size int64, excludedCacheKey string) {
	logger = logger.Session("make-room", lager.Data{"size": size, "excludedCacheKey": excludedCacheKey})

	usedSpace := c.usedSpace(logger)

	logger.Debug("begin-search-for-oldest-entry", lager.Data{"max-size-allowed": c.maxSizeInBytes, "used-space": usedSpace, "desired-additional-space": size})
	for c.maxSizeInBytes < usedSpace+size {
		var oldestEntry *FileCacheEntry
		oldestAccessTime, oldestCacheKey := maxTime(), ""
		logger.Debug("iterate-over-entries")
		for ck, f := range c.Entries {
			logger.Debug("considering-entry", lager.Data{"cache-key": ck, "entry": f})

			if f.Access.Before(oldestAccessTime) && ck != excludedCacheKey && !f.inUse(logger) {
				oldestAccessTime = f.Access
				oldestEntry = f
				oldestCacheKey = ck
				logger.Debug("new-candidate-for-removal", lager.Data{"cache-key": ck, "entry": f, "updated-oldest-access-time": oldestAccessTime})
			}
		}

		if oldestEntry == nil {
			logger.Debug("no-candidates-for-removal")
			// could not find anything we could remove
			return
		}

		usedSpace -= oldestEntry.Size
		logger.Debug("removing-entry", lager.Data{"entry": oldestEntry, "cache-key": oldestCacheKey, "updated-used-space": usedSpace})
		c.remove(logger, oldestCacheKey)
	}

	return
}

func (c *FileCache) usedSpace(logger lager.Logger) int64 {
	space := int64(0)
	for _, f := range c.Entries {
		space += f.Size
	}
	return space
}

func extractTarToDirectory(logger lager.Logger, sourcePath, destinationDir string) error {
	logger = logger.Session("extract-tar-to-directory")
	logger.Debug("extract-tar-to-directory", lager.Data{"source-path": sourcePath, "destination-dir": destinationDir})

	e := extractor.NewTar()
	return e.Extract(sourcePath, destinationDir)
}

func maxTime() time.Time {
	unixToInternal := int64((1969*365 + 1969/4 - 1969/100 + 1969/400) * 24 * 60 * 60)
	return time.Unix(1<<63-1-unixToInternal, 999999999)
}
