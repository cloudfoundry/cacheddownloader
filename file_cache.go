package cacheddownloader

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var (
	lock           = &sync.Mutex{}
	EntryNotFound  = errors.New("Entry Not Found")
	NotEnoughSpace = errors.New("No space available")
	AlreadyClosed  = errors.New("Already closed directory")
	NotCacheable   = errors.New("Not cacheable directory")
)

type FileCache struct {
	cachedPath     string
	maxSizeInBytes int64
	entries        map[string]*fileCacheEntry
	oldEntries     map[string]*fileCacheEntry
	seq            uint64
}

type fileCacheEntry struct {
	size                  int64
	access                time.Time
	cachingInfo           CachingInfoType
	filePath              string
	expandedDirectoryPath string
	inuseCount            int
}

func NewCache(dir string, maxSizeInBytes int64) *FileCache {
	return &FileCache{
		cachedPath:     dir,
		maxSizeInBytes: maxSizeInBytes,
		entries:        map[string]*fileCacheEntry{},
		oldEntries:     map[string]*fileCacheEntry{},
		seq:            0,
	}
}

func newFileCacheEntry(cachePath string, size int64, cachingInfo CachingInfoType) *fileCacheEntry {
	return &fileCacheEntry{
		size:                  size,
		filePath:              cachePath,
		access:                time.Now(),
		cachingInfo:           cachingInfo,
		expandedDirectoryPath: "",
		inuseCount:            1,
	}
}

func (e *fileCacheEntry) incrementUse() {
	e.inuseCount++
}

func (e *fileCacheEntry) decrementUse() {
	e.inuseCount--
	count := e.inuseCount

	if count == 0 {
		err := os.RemoveAll(e.filePath)
		if err != nil {
			fmt.Errorf("Unable to delete cached file", err)
		}

		// if there is a directory we need to remove it as well
		os.RemoveAll(e.expandedDirectoryPath)
		if err != nil {
			fmt.Errorf("Unable to delete cached directory", err)
		}
	}
}

// Can we change this to be an io.ReadCloser return
func (e *fileCacheEntry) readCloser() (*CachedFile, error) {
	f, err := os.Open(e.filePath)
	if err != nil {
		return nil, err
	}

	e.incrementUse()
	readCloser := NewFileCloser(f, func(filePath string) {
		lock.Lock()
		e.decrementUse()
		lock.Unlock()
	})

	return readCloser, nil
}

func (e *fileCacheEntry) expandedDirectory() (string, error) {
	e.incrementUse()

	// if it has not been extracted before expand it!
	if e.expandedDirectoryPath == "" {
		e.expandedDirectoryPath = e.filePath + ".d"
		err := extractTarToDirectory(e.filePath, e.expandedDirectoryPath)
		if err != nil {
			return "", err
		}
	}

	return e.expandedDirectoryPath, nil
}

func (c *FileCache) CloseDirectory(cacheKey, dirPath string) error {
	lock.Lock()
	defer lock.Unlock()

	entry := c.entries[cacheKey]
	if entry != nil && entry.expandedDirectoryPath == dirPath {
		if entry.inuseCount == 1 {
			// We don't think anybody is using this so throw an error
			return AlreadyClosed
		}

		entry.decrementUse()
		return nil
	}

	// Key didn't match anything in the current cache, so
	// check and clean up old entries
	entry = c.oldEntries[cacheKey+dirPath]
	if entry == nil {
		return EntryNotFound
	}

	entry.decrementUse()
	if entry.inuseCount == 0 {
		// done with this old entry, so clean it up
		delete(c.oldEntries, cacheKey+dirPath)
	}
	return nil
}

func (c *FileCache) Add(cacheKey, sourcePath string, size int64, cachingInfo CachingInfoType) (*CachedFile, error) {
	lock.Lock()
	defer lock.Unlock()

	oldEntry := c.entries[cacheKey]

	if !c.makeRoom(size, "") {
		//file does not fit in cache...
		return nil, NotEnoughSpace
	}

	c.seq++
	uniqueName := fmt.Sprintf("%s-%d-%d", cacheKey, time.Now().UnixNano(), c.seq)
	cachePath := filepath.Join(c.cachedPath, uniqueName)

	err := os.Rename(sourcePath, cachePath)
	if err != nil {
		return nil, err
	}

	newEntry := newFileCacheEntry(cachePath, size, cachingInfo)
	c.entries[cacheKey] = newEntry
	if oldEntry != nil {
		oldEntry.decrementUse()
		c.updateOldEntries(cacheKey, oldEntry)
	}
	return newEntry.readCloser()
}

func (c *FileCache) AddDirectory(cacheKey, sourcePath string, size int64, cachingInfo CachingInfoType) (string, error) {
	lock.Lock()
	defer lock.Unlock()

	// double the size when expanding to directories
	newSize := 2 * size

	oldEntry := c.entries[cacheKey]

	if !c.makeRoom(newSize, "") {
		//file does not fit in cache...
		return "", NotEnoughSpace
	}

	c.seq++
	uniqueName := fmt.Sprintf("%s-%d-%d", cacheKey, time.Now().UnixNano(), c.seq)
	cachePath := filepath.Join(c.cachedPath, uniqueName)

	err := os.Rename(sourcePath, cachePath)
	if err != nil {
		return "", err
	}
	newEntry := newFileCacheEntry(cachePath, newSize, cachingInfo)
	c.entries[cacheKey] = newEntry
	if oldEntry != nil {
		oldEntry.decrementUse()
		c.updateOldEntries(cacheKey, oldEntry)
	}
	return newEntry.expandedDirectory()
}

func (c *FileCache) Get(cacheKey string) (*CachedFile, CachingInfoType, error) {
	lock.Lock()
	defer lock.Unlock()

	entry := c.entries[cacheKey]
	if entry == nil {
		return nil, CachingInfoType{}, EntryNotFound
	}

	entry.access = time.Now()
	readCloser, err := entry.readCloser()
	if err != nil {
		return nil, CachingInfoType{}, err
	}

	return readCloser, entry.cachingInfo, nil
}

func (c *FileCache) GetDirectory(cacheKey string) (string, CachingInfoType, error) {
	lock.Lock()
	defer lock.Unlock()

	entry := c.entries[cacheKey]
	if entry == nil {
		return "", CachingInfoType{}, EntryNotFound
	}

	// Was it expanded before
	if entry.expandedDirectoryPath == "" {
		// Do we have enough room to double the size?
		if !c.makeRoom(entry.size, cacheKey) {
			//file does not fit in cache...
			return "", CachingInfoType{}, NotEnoughSpace
		} else {
			entry.size = entry.size * 2
		}
	}

	entry.access = time.Now()
	dir, err := entry.expandedDirectory()
	if err != nil {
		return "", CachingInfoType{}, err
	}

	return dir, entry.cachingInfo, nil
}

func (c *FileCache) Remove(cacheKey string) {
	lock.Lock()
	c.remove(cacheKey)
	lock.Unlock()
}

func (c *FileCache) remove(cacheKey string) {
	entry := c.entries[cacheKey]
	if entry != nil {
		entry.decrementUse()
		c.updateOldEntries(cacheKey, entry)
		delete(c.entries, cacheKey)
	}
}

func (c *FileCache) updateOldEntries(cacheKey string, entry *fileCacheEntry) {
	if entry != nil {
		if entry.inuseCount > 0 && entry.expandedDirectoryPath != "" {
			// put it in the oldEntries Cache since somebody may still be using the directory
			c.oldEntries[cacheKey+entry.expandedDirectoryPath] = entry
		} else {
			// We need to remove it from oldEntries
			delete(c.oldEntries, cacheKey+entry.expandedDirectoryPath)
		}
	}
}

func (c *FileCache) makeRoom(size int64, excludedCacheKey string) bool {
	if size > c.maxSizeInBytes {
		return false
	}

	usedSpace := c.usedSpace()
	for c.maxSizeInBytes < usedSpace+size {
		var oldestEntry *fileCacheEntry
		oldestAccessTime, oldestCacheKey := time.Now(), ""
		for ck, f := range c.entries {
			if f.access.Before(oldestAccessTime) && ck != excludedCacheKey {
				oldestAccessTime = f.access
				oldestEntry = f
				oldestCacheKey = ck
			}
		}

		if oldestEntry == nil {
			// could not find anything we could remove
			return false
		}

		usedSpace -= oldestEntry.size
		c.remove(oldestCacheKey)
	}

	return true
}

func (c *FileCache) usedSpace() int64 {
	space := int64(0)
	for _, f := range c.entries {
		space += f.size
	}
	return space
}

func extractTarToDirectory(sourcePath, destinationDir string) error {
	_, err := os.Stat(destinationDir)
	if err != nil && err.(*os.PathError).Err != syscall.ENOENT {
		return err
	}

	file, err := os.Open(sourcePath)
	if err != nil {
		return err
	}

	defer file.Close()

	var fileReader io.ReadCloser = file

	// Make the target directory
	err = os.MkdirAll(destinationDir, 0777)
	if err != nil {
		return err
	}

	tarBallReader := tar.NewReader(fileReader)
	// Extracting tarred files
	for {
		header, err := tarBallReader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// get the individual filename and extract to the current directory
		filename := header.Name

		switch header.Typeflag {
		case tar.TypeDir:
			// handle directory
			fullpath := filepath.Join(destinationDir, filename)
			err = os.MkdirAll(fullpath, os.FileMode(header.Mode))

			if err != nil {
				return err
			}

		default:
			// handle normal file
			fullpath := filepath.Join(destinationDir, filename)

			err := os.MkdirAll(filepath.Dir(fullpath), 0777)
			if err != nil {
				return err
			}

			writer, err := os.Create(fullpath)

			if err != nil {
				return err
			}

			io.Copy(writer, tarBallReader)

			err = os.Chmod(fullpath, os.FileMode(header.Mode))

			if err != nil {
				return err
			}

			writer.Close()

		}
	}
	return nil
}
