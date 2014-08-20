// +build windows

package cacheddownloader

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type cacheFileWrapper struct {
	file         *os.File
	filePath     string
	cacheManager *cachedDownloader
}

func newCacheFileWrapper(fine *os.File, filePath string, cacheManager *cachedDownloader) *cacheFileWrapper {
	res := &cacheFileWrapper{
		file:         fine,
		filePath:     filePath,
		cacheManager: cacheManager,
	}
	runtime.SetFinalizer(res, finalizeCacheFileWrapper)
	return res
}

func (fw *cacheFileWrapper) Read(p []byte) (int, error) {
	return fw.file.Read(p)
}

func (fw *cacheFileWrapper) Close() error {
	err := fw.file.Close()
	if err != nil {
		return err
	}
	fw.cacheManager.tryRemoveUntrackedCacheFile(fw.filePath)
	runtime.SetFinalizer(fw, nil)
	return nil
}

func finalizeCacheFileWrapper(f *cacheFileWrapper) {
	f.Close()
}

type CachedFile struct {
	size        int64
	access      time.Time
	cachingInfo CachingInfoType
	filePath    string
}

type cachedDownloader struct {
	cachedPath     string
	uncachedPath   string
	maxSizeInBytes int64
	downloader     *Downloader
	lock           *sync.Mutex

	cachedFiles    map[string]CachedFile
	cacheFilePaths map[string]string
}

func New(cachedPath string, uncachedPath string, maxSizeInBytes int64, downloadTimeout time.Duration) *cachedDownloader {
	os.RemoveAll(cachedPath)
	os.MkdirAll(cachedPath, 0770)
	return &cachedDownloader{
		cachedPath:     cachedPath,
		uncachedPath:   uncachedPath,
		maxSizeInBytes: maxSizeInBytes,
		downloader:     NewDownloader(downloadTimeout),
		lock:           &sync.Mutex{},
		cachedFiles:    map[string]CachedFile{},
		cacheFilePaths: map[string]string{},
	}
}

func (c *cachedDownloader) Fetch(url *url.URL, cacheKey string) (io.ReadCloser, error) {
	if cacheKey == "" {
		return c.fetchUncachedFile(url)
	} else {
		cacheKey = fmt.Sprintf("%x", md5.Sum([]byte(cacheKey)))
		return c.fetchCachedFile(url, cacheKey)
	}
}

func (c *cachedDownloader) fetchUncachedFile(url *url.URL) (io.ReadCloser, error) {
	destinationFile, err := ioutil.TempFile(c.uncachedPath, "uncached")
	if err != nil {
		return nil, err
	}
	destinationFileName := destinationFile.Name()

	_, _, _, err = c.downloader.Download(url, destinationFile, CachingInfoType{})
	if err != nil {
		destinationFile.Close()
		os.RemoveAll(destinationFileName)
		return nil, err
	}

	destinationFile.Seek(0, 0)

	res := newCacheFileWrapper(destinationFile, destinationFileName, c)

	return res, nil
}

func (c *cachedDownloader) fetchCachedFile(url *url.URL, cacheKey string) (io.ReadCloser, error) {
	c.recordAccessForCacheKey(cacheKey)

	//download the file to a temporary location
	tempFile, err := ioutil.TempFile(c.uncachedPath, cacheKey+"-")
	if err != nil {
		return nil, err
	}

	tempFileName := tempFile.Name()
	// use RemoveAll. It has a better behavior on Windows. OS.Remove will remove the dir of the file, if the file dosn't exist and the dir of the file is empty.
	defer os.RemoveAll(tempFileName) //OK, even if we return tempFile 'cause that's how UNIX works.

	didDownload, size, cachingInfo, err := c.downloader.Download(url, tempFile, c.cachingInfoForCacheKey(cacheKey))
	if err != nil {
		tempFile.Close()
		return nil, err
	}

	tempFile.Close()

	resFilePath := tempFileName

	if didDownload {
		if cachingInfo.ETag == "" && cachingInfo.LastModified == "" {
			c.removeCacheEntryFor(cacheKey)
		} else {
			c.removeCacheFileFor(cacheKey)

			//make room for the file and move it in (if possible)
			movedToCache, err := c.moveFileIntoCache(cacheKey, tempFileName, size)
			if err != nil {
				return nil, err
			}

			if movedToCache {
				c.setCachingInfoForCacheKey(cacheKey, cachingInfo)
				resFilePath = c.pathForCacheKeyWithLock(cacheKey)
			}
		}
	} else {
		resFilePath = c.pathForCacheKeyWithLock(cacheKey)
	}

	f, err := os.Open(resFilePath)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	res := newCacheFileWrapper(f, resFilePath, c)

	return res, nil
}

func (c *cachedDownloader) moveFileIntoCache(cacheKey string, sourcePath string, size int64) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if size > c.maxSizeInBytes {
		//file does not fit in cache...
		return false, nil
	}

	usedSpace := int64(0)
	for ck, f := range c.cachedFiles {
		if ck != cacheKey {
			usedSpace += f.size
		}
	}

	for c.maxSizeInBytes < usedSpace+size {
		oldestAccessTime, oldestCacheKey := time.Now(), ""
		for ck, f := range c.cachedFiles {
			if ck != cacheKey {
				if f.access.Before(oldestAccessTime) {
					oldestCacheKey = ck
					oldestAccessTime = f.access
				}
			}
		}

		usedSpace -= c.cachedFiles[oldestCacheKey].size

		fp := c.pathForCacheKey(oldestCacheKey)

		if fp != "" {
			delete(c.cacheFilePaths, fp)
			os.RemoveAll(fp)
		}
		delete(c.cachedFiles, oldestCacheKey)
	}

	cachePath := filepath.Join(c.cachedPath, filepath.Base(sourcePath))

	err := os.Rename(sourcePath, cachePath)
	if err != nil {
		return false, err
	}

	f := c.cachedFiles[cacheKey]
	f.size = size
	f.filePath = cachePath
	c.cachedFiles[cacheKey] = f

	c.cacheFilePaths[cachePath] = cacheKey

	return true, nil
}

func (c *cachedDownloader) pathForCacheKey(cacheKey string) string {
	f := c.cachedFiles[cacheKey]
	return f.filePath
}

func (c *cachedDownloader) pathForCacheKeyWithLock(cacheKey string) string {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.pathForCacheKey(cacheKey)
}

func (c *cachedDownloader) removeCacheEntryFor(cacheKey string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	fp := c.pathForCacheKey(cacheKey)

	if fp != "" {
		delete(c.cacheFilePaths, fp)
		os.RemoveAll(fp)
	}
	delete(c.cachedFiles, cacheKey)
}

func (c *cachedDownloader) removeCacheFileFor(cacheKey string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	fp := c.pathForCacheKey(cacheKey)

	if fp != "" {
		delete(c.cacheFilePaths, fp)
		os.RemoveAll(fp)
	}

	cf := c.cachedFiles[cacheKey]
	cf.filePath = ""
	c.cachedFiles[cacheKey] = cf
}

func (c *cachedDownloader) recordAccessForCacheKey(cacheKey string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	f := c.cachedFiles[cacheKey]
	f.access = time.Now()
	c.cachedFiles[cacheKey] = f
}

func (c *cachedDownloader) cachingInfoForCacheKey(cacheKey string) CachingInfoType {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.cachedFiles[cacheKey].cachingInfo
}

func (c *cachedDownloader) setCachingInfoForCacheKey(cacheKey string, cachingInfo CachingInfoType) {
	c.lock.Lock()
	defer c.lock.Unlock()
	f := c.cachedFiles[cacheKey]
	f.cachingInfo = cachingInfo
	c.cachedFiles[cacheKey] = f
}

func (c *cachedDownloader) tryRemoveUntrackedCacheFile(cacheFilePath string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	_, isTracked := c.cacheFilePaths[cacheFilePath]
	if !isTracked {
		os.RemoveAll(cacheFilePath)
	}
}
