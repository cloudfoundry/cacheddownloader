package cacheddownloader

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"time"
)

type CachedDownloader interface {
	Fetch(url *url.URL, cacheKey string) (io.ReadCloser, error)
}

type CachingInfoType struct {
	ETag         string
	LastModified string
}

type cachedDownloader struct {
	downloader   *Downloader
	uncachedPath string
	cache        *fileCache
}

func New(cachedPath string, uncachedPath string, maxSizeInBytes int64, downloadTimeout time.Duration) *cachedDownloader {
	os.RemoveAll(cachedPath)
	os.MkdirAll(cachedPath, 0770)
	return &cachedDownloader{
		downloader:   NewDownloader(downloadTimeout),
		uncachedPath: uncachedPath,
		cache:        NewCache(cachedPath, maxSizeInBytes),
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

	res := NewFileCloser(destinationFile, func(filePath string) {
		os.RemoveAll(filePath)
	})

	return res, nil
}

func (c *cachedDownloader) fetchCachedFile(url *url.URL, cacheKey string) (io.ReadCloser, error) {
	c.cache.RecordAccess(cacheKey)

	downloadedFile, err := ioutil.TempFile(c.uncachedPath, cacheKey+"-")
	if err != nil {
		return nil, err
	}

	downloadedFileName := downloadedFile.Name()
	// use RemoveAll. It has a better behavior on Windows. OS.Remove will remove the dir of the file, if the file dosn't exist and the dir of the file is empty.
	defer os.RemoveAll(downloadedFileName) //OK, even if we return downloadedFile 'cause that's how UNIX works.

	didDownload, size, cachingInfo, err := c.downloader.Download(url, downloadedFile, c.cache.Info(cacheKey))
	if err != nil {
		downloadedFile.Close()
		return nil, err
	}

	downloadedFile.Close()

	var filePathToRead string

	if didDownload {
		if cachingInfo.ETag == "" && cachingInfo.LastModified == "" {
			c.cache.RemoveEntry(cacheKey)
			filePathToRead = downloadedFileName
		} else {
			movedToCache, err := c.cache.Add(cacheKey, downloadedFileName, size, cachingInfo)
			if err != nil {
				return nil, err
			}

			if movedToCache {
				filePathToRead = c.cache.PathForKey(cacheKey)
			} else {
				filePathToRead = downloadedFileName
			}
		}
	} else {
		filePathToRead = c.cache.PathForKey(cacheKey)
	}

	f, err := os.Open(filePathToRead)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	res := NewFileCloser(f, func(filePath string) {
		c.cache.RemoveFileIfUntracked(filePath)
	})

	return res, nil
}
