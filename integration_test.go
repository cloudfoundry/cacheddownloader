package cacheddownloader_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/cloudfoundry-incubator/cacheddownloader"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Integration", func() {
	var (
		server              *httptest.Server
		serverPath          string
		cachedPath          string
		uncachedPath        string
		cacheMaxSizeInBytes int64         = 1024
		downloadTimeout     time.Duration = time.Second
		downloader          cacheddownloader.CachedDownloader
		url                 *url.URL
	)

	BeforeEach(func() {
		var err error

		serverPath, err = ioutil.TempDir("", "cached_downloader_integration_server")
		Expect(err).NotTo(HaveOccurred())

		cachedPath, err = ioutil.TempDir("", "cached_downloader_integration_cache")
		Expect(err).NotTo(HaveOccurred())

		uncachedPath, err = ioutil.TempDir("", "cached_downloader_integration_uncached")
		Expect(err).NotTo(HaveOccurred())

		handler := http.FileServer(http.Dir(serverPath))
		server = httptest.NewServer(handler)

		url, err = url.Parse(server.URL + "/file")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		os.RemoveAll(serverPath)
		os.RemoveAll(cachedPath)
		os.RemoveAll(uncachedPath)
		server.Close()
	})

	fetch := func() ([]byte, time.Time) {
		url, err := url.Parse(server.URL + "/file")
		Expect(err).NotTo(HaveOccurred())

		reader, _, err := downloader.Fetch(url, "the-cache-key", cacheddownloader.NoopTransform, make(chan struct{}))
		Expect(err).NotTo(HaveOccurred())
		defer reader.Close()

		readData, err := ioutil.ReadAll(reader)
		Expect(err).NotTo(HaveOccurred())

		cacheContents, err := ioutil.ReadDir(cachedPath)
		Expect(cacheContents).To(HaveLen(1))
		Expect(err).NotTo(HaveOccurred())

		content, err := ioutil.ReadFile(filepath.Join(cachedPath, cacheContents[0].Name()))
		Expect(err).NotTo(HaveOccurred())

		Expect(readData).To(Equal(content))

		return content, cacheContents[0].ModTime()
	}

	Describe("Cached Downloader", func() {
		BeforeEach(func() {
			downloader = cacheddownloader.New(cachedPath, uncachedPath, cacheMaxSizeInBytes, downloadTimeout, 10, false)

			// touch a file on disk
			err := ioutil.WriteFile(filepath.Join(serverPath, "file"), []byte("a"), 0666)
			Expect(err).NotTo(HaveOccurred())
		})

		It("caches downloads", func() {
			// download file once
			content, modTimeBefore := fetch()
			Expect(content).To(Equal([]byte("a")))

			time.Sleep(time.Second)

			// download again should be cached
			content, modTimeAfter := fetch()
			Expect(content).To(Equal([]byte("a")))
			Expect(modTimeBefore).To(Equal(modTimeAfter))

			time.Sleep(time.Second)

			// touch file again
			err := ioutil.WriteFile(filepath.Join(serverPath, "file"), []byte("b"), 0666)
			Expect(err).NotTo(HaveOccurred())

			// download again and we should get a file containing "b"
			content, _ = fetch()
			Expect(content).To(Equal([]byte("b")))
		})
	})
})
