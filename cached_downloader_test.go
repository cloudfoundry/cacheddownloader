package cacheddownloader_test

import (
	"archive/tar"
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	Url "net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/cacheddownloader"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/ghttp"
)

const MAX_CONCURRENT_DOWNLOADS = 10

func computeMd5(key string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(key)))
}

var _ = Describe("File cache", func() {
	var (
		cache           cacheddownloader.CachedDownloader
		cachedPath      string
		uncachedPath    string
		maxSizeInBytes  int64
		cacheKey        string
		downloadContent []byte
		url             *Url.URL
		server          *ghttp.Server
		cancelChan      chan struct{}
	)

	BeforeEach(func() {
		var err error
		cachedPath, err = ioutil.TempDir("", "test_file_cached")
		Expect(err).NotTo(HaveOccurred())

		uncachedPath, err = ioutil.TempDir("", "test_file_uncached")
		Expect(err).NotTo(HaveOccurred())

		// This needs to be larger now since when we do the tests with directories
		// the tar file is at least 4000 bytes and we calculate 8000 bytes with the
		// archive and the directory
		maxSizeInBytes = 32000

		cacheKey = "the-cache-key"

		cache = cacheddownloader.New(cachedPath, uncachedPath, maxSizeInBytes, 1*time.Second, MAX_CONCURRENT_DOWNLOADS, false)
		server = ghttp.NewServer()

		url, err = Url.Parse(server.URL() + "/my_file")
		Expect(err).NotTo(HaveOccurred())

		cancelChan = make(chan struct{})
	})

	AfterEach(func() {
		os.RemoveAll(cachedPath)
		os.RemoveAll(uncachedPath)
	})

	var (
		file     io.ReadCloser
		fileSize int64
		err      error
		dir      string
	)

	Describe("when the cache folder does not exist", func() {
		It("should create it", func() {
			os.RemoveAll(cachedPath)
			cache = cacheddownloader.New(cachedPath, uncachedPath, maxSizeInBytes, time.Second, MAX_CONCURRENT_DOWNLOADS, false)
			_, err := os.Stat(cachedPath)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("when the cache folder has stuff in it", func() {
		It("should nuke that stuff", func() {
			filename := filepath.Join(cachedPath, "last_nights_dinner")
			ioutil.WriteFile(filename, []byte("leftovers"), 0666)
			cache = cacheddownloader.New(cachedPath, uncachedPath, maxSizeInBytes, time.Second, MAX_CONCURRENT_DOWNLOADS, false)
			_, err := os.Stat(filename)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("When providing a file that should not be cached", func() {
		Context("when the download succeeds", func() {
			BeforeEach(func() {
				downloadContent = []byte("777")

				header := http.Header{}
				header.Set("ETag", "foo")
				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/my_file"),
					ghttp.RespondWith(http.StatusOK, string(downloadContent), header),
				))

				file, fileSize, err = cache.Fetch(url, "", cacheddownloader.NoopTransform, cancelChan)
			})

			It("should not error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("should return a readCloser that streams the file", func() {
				Expect(file).NotTo(BeNil())
				Expect(fileSize).To(BeNumerically("==", 3))
				Expect(ioutil.ReadAll(file)).To(Equal(downloadContent))
			})

			It("should delete the file when we close the readCloser", func() {
				err := file.Close()
				Expect(err).NotTo(HaveOccurred())
				Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
				Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(0))
			})
		})

		Context("when the download fails", func() {
			BeforeEach(func() {
				server.AllowUnhandledRequests = true //will 500 for any attempted requests
				file, fileSize, err = cache.Fetch(url, "", cacheddownloader.NoopTransform, cancelChan)
			})

			It("should return an error and no file", func() {
				Expect(file).To(BeNil())
				Expect(fileSize).To(BeNumerically("==", 0))
				Expect(err).To(HaveOccurred())
			})

			It("should clean up after itself", func() {
				Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
				Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(0))
			})
		})
	})

	Describe("When providing a file that should be cached", func() {
		var cacheFilePath string
		var returnedHeader http.Header

		BeforeEach(func() {
			cacheFilePath = filepath.Join(cachedPath, computeMd5(cacheKey))
			returnedHeader = http.Header{}
			returnedHeader.Set("ETag", "my-original-etag")
		})

		Context("when the file is not in the cache", func() {
			var (
				transformer cacheddownloader.CacheTransformer

				fetchedFile     io.ReadCloser
				fetchedFileSize int64
				fetchErr        error
			)

			BeforeEach(func() {
				transformer = cacheddownloader.NoopTransform
			})

			JustBeforeEach(func() {
				fetchedFile, fetchedFileSize, fetchErr = cache.Fetch(url, cacheKey, transformer, cancelChan)
			})

			Context("when the download succeeds", func() {
				BeforeEach(func() {
					downloadContent = []byte(strings.Repeat("7", int(maxSizeInBytes/2)))
					server.AppendHandlers(ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/my_file"),
						http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							Expect(req.Header.Get("If-None-Match")).To(BeEmpty())
						}),
						ghttp.RespondWith(http.StatusOK, string(downloadContent), returnedHeader),
					))
				})

				It("should not error", func() {
					Expect(fetchErr).NotTo(HaveOccurred())
				})

				It("should return a readCloser that streams the file", func() {
					Expect(fetchedFile).NotTo(BeNil())
					Expect(fetchedFileSize).To(BeNumerically("==", maxSizeInBytes/2))
					Expect(ioutil.ReadAll(fetchedFile)).To(Equal(downloadContent))
				})

				It("should return a file within the cache", func() {
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(1))
				})

				It("should remove any temporary assets generated along the way", func() {
					Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
				})

				Describe("downloading with a transformer", func() {
					BeforeEach(func() {
						transformer = func(source string, destination string) (int64, error) {
							err := ioutil.WriteFile(destination, []byte("hello tmp"), 0644)
							Expect(err).NotTo(HaveOccurred())

							return 100, err
						}
					})

					It("passes the download through the transformer", func() {
						Expect(fetchErr).NotTo(HaveOccurred())

						content, err := ioutil.ReadAll(fetchedFile)
						Expect(err).NotTo(HaveOccurred())
						Expect(string(content)).To(Equal("hello tmp"))
					})
				})
			})

			Context("when the download succeeds but does not have an ETag", func() {
				BeforeEach(func() {
					downloadContent = []byte(strings.Repeat("7", int(maxSizeInBytes/2)))
					server.AppendHandlers(ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/my_file"),
						http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							Expect(req.Header.Get("If-None-Match")).To(BeEmpty())
						}),
						ghttp.RespondWith(http.StatusOK, string(downloadContent)),
					))
				})

				It("should not error", func() {
					Expect(fetchErr).NotTo(HaveOccurred())
				})

				It("should return a readCloser that streams the file", func() {
					Expect(fetchedFile).NotTo(BeNil())
					Expect(ioutil.ReadAll(fetchedFile)).To(Equal(downloadContent))
				})

				It("should not store the file", func() {
					fetchedFile.Close()
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(0))
					Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
				})
			})

			Context("when the download fails", func() {
				BeforeEach(func() {
					server.AllowUnhandledRequests = true //will 500 for any attempted requests
				})

				It("should return an error and no file", func() {
					Expect(fetchedFile).To(BeNil())
					Expect(fetchErr).To(HaveOccurred())
				})

				It("should clean up after itself", func() {
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(0))
					Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
				})
			})
		})

		Context("when the file is already on disk in the cache", func() {
			var fileContent []byte
			var status int
			var downloadContent string

			BeforeEach(func() {
				status = http.StatusOK
				fileContent = []byte("now you see it")

				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/my_file"),
					ghttp.RespondWith(http.StatusOK, string(fileContent), returnedHeader),
				))

				f, s, _ := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
				defer f.Close()
				Expect(s).To(BeNumerically("==", len(fileContent)))

				downloadContent = "now you don't"

				etag := "second-round-etag"
				returnedHeader.Set("ETag", etag)

				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/my_file"),
					http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						Expect(req.Header.Get("If-None-Match")).To(Equal("my-original-etag"))
					}),
					ghttp.RespondWithPtr(&status, &downloadContent, returnedHeader),
				))
			})

			It("should perform the request with the correct modified headers", func() {
				cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
				Expect(server.ReceivedRequests()).To(HaveLen(2))
			})

			Context("if the file has been modified", func() {
				BeforeEach(func() {
					status = http.StatusOK
				})

				It("should redownload the file", func() {
					f, _, _ := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
					defer f.Close()

					paths, _ := filepath.Glob(cacheFilePath + "*")
					Expect(ioutil.ReadFile(paths[0])).To(Equal([]byte(downloadContent)))
				})

				It("should return a readcloser pointing to the file", func() {
					file, fileSize, err := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
					Expect(err).NotTo(HaveOccurred())
					Expect(ioutil.ReadAll(file)).To(Equal([]byte(downloadContent)))
					Expect(fileSize).To(BeNumerically("==", len(downloadContent)))
				})

				It("should have put the file in the cache", func() {
					f, s, err := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
					f.Close()
					Expect(err).NotTo(HaveOccurred())
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(1))
					Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
					Expect(s).To(BeNumerically("==", len(downloadContent)))
				})
			})

			Context("if the file has been modified, but the new file has no etag", func() {
				BeforeEach(func() {
					status = http.StatusOK
					returnedHeader.Del("ETag")
				})

				It("should return a readcloser pointing to the file", func() {
					file, fileSize, err := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
					Expect(err).NotTo(HaveOccurred())
					Expect(ioutil.ReadAll(file)).To(Equal([]byte(downloadContent)))
					Expect(fileSize).To(BeNumerically("==", len(downloadContent)))
				})

				It("should have removed the file from the cache", func() {
					f, s, err := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
					f.Close()
					Expect(err).NotTo(HaveOccurred())
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(0))
					Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
					Expect(s).To(BeNumerically("==", len(downloadContent)))
				})
			})

			Context("if the file has not been modified", func() {
				BeforeEach(func() {
					status = http.StatusNotModified
				})

				It("should not redownload the file", func() {
					f, s, err := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
					Expect(err).NotTo(HaveOccurred())
					defer f.Close()

					paths, _ := filepath.Glob(cacheFilePath + "*")
					Expect(ioutil.ReadFile(paths[0])).To(Equal(fileContent))
					Expect(s).To(BeZero())
				})

				It("should return a readcloser pointing to the file", func() {
					file, _, err := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
					Expect(err).NotTo(HaveOccurred())
					Expect(ioutil.ReadAll(file)).To(Equal(fileContent))
				})
			})
		})

		Context("when the file size exceeds the total available cache size", func() {
			BeforeEach(func() {
				downloadContent = []byte(strings.Repeat("7", int(maxSizeInBytes*3)))

				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/my_file"),
					ghttp.RespondWith(http.StatusOK, string(downloadContent), returnedHeader),
				))

				file, fileSize, err = cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
			})

			It("should not error", func() {
				Expect(err).NotTo(HaveOccurred())
			})

			It("should return a readCloser that streams the file", func() {
				Expect(file).NotTo(BeNil())
				Expect(ioutil.ReadAll(file)).To(Equal(downloadContent))
				Expect(fileSize).To(BeNumerically("==", maxSizeInBytes*3))
			})

			It("should put the file in the uncached path, then delete it", func() {
				err := file.Close()
				Expect(err).NotTo(HaveOccurred())
				Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(0))
				Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
			})

			Context("when the file is downloaded the second time", func() {
				BeforeEach(func() {
					err = file.Close()
					Expect(err).NotTo(HaveOccurred())

					server.AppendHandlers(ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/my_file"),
						http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							Expect(req.Header.Get("If-None-Match")).To(BeEmpty())
						}),
						ghttp.RespondWith(http.StatusOK, string(downloadContent), returnedHeader),
					))

					file, _, err = cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
				})

				It("should not error", func() {
					Expect(err).NotTo(HaveOccurred())
				})

				It("should return a readCloser that streams the file", func() {
					Expect(file).NotTo(BeNil())
					Expect(ioutil.ReadAll(file)).To(Equal(downloadContent))
				})
			})
		})

		Context("when the cache is full", func() {
			fetchFileOfSize := func(name string, size int) {
				downloadContent = []byte(strings.Repeat("7", size))
				url, _ = Url.Parse(server.URL() + "/" + name)
				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/"+name),
					ghttp.RespondWith(http.StatusOK, string(downloadContent), returnedHeader),
				))

				cachedFile, _, err := cache.Fetch(url, name, cacheddownloader.NoopTransform, cancelChan)
				Expect(err).NotTo(HaveOccurred())
				Expect(ioutil.ReadAll(cachedFile)).To(Equal(downloadContent))
				cachedFile.Close()
			}

			BeforeEach(func() {
				fetchFileOfSize("A", int(maxSizeInBytes/4))
				fetchFileOfSize("B", int(maxSizeInBytes/4))
				fetchFileOfSize("C", int(maxSizeInBytes/4))
			})

			It("deletes the oldest cached files until there is space", func() {
				//try to add a file that has size larger
				fetchFileOfSize("D", int(maxSizeInBytes/2)+1)

				Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(2))

				Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("A")+"*"))).To(HaveLen(0))
				Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("B")+"*"))).To(HaveLen(0))
				Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("C")+"*"))).To(HaveLen(1))
				Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("D")+"*"))).To(HaveLen(1))
			})

			Describe("when one of the files has just been read", func() {
				BeforeEach(func() {
					server.AppendHandlers(ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/A"),
						ghttp.RespondWith(http.StatusNotModified, "", returnedHeader),
					))

					url, _ = Url.Parse(server.URL() + "/A")
					// windows timer increments every 15.6ms, without the sleep
					// A, B & C will sometimes have the same timestamp
					time.Sleep(16 * time.Millisecond)
					cache.Fetch(url, "A", cacheddownloader.NoopTransform, cancelChan)
				})

				It("considers that file to be the newest", func() {
					//try to add a file that has size larger
					fetchFileOfSize("D", int(maxSizeInBytes/2)+1)

					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(2))

					Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("A")+"*"))).To(HaveLen(1))
					Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("B")+"*"))).To(HaveLen(0))
					Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("C")+"*"))).To(HaveLen(0))
					Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("D")+"*"))).To(HaveLen(1))
				})
			})
		})
	})

	Describe("rate limiting", func() {

		Context("when multiple, concurrent requests occur", func() {
			var barrier chan interface{}
			var results chan bool

			BeforeEach(func() {
				barrier = make(chan interface{}, 1)
				results = make(chan bool, 1)

				downloadContent = []byte(strings.Repeat("7", int(maxSizeInBytes/2)))
				server.AppendHandlers(
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/my_file"),
						http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							barrier <- nil
							Consistently(results, .5).ShouldNot(Receive())
						}),
						ghttp.RespondWith(http.StatusOK, string(downloadContent)),
					),
					ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/my_file"),
						http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							results <- true
						}),
						ghttp.RespondWith(http.StatusOK, string(downloadContent)),
					),
				)
			})

			It("processes one at a time", func() {
				go func() {
					cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
					barrier <- nil
				}()
				<-barrier
				cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
				<-barrier
			})
		})

		Context("when cancelling", func() {
			var requestInitiated chan struct{}
			var completeRequest chan struct{}

			BeforeEach(func() {
				requestInitiated = make(chan struct{})
				completeRequest = make(chan struct{})

				server.AllowUnhandledRequests = true
				server.AppendHandlers(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requestInitiated <- struct{}{}
						<-completeRequest
						w.Write([]byte("response data..."))
					}),
				)

				serverUrl := server.URL() + "/somepath"
				url, _ = url.Parse(serverUrl)
			})

			It("stops waiting", func() {
				errs := make(chan error)

				go func() {
					_, _, err := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, make(chan struct{}))
					errs <- err
				}()
				Eventually(requestInitiated).Should(Receive())

				close(cancelChan)

				_, _, err := cache.Fetch(url, cacheKey, cacheddownloader.NoopTransform, cancelChan)
				Expect(err).To(BeAssignableToTypeOf(cacheddownloader.NewDownloadCancelledError("", 0, cacheddownloader.NoBytesReceived)))

				close(completeRequest)
				Eventually(errs).Should(Receive(BeNil()))
			})
		})
	})

	Describe("When providing a tar that should be cached", func() {
		var cacheFilePath string
		var returnedHeader http.Header

		BeforeEach(func() {
			cacheFilePath = filepath.Join(cachedPath, computeMd5(cacheKey))
			returnedHeader = http.Header{}
			returnedHeader.Set("ETag", "my-original-etag")
		})

		Context("when the file is not in the cache", func() {
			var (
				fetchedDir string
				fetchErr   error
			)

			JustBeforeEach(func() {
				fetchedDir, fetchErr = cache.FetchAsDirectory(url, cacheKey, cancelChan)
			})

			Context("when the download succeeds", func() {
				BeforeEach(func() {
					downloadContent = CreateTarBuffer("test content", 0).Bytes()
					server.AppendHandlers(ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/my_file"),
						http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							Expect(req.Header.Get("If-None-Match")).To(BeEmpty())
						}),
						ghttp.RespondWith(http.StatusOK, string(downloadContent), returnedHeader),
					))
				})

				It("should not error", func() {
					Expect(fetchErr).NotTo(HaveOccurred())
				})

				It("should return a directory", func() {
					Expect(fetchedDir).NotTo(BeEmpty())
					fileInfo, err := os.Stat(fetchedDir)
					Expect(err).NotTo(HaveOccurred())
					Expect(fileInfo.IsDir()).To(BeTrue())
				})

				It("should store the tarball and directory in the cache", func() {
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(2))
				})

				It("should remove any temporary assets generated along the way", func() {
					Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
				})
			})

			Context("when the download succeeds but does not have an ETag", func() {
				BeforeEach(func() {
					downloadContent = CreateTarBuffer("test content", 0).Bytes()
					server.AppendHandlers(ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/my_file"),
						http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							Expect(req.Header.Get("If-None-Match")).To(BeEmpty())
						}),
						ghttp.RespondWith(http.StatusOK, string(downloadContent)),
					))
				})

				It("should error", func() {
					Expect(fetchErr).To(HaveOccurred())
				})
			})

			Context("when the download fails", func() {
				BeforeEach(func() {
					server.AllowUnhandledRequests = true //will 500 for any attempted requests
				})

				It("should return an error and no file", func() {
					Expect(fetchedDir).To(BeEmpty())
					Expect(fetchErr).To(HaveOccurred())
				})

				It("should clean up after itself", func() {
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(0))
					Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
				})
			})
		})

		Context("when the file is already on disk in the cache", func() {
			var fileContent []byte
			var status int
			var downloadContent []byte

			BeforeEach(func() {
				status = http.StatusOK
				fileContent = CreateTarBuffer("now you see it", 0).Bytes()

				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/my_file"),
					ghttp.RespondWith(http.StatusOK, string(fileContent), returnedHeader),
				))

				dir, _ := cache.FetchAsDirectory(url, cacheKey, cancelChan)
				err := cache.CloseDirectory(cacheKey, dir)
				Expect(err).ToNot(HaveOccurred())

				downloadContent = CreateTarBuffer("now you don't", 0).Bytes()

				etag := "second-round-etag"
				returnedHeader.Set("ETag", etag)

				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/my_file"),
					http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						Expect(req.Header.Get("If-None-Match")).To(Equal("my-original-etag"))
					}),
					ghttp.RespondWithPtr(&status, &downloadContent, returnedHeader),
				))
			})

			It("should perform the request with the correct modified headers", func() {
				dir, _ := cache.FetchAsDirectory(url, cacheKey, cancelChan)
				err := cache.CloseDirectory(cacheKey, dir)
				Expect(err).ToNot(HaveOccurred())
				Expect(server.ReceivedRequests()).To(HaveLen(2))
			})

			Context("if the file has been modified", func() {
				BeforeEach(func() {
					status = http.StatusOK
				})

				It("should redownload the file", func() {
					dir, _ := cache.FetchAsDirectory(url, cacheKey, cancelChan)
					err := cache.CloseDirectory(cacheKey, dir)
					Expect(err).ToNot(HaveOccurred())
				})

				It("should return a string pointing to the file", func() {
					dir, err := cache.FetchAsDirectory(url, cacheKey, cancelChan)
					Expect(err).NotTo(HaveOccurred())
					fileInfo, err := os.Stat(dir)
					Expect(err).NotTo(HaveOccurred())
					Expect(fileInfo.IsDir()).To(BeTrue())
				})

				It("should have put the file in the cache", func() {
					dir, err := cache.FetchAsDirectory(url, cacheKey, cancelChan)
					Expect(err).NotTo(HaveOccurred())
					err = cache.CloseDirectory(cacheKey, dir)
					Expect(err).ToNot(HaveOccurred())
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(2))
					Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
				})
			})

			Context("if the file has been modified, but the new file has no etag", func() {
				var fetchErr error
				var fetchDir string

				BeforeEach(func() {
					status = http.StatusOK
					returnedHeader.Del("ETag")
					fetchDir, fetchErr = cache.FetchAsDirectory(url, cacheKey, cancelChan)
				})

				It("should error", func() {
					Expect(fetchErr).To(HaveOccurred())
				})

				It("should have removed the file from the cache", func() {
					Expect(err).NotTo(HaveOccurred())
					cache.CloseDirectory(cacheKey, fetchDir)
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(0))
				})
			})

			Context("if the file has not been modified", func() {
				BeforeEach(func() {
					status = http.StatusNotModified
				})

				It("should not redownload the file", func() {
					dir, err := cache.FetchAsDirectory(url, cacheKey, cancelChan)
					Expect(err).NotTo(HaveOccurred())
					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(2))
					cache.CloseDirectory(cacheKey, dir)
				})

				It("should return a directory pointing to the file", func() {
					dir, err := cache.FetchAsDirectory(url, cacheKey, cancelChan)
					Expect(err).NotTo(HaveOccurred())
					fileInfo, err := os.Stat(dir)
					Expect(err).NotTo(HaveOccurred())
					Expect(fileInfo.IsDir()).To(BeTrue())
				})
			})

		})

		Context("when the file size exceeds the total available cache size", func() {
			var fileContent []byte
			BeforeEach(func() {
				fileContent = CreateTarBuffer("Test small string", 100).Bytes()

				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/my_file"),
					ghttp.RespondWith(http.StatusOK, string(fileContent), returnedHeader),
				))

				dir, err = cache.FetchAsDirectory(url, cacheKey, cancelChan)
			})

			It("should error", func() {
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when the cache is full", func() {
			fetchDirOfSize := func(name string, size int) {
				fileContent := CreateTarBuffer("Test small string", size).Bytes()

				url, _ = Url.Parse(server.URL() + "/" + name)
				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/"+name),
					ghttp.RespondWith(http.StatusOK, string(fileContent), returnedHeader),
				))

				dir, err := cache.FetchAsDirectory(url, name, cancelChan)
				Expect(err).NotTo(HaveOccurred())
				fileInfo, err := os.Stat(dir)
				Expect(err).NotTo(HaveOccurred())
				Expect(fileInfo.IsDir()).To(BeTrue())
				cache.CloseDirectory(name, dir)
			}

			BeforeEach(func() {
				fetchDirOfSize("A", 0)
				fetchDirOfSize("B", 0)
				fetchDirOfSize("C", 0)
			})

			It("should have all entries in the cache", func() {
				Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(6))
			})

			It("deletes the oldest cached files until there is space", func() {
				//try to add a file that has size larger
				fetchDirOfSize("D", 3)

				Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(4))

				Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("A")+"*"))).To(HaveLen(0))
				Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("B")+"*"))).To(HaveLen(0))
				Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("C")+"*"))).To(HaveLen(2))
				Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("D")+"*"))).To(HaveLen(2))
			})

			Describe("when one of the files has just been read", func() {
				BeforeEach(func() {
					server.AppendHandlers(ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/A"),
						ghttp.RespondWith(http.StatusNotModified, "", returnedHeader),
					))

					url, _ = Url.Parse(server.URL() + "/A")
					// windows timer increments every 15.6ms, without the sleep
					// A, B & C will sometimes have the same timestamp
					time.Sleep(16 * time.Millisecond)
					cache.FetchAsDirectory(url, "A", cancelChan)
				})

				It("considers that file to be the newest", func() {
					//try to add a file that has size larger
					fetchDirOfSize("D", 3)

					Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(4))

					Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("A")+"*"))).To(HaveLen(2))
					Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("B")+"*"))).To(HaveLen(0))
					Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("C")+"*"))).To(HaveLen(0))
					Expect(filepath.Glob(filepath.Join(cachedPath, computeMd5("D")+"*"))).To(HaveLen(2))
				})
			})
		})
	})

	Describe("When doing a Fetch and then a FetchAsDirectory", func() {
		var cacheFilePath string
		var returnedHeader http.Header

		BeforeEach(func() {
			cacheFilePath = filepath.Join(cachedPath, computeMd5(cacheKey))
			returnedHeader = http.Header{}
			returnedHeader.Set("ETag", "my-original-etag")
		})

		Context("when the archive is fetched", func() {
			var (
				fetchedFile     io.ReadCloser
				fetchedFileSize int64
				fetchErr        error
				status          int
			)

			JustBeforeEach(func() {
				status = http.StatusOK

				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/my_file"),
					ghttp.RespondWith(http.StatusOK, string(downloadContent), returnedHeader),
				))

				fetchedFile, fetchedFileSize, fetchErr = cache.Fetch(url, cacheKey, cacheddownloader.TarTransform, cancelChan)
				Expect(fetchErr).NotTo(HaveOccurred())
				Expect(fetchedFile).NotTo(BeNil())
				Expect(ioutil.ReadAll(fetchedFile)).To(Equal(downloadContent))
				fetchedFile.Close()
			})

			Context("then is fetched with FetchAsDirecory", func() {
				var (
					fetchedDir  string
					fetchDirErr error
				)

				JustBeforeEach(func() {
					server.AppendHandlers(ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/my_file"),
						http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							Expect(req.Header.Get("If-None-Match")).To(Equal("my-original-etag"))
						}),
						ghttp.RespondWithPtr(&status, &downloadContent, returnedHeader),
					))
					fetchedDir, fetchDirErr = cache.FetchAsDirectory(url, cacheKey, cancelChan)
				})

				Context("the content will fit in the cache on expansion", func() {
					BeforeEach(func() {
						downloadContent = CreateTarBuffer("test content", 0).Bytes()
					})

					It("should not error", func() {
						Expect(fetchDirErr).NotTo(HaveOccurred())
						Expect(fetchedDir).NotTo(BeEmpty())
						fileInfo, err := os.Stat(fetchedDir)
						Expect(err).NotTo(HaveOccurred())
						Expect(fileInfo.IsDir()).To(BeTrue())
						Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(2))
						Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
					})
				})

				Context("the content will NOT fit in the cache on expansion", func() {
					BeforeEach(func() {
						downloadContent = CreateTarBuffer("test content", 12).Bytes()
					})

					It("should error", func() {
						Expect(fetchDirErr).To(HaveOccurred())
						Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(1))
					})
				})

				Context("the content will fit in the cache on expansion if we make room", func() {
					BeforeEach(func() {
						fillerContent := []byte(strings.Repeat("7", int(maxSizeInBytes/2)))
						urlFiller, _ := Url.Parse(server.URL() + "/filler")
						server.AppendHandlers(ghttp.CombineHandlers(
							ghttp.VerifyRequest("GET", "/filler"),
							ghttp.RespondWith(http.StatusOK, string(fillerContent), returnedHeader),
						))

						cachedFile, _, err := cache.Fetch(urlFiller, "filler", cacheddownloader.NoopTransform, cancelChan)
						Expect(err).NotTo(HaveOccurred())
						Expect(ioutil.ReadAll(cachedFile)).To(Equal(fillerContent))
						cachedFile.Close()

						downloadContent = CreateTarBuffer("test content", 10).Bytes()
					})

					It("should not error", func() {
						Expect(fetchDirErr).NotTo(HaveOccurred())
						Expect(fetchedDir).NotTo(BeEmpty())
						fileInfo, err := os.Stat(fetchedDir)
						Expect(err).NotTo(HaveOccurred())
						Expect(fileInfo.IsDir()).To(BeTrue())
						Expect(ioutil.ReadDir(cachedPath)).To(HaveLen(2))
						Expect(ioutil.ReadDir(uncachedPath)).To(HaveLen(0))
					})
				})
			})
		})
	})

	Describe("When doing a FetchAsDirectory and then a Fetch", func() {
		var cacheFilePath string
		var returnedHeader http.Header

		BeforeEach(func() {
			cacheFilePath = filepath.Join(cachedPath, computeMd5(cacheKey))
			returnedHeader = http.Header{}
			returnedHeader.Set("ETag", "my-original-etag")
		})

		Context("when the archive is fetched", func() {
			var (
				fetchedFile     io.ReadCloser
				fetchedFileSize int64
				fetchErr        error
				status          int
			)

			JustBeforeEach(func() {
				status = http.StatusOK

				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/my_file"),
					ghttp.RespondWith(http.StatusOK, string(downloadContent), returnedHeader),
				))

				dir, err := cache.FetchAsDirectory(url, cacheKey, cancelChan)
				Expect(err).NotTo(HaveOccurred())
				Expect(dir).NotTo(BeEmpty())
				fileInfo, err := os.Stat(dir)
				Expect(err).NotTo(HaveOccurred())
				Expect(fileInfo.IsDir()).To(BeTrue())
			})

			Context("then is fetched with Fetch", func() {
				JustBeforeEach(func() {
					server.AppendHandlers(ghttp.CombineHandlers(
						ghttp.VerifyRequest("GET", "/my_file"),
						http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
							Expect(req.Header.Get("If-None-Match")).To(Equal("my-original-etag"))
						}),
						ghttp.RespondWithPtr(&status, &downloadContent, returnedHeader),
					))
					fetchedFile, fetchedFileSize, fetchErr = cache.Fetch(url, cacheKey, cacheddownloader.TarTransform, cancelChan)
				})
				BeforeEach(func() {
					downloadContent = CreateTarBuffer("test content", 0).Bytes()
				})

				It("should not error", func() {
					Expect(fetchErr).NotTo(HaveOccurred())
					Expect(fetchedFile).NotTo(BeNil())
					Expect(ioutil.ReadAll(fetchedFile)).To(Equal(downloadContent))
					fetchedFile.Close()
				})
			})
		})
	})
})

type constTransformer struct {
	file string
	size int64
	err  error
}

func (t constTransformer) ConstTransform(path string) (string, int64, error) {
	return t.file, t.size, t.err
}

func CreateTarBuffer(content string, numFiles int) *bytes.Buffer {
	// Create a buffer to write our archive to.
	buf := new(bytes.Buffer)

	// Create a new tar archive.
	tw := tar.NewWriter(buf)
	// Add some files to the archive.
	var files = []struct {
		Name, Body string
		Type       byte
		Mode       int64
	}{
		{"readme.txt", "This archive contains some text files.", tar.TypeReg, 0600},
		{"diego.txt", "Diego names:\nVizzini\nGeoffrey\nPrincess Buttercup\n", tar.TypeReg, 0600},
		{"testdir", "", tar.TypeDir, 0766},
		{"testdir/file.txt", content, tar.TypeReg, 0600},
	}

	for _, file := range files {
		hdr := &tar.Header{
			Name:     file.Name,
			Typeflag: file.Type,
			Mode:     file.Mode,
			Size:     int64(len(file.Body)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			log.Fatalln(err)
		}
		if _, err := tw.Write([]byte(file.Body)); err != nil {
			log.Fatalln(err)
		}
	}

	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("file_%d", i)
		hdr := &tar.Header{
			Name:     filename,
			Typeflag: tar.TypeReg,
			Mode:     0600,
			Size:     int64(len(content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			log.Fatalln(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			log.Fatalln(err)
		}

	}
	// Make sure to check the error on Close.
	if err := tw.Close(); err != nil {
		log.Fatalln(err)
	}

	return buf
}
