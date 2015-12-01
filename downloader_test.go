package cacheddownloader_test

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cloudfoundry-incubator/cacheddownloader"
	"github.com/onsi/gomega/ghttp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func md5HexEtag(content string) string {
	contentHash := md5.New()
	contentHash.Write([]byte(content))
	return fmt.Sprintf(`"%x"`, contentHash.Sum(nil))
}

var _ = Describe("Downloader", func() {
	var downloader *cacheddownloader.Downloader
	var testServer *httptest.Server
	var serverRequestUrls []string
	var lock *sync.Mutex
	var cancelChan chan struct{}

	createDestFile := func() (*os.File, error) {
		return ioutil.TempFile("", "foo")
	}

	BeforeEach(func() {
		testServer = nil
		downloader = cacheddownloader.NewDownloader(100*time.Millisecond, 10, false)
		lock = &sync.Mutex{}
		cancelChan = make(chan struct{}, 0)
	})

	Describe("Download", func() {
		var serverUrl *url.URL

		BeforeEach(func() {
			serverRequestUrls = []string{}
		})

		AfterEach(func() {
			if testServer != nil {
				testServer.Close()
			}
		})

		Context("when the download is successful", func() {
			var (
				expectedSize int64

				downloadErr    error
				downloadedFile string

				downloadCachingInfo cacheddownloader.CachingInfoType
				expectedCachingInfo cacheddownloader.CachingInfoType
				expectedEtag        string
			)

			AfterEach(func() {
				if downloadedFile != "" {
					os.Remove(downloadedFile)
				}
			})

			JustBeforeEach(func() {
				serverUrl, _ = url.Parse(testServer.URL + "/somepath")
				downloadedFile, downloadCachingInfo, downloadErr = downloader.Download(serverUrl, createDestFile, cacheddownloader.CachingInfoType{}, cancelChan)
			})

			Context("and contains a matching MD5 Hash in the Etag", func() {
				var attempts int
				BeforeEach(func() {
					attempts = 0
					testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						lock.Lock()
						serverRequestUrls = append(serverRequestUrls, r.RequestURI)
						attempts++
						lock.Unlock()

						msg := "Hello, client"
						expectedCachingInfo = cacheddownloader.CachingInfoType{
							ETag:         md5HexEtag(msg),
							LastModified: "The 70s",
						}
						w.Header().Set("ETag", expectedCachingInfo.ETag)
						w.Header().Set("Last-Modified", expectedCachingInfo.LastModified)

						bytesWritten, _ := fmt.Fprint(w, msg)
						expectedSize = int64(bytesWritten)
					}))
				})

				It("does not return an error", func() {
					Expect(downloadErr).NotTo(HaveOccurred())
				})

				It("only tries once", func() {
					Expect(attempts).To(Equal(1))
				})

				It("claims to have downloaded", func() {
					Expect(downloadedFile).NotTo(BeEmpty())
				})

				It("gets a file from a url", func() {
					lock.Lock()
					urlFromServer := testServer.URL + serverRequestUrls[0]
					Expect(urlFromServer).To(Equal(serverUrl.String()))
					lock.Unlock()
				})

				It("should use the provided file as the download location", func() {
					fileContents, _ := ioutil.ReadFile(downloadedFile)
					Expect(fileContents).To(ContainSubstring("Hello, client"))
				})

				It("returns the ETag", func() {
					Expect(downloadCachingInfo).To(Equal(expectedCachingInfo))
				})
			})

			Context("and contains an Etag that is not an MD5 Hash ", func() {
				BeforeEach(func() {
					expectedEtag = "not the hex you are looking for"
					testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("ETag", expectedEtag)
						bytesWritten, _ := fmt.Fprint(w, "Hello, client")
						expectedSize = int64(bytesWritten)
					}))
				})

				It("succeeds without doing a checksum", func() {
					Expect(downloadedFile).NotTo(BeEmpty())
					Expect(downloadErr).NotTo(HaveOccurred())
				})

				It("should returns the ETag in the caching info", func() {
					Expect(downloadCachingInfo.ETag).To(Equal(expectedEtag))
				})
			})

			Context("and contains no Etag at all", func() {
				BeforeEach(func() {
					testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						bytesWritten, _ := fmt.Fprint(w, "Hello, client")
						expectedSize = int64(bytesWritten)
					}))
				})

				It("succeeds without doing a checksum", func() {
					Expect(downloadedFile).NotTo(BeEmpty())
					Expect(downloadErr).NotTo(HaveOccurred())
				})

				It("should returns no ETag in the caching info", func() {
					Expect(downloadCachingInfo).To(BeZero())
				})
			})
		})

		Context("when the download times out", func() {
			var requestInitiated chan struct{}

			BeforeEach(func() {
				requestInitiated = make(chan struct{})

				testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requestInitiated <- struct{}{}

					time.Sleep(300 * time.Millisecond)
					fmt.Fprint(w, "Hello, client")
				}))

				serverUrl, _ = url.Parse(testServer.URL + "/somepath")
			})

			It("should retry 3 times and return an error", func() {
				errs := make(chan error)
				downloadedFiles := make(chan string)

				go func() {
					downloadedFile, _, err := downloader.Download(serverUrl, createDestFile, cacheddownloader.CachingInfoType{}, cancelChan)
					errs <- err
					downloadedFiles <- downloadedFile
				}()

				Eventually(requestInitiated).Should(Receive())
				Eventually(requestInitiated).Should(Receive())
				Eventually(requestInitiated).Should(Receive())

				Expect(<-errs).To(HaveOccurred())
				Expect(<-downloadedFiles).To(BeEmpty())
			})
		})

		Context("when the download fails with a protocol error", func() {
			BeforeEach(func() {
				// No server to handle things!
				serverUrl, _ = url.Parse("http://127.0.0.1:54321/somepath")
			})

			It("should return the error", func() {
				downloadedFile, _, err := downloader.Download(serverUrl, createDestFile, cacheddownloader.CachingInfoType{}, cancelChan)
				Expect(err).To(HaveOccurred())
				Expect(downloadedFile).To(BeEmpty())
			})
		})

		Context("when the download fails with a status code error", func() {
			BeforeEach(func() {
				testServer = httptest.NewServer(http.NotFoundHandler())

				serverUrl, _ = url.Parse(testServer.URL + "/somepath")
			})

			It("should return the error", func() {
				downloadedFile, _, err := downloader.Download(serverUrl, createDestFile, cacheddownloader.CachingInfoType{}, cancelChan)
				Expect(err).To(HaveOccurred())
				Expect(downloadedFile).To(BeEmpty())
			})
		})

		Context("when the read exceeds the deadline timeout", func() {
			var done chan struct{}

			BeforeEach(func() {
				done = make(chan struct{}, 3)
				downloader = cacheddownloader.NewDownloaderWithDeadline(1*time.Second, 30*time.Millisecond, 10, false)

				testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(100 * time.Millisecond)
					done <- struct{}{}
				}))

				serverUrl, _ = url.Parse(testServer.URL + "/somepath")
			})

			AfterEach(func() {
				Eventually(done).Should(HaveLen(3))
			})

			It("fails with a nested read error", func() {
				errs := make(chan error)

				go func() {
					_, _, err := downloader.Download(serverUrl, createDestFile, cacheddownloader.CachingInfoType{}, cancelChan)
					errs <- err
				}()

				var err error
				Eventually(errs).Should(Receive(&err))
				uErr, ok := err.(*url.Error)
				Expect(ok).To(BeTrue())
				opErr, ok := uErr.Err.(*net.OpError)
				Expect(ok).To(BeTrue())
				Expect(opErr.Op).To(Equal("read"))
			})
		})

		Context("when the download's ETag fails the checksum", func() {
			BeforeEach(func() {
				testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					realMsg := "Hello, client"
					incompleteMsg := "Hello, clien"

					w.Header().Set("ETag", md5HexEtag(realMsg))

					fmt.Fprint(w, incompleteMsg)
				}))

				serverUrl, _ = url.Parse(testServer.URL + "/somepath")
			})

			It("should return an error", func() {
				downloadedFile, cachingInfo, err := downloader.Download(serverUrl, createDestFile, cacheddownloader.CachingInfoType{}, cancelChan)
				Expect(err.Error()).To(ContainSubstring("Checksum"))
				Expect(downloadedFile).To(BeEmpty())
				Expect(cachingInfo).To(BeZero())
			})
		})

		Context("when the Content-Length does not match the downloaded file size", func() {
			BeforeEach(func() {
				testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					realMsg := "Hello, client"
					incompleteMsg := "Hello, clientsss"

					w.Header().Set("Content-Length", strconv.Itoa(len(realMsg)))

					fmt.Fprint(w, incompleteMsg)
				}))

				serverUrl, _ = url.Parse(testServer.URL + "/somepath")
			})

			It("should return an error", func() {
				downloadedFile, cachingInfo, err := downloader.Download(serverUrl, createDestFile, cacheddownloader.CachingInfoType{}, cancelChan)
				Expect(err).To(HaveOccurred())
				Expect(downloadedFile).To(BeEmpty())
				Expect(cachingInfo).To(BeZero())
			})
		})

		Context("when cancelling", func() {
			var requestInitiated chan struct{}
			var completeRequest chan struct{}

			BeforeEach(func() {
				requestInitiated = make(chan struct{})
				completeRequest = make(chan struct{})

				testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requestInitiated <- struct{}{}
					<-completeRequest
					w.Write(bytes.Repeat([]byte("a"), 1024))
					w.(http.Flusher).Flush()
					<-completeRequest
				}))

				serverUrl, _ = url.Parse(testServer.URL + "/somepath")
			})

			It("cancels the request", func() {
				errs := make(chan error)

				go func() {
					_, _, err := downloader.Download(serverUrl, createDestFile, cacheddownloader.CachingInfoType{}, cancelChan)
					errs <- err
				}()

				Eventually(requestInitiated).Should(Receive())
				close(cancelChan)

				Eventually(errs).Should(Receive(BeAssignableToTypeOf(cacheddownloader.NewDownloadCancelledError("", 0, cacheddownloader.NoBytesReceived))))

				close(completeRequest)
			})

			It("stops the download", func() {
				errs := make(chan error)

				go func() {
					_, _, err := downloader.Download(serverUrl, createDestFile, cacheddownloader.CachingInfoType{}, cancelChan)
					errs <- err
				}()

				Eventually(requestInitiated).Should(Receive())
				completeRequest <- struct{}{}
				close(cancelChan)

				Eventually(errs).Should(Receive(BeAssignableToTypeOf(cacheddownloader.NewDownloadCancelledError("", 0, cacheddownloader.NoBytesReceived))))
				close(completeRequest)
			})
		})
	})

	Describe("Concurrent downloads", func() {
		var (
			server    *ghttp.Server
			serverUrl *url.URL
			barrier   chan interface{}
			results   chan bool
			tempDir   string
		)

		BeforeEach(func() {
			barrier = make(chan interface{}, 1)
			results = make(chan bool, 1)

			downloader = cacheddownloader.NewDownloader(1*time.Second, 1, false)

			var err error
			tempDir, err = ioutil.TempDir("", "temp-dl-dir")
			Expect(err).NotTo(HaveOccurred())

			server = ghttp.NewServer()
			server.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/the-file"),
					http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						barrier <- nil
						Consistently(results, .5).ShouldNot(Receive())
					}),
					ghttp.RespondWith(http.StatusOK, "download content", http.Header{}),
				),
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/the-file"),
					http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						results <- true
					}),
					ghttp.RespondWith(http.StatusOK, "download content", http.Header{}),
				),
			)

			serverUrl, _ = url.Parse(server.URL() + "/the-file")
		})

		AfterEach(func() {
			server.Close()
			os.RemoveAll(tempDir)
		})

		downloadTestFile := func(cancelChan <-chan struct{}) (path string, cachingInfoOut cacheddownloader.CachingInfoType, err error) {
			return downloader.Download(
				serverUrl,
				func() (*os.File, error) {
					return ioutil.TempFile(tempDir, "the-file")
				},
				cacheddownloader.CachingInfoType{},
				cancelChan,
			)
		}

		It("only allows n downloads at the same time", func() {
			go func() {
				downloadTestFile(make(chan struct{}, 0))
				barrier <- nil
			}()

			<-barrier
			downloadTestFile(cancelChan)
			<-barrier
		})

		Context("when cancelling", func() {
			It("bails when waiting", func() {
				go func() {
					downloadTestFile(make(chan struct{}, 0))
					barrier <- nil
				}()

				<-barrier
				cancelChan := make(chan struct{}, 0)
				close(cancelChan)
				_, _, err := downloadTestFile(cancelChan)
				Expect(err).To(BeAssignableToTypeOf(cacheddownloader.NewDownloadCancelledError("", 0, cacheddownloader.NoBytesReceived)))
				<-barrier
			})
		})

		Context("Downloading with caching info", func() {
			var (
				server     *ghttp.Server
				cachedInfo cacheddownloader.CachingInfoType
				statusCode int
				serverUrl  *url.URL
				body       string
			)

			BeforeEach(func() {
				cachedInfo = cacheddownloader.CachingInfoType{
					ETag:         "It's Just a Flesh Wound",
					LastModified: "The 60s",
				}

				server = ghttp.NewServer()
				server.AppendHandlers(ghttp.CombineHandlers(
					ghttp.VerifyRequest("GET", "/get-the-file"),
					ghttp.VerifyHeader(http.Header{
						"If-None-Match":     []string{cachedInfo.ETag},
						"If-Modified-Since": []string{cachedInfo.LastModified},
					}),
					ghttp.RespondWithPtr(&statusCode, &body),
				))

				serverUrl, _ = url.Parse(server.URL() + "/get-the-file")
			})

			AfterEach(func() {
				server.Close()
			})

			Context("when the server replies with 304", func() {
				BeforeEach(func() {
					statusCode = http.StatusNotModified
				})

				It("should return that it did not download", func() {
					downloadedFile, _, err := downloader.Download(serverUrl, createDestFile, cachedInfo, cancelChan)
					Expect(downloadedFile).To(BeEmpty())
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("when the server replies with 200", func() {
				var (
					downloadedFile string
					err            error
				)

				BeforeEach(func() {
					statusCode = http.StatusOK
					body = "quarb!"
				})

				AfterEach(func() {
					if downloadedFile != "" {
						os.Remove(downloadedFile)
					}
				})

				It("should download the file", func() {
					downloadedFile, _, err = downloader.Download(serverUrl, createDestFile, cachedInfo, cancelChan)
					Expect(err).NotTo(HaveOccurred())

					info, err := os.Stat(downloadedFile)
					Expect(err).NotTo(HaveOccurred())
					Expect(info.Size()).To(Equal(int64(len(body))))
				})
			})

			Context("for anything else (including a server error)", func() {
				BeforeEach(func() {
					statusCode = http.StatusInternalServerError

					// cope with built in retry
					for i := 0; i < cacheddownloader.MAX_DOWNLOAD_ATTEMPTS; i++ {
						server.AppendHandlers(ghttp.CombineHandlers(
							ghttp.VerifyRequest("GET", "/get-the-file"),
							ghttp.VerifyHeader(http.Header{
								"If-None-Match":     []string{cachedInfo.ETag},
								"If-Modified-Since": []string{cachedInfo.LastModified},
							}),
							ghttp.RespondWithPtr(&statusCode, &body),
						))
					}
				})

				It("should return false with an error", func() {
					downloadedFile, _, err := downloader.Download(serverUrl, createDestFile, cachedInfo, cancelChan)
					Expect(downloadedFile).To(BeEmpty())
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})
})
