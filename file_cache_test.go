package cacheddownloader_test

import (
	"io"
	"io/ioutil"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/pivotal-golang/cacheddownloader"
)

var _ = Describe("FileCache", func() {
	var cache *FileCache
	var cacheDir string
	var err error

	var sourceFile *os.File

	BeforeEach(func() {
		cacheDir, err = ioutil.TempDir("", "cache-test")
		Expect(err).NotTo(HaveOccurred())

		cache = NewCache(cacheDir, 123424)

		sourceFile = createFile("cache-test-file", "the-file-content")
	})

	AfterEach(func() {
		os.RemoveAll(sourceFile.Name())
		os.RemoveAll(cacheDir)
	})

	Describe("Add", func() {
		var cacheKey string
		var fileSize int64
		var cacheInfo CachingInfoType
		var readCloser io.ReadCloser

		BeforeEach(func() {
			cacheKey = "the-cache-key"
			fileSize = 100
			cacheInfo = CachingInfoType{}
		})

		It("fails if room cannot be allocated", func() {
			var err error
			readCloser, err = cache.Add(cacheKey, sourceFile.Name(), 250000, cacheInfo)
			Expect(err).To(Equal(NotEnoughSpace))
			Expect(readCloser).To(BeNil())
		})

		Context("when closed is called", func() {
			JustBeforeEach(func() {
				var err error
				readCloser, err = cache.Add(cacheKey, sourceFile.Name(), fileSize, cacheInfo)
				Expect(err).NotTo(HaveOccurred())
				Expect(readCloser).NotTo(BeNil())
			})

			Context("once", func() {
				It("succeeds and has 1 file in the cache", func() {
					Expect(readCloser.Close()).NotTo(HaveOccurred())
					Expect(filenamesInDir(cacheDir)).To(HaveLen(1))
				})
			})

			Context("more than once", func() {
				It("fails", func() {
					Expect(readCloser.Close()).NotTo(HaveOccurred())
					Expect(readCloser.Close()).To(HaveOccurred())
				})
			})
		})

		Context("when the cache is empty", func() {
			JustBeforeEach(func() {
				var err error
				readCloser, err = cache.Add(cacheKey, sourceFile.Name(), fileSize, cacheInfo)
				Expect(err).NotTo(HaveOccurred())
				Expect(readCloser).NotTo(BeNil())
			})

			AfterEach(func() {
				readCloser.Close()
			})

			It("returns a reader", func() {
				content, err := ioutil.ReadAll(readCloser)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(content)).To(Equal("the-file-content"))
			})

			It("has 1 file in the cache", func() {
				Expect(filenamesInDir(cacheDir)).To(HaveLen(1))
			})
		})

		Context("when a cachekey exists", func() {
			var newSourceFile *os.File
			var newFileSize int64
			var newCacheInfo CachingInfoType
			var newReader io.ReadCloser

			BeforeEach(func() {
				newSourceFile = createFile("cache-test-file", "new-file-content")
				newFileSize = fileSize
				newCacheInfo = cacheInfo
			})

			JustBeforeEach(func() {
				readCloser, err = cache.Add(cacheKey, sourceFile.Name(), fileSize, cacheInfo)
				Expect(err).NotTo(HaveOccurred())
				Expect(readCloser).NotTo(BeNil())
			})

			AfterEach(func() {
				readCloser.Close()
				os.RemoveAll(newSourceFile.Name())
			})

			Context("when adding the same cache key with identical info", func() {
				It("ignores the add", func() {
					reader, err := cache.Add(cacheKey, newSourceFile.Name(), fileSize, cacheInfo)
					Expect(err).NotTo(HaveOccurred())
					Expect(reader).NotTo(BeNil())
				})
			})

			Context("when a adding the same cache key and different info", func() {
				JustBeforeEach(func() {
					var err error
					newReader, err = cache.Add(cacheKey, newSourceFile.Name(), newFileSize, newCacheInfo)
					Expect(err).NotTo(HaveOccurred())
					Expect(newReader).NotTo(BeNil())
				})

				AfterEach(func() {
					newReader.Close()
				})

				Context("different file size", func() {
					BeforeEach(func() {
						newFileSize = fileSize - 1
					})

					It("returns a reader for the new content", func() {
						content, err := ioutil.ReadAll(newReader)
						Expect(err).NotTo(HaveOccurred())
						Expect(string(content)).To(Equal("new-file-content"))
					})

					It("has files in the cache", func() {
						Expect(filenamesInDir(cacheDir)).To(HaveLen(2))
						Expect(readCloser.Close()).NotTo(HaveOccurred())
						Expect(filenamesInDir(cacheDir)).To(HaveLen(1))
					})

				})

				Context("different caching info", func() {
					BeforeEach(func() {
						newCacheInfo = CachingInfoType{
							LastModified: "1234",
						}
					})

					It("returns a reader for the new content", func() {
						content, err := ioutil.ReadAll(newReader)
						Expect(err).NotTo(HaveOccurred())
						Expect(string(content)).To(Equal("new-file-content"))
					})

					It("has files in the cache", func() {
						Expect(filenamesInDir(cacheDir)).To(HaveLen(2))
						Expect(readCloser.Close()).NotTo(HaveOccurred())
						Expect(filenamesInDir(cacheDir)).To(HaveLen(1))
					})

					It("still allows the previous reader to read", func() {
						content, err := ioutil.ReadAll(readCloser)
						Expect(err).NotTo(HaveOccurred())
						Expect(string(content)).To(Equal("the-file-content"))
					})
				})
			})
		})
	})

	Describe("Get", func() {
		var cacheKey string
		var fileSize int64
		var cacheInfo CachingInfoType

		BeforeEach(func() {
			cacheKey = "key"
			fileSize = 100
			cacheInfo = CachingInfoType{}
		})

		Context("when there is nothing", func() {
			It("returns nothing", func() {
				reader, ci, err := cache.Get(cacheKey)
				Expect(err).To(Equal(EntryNotFound))
				Expect(reader).To(BeNil())
				Expect(ci).To(Equal(cacheInfo))
			})
		})

		Context("when there is an item", func() {
			BeforeEach(func() {
				cacheInfo.LastModified = "1234"
				reader, err := cache.Add(cacheKey, sourceFile.Name(), fileSize, cacheInfo)
				Expect(err).NotTo(HaveOccurred())
				reader.Close()
			})

			It("returns a reader for the item", func() {
				reader, ci, err := cache.Get(cacheKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(reader).NotTo(BeNil())
				Expect(ci).To(Equal(cacheInfo))

				content, err := ioutil.ReadAll(reader)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(content)).To(Equal("the-file-content"))
			})

			Context("when the item is replaced", func() {
				var newSourceFile *os.File

				JustBeforeEach(func() {
					newSourceFile = createFile("cache-test-file", "new-file-content")

					cacheInfo.LastModified = "123"
					reader, err := cache.Add(cacheKey, newSourceFile.Name(), fileSize, cacheInfo)
					Expect(err).NotTo(HaveOccurred())
					reader.Close()
				})

				AfterEach(func() {
					os.RemoveAll(newSourceFile.Name())
				})

				It("gets the new item", func() {
					reader, ci, err := cache.Get(cacheKey)
					Expect(err).NotTo(HaveOccurred())
					Expect(reader).NotTo(BeNil())
					Expect(ci).To(Equal(cacheInfo))

					content, err := ioutil.ReadAll(reader)
					Expect(err).NotTo(HaveOccurred())
					Expect(string(content)).To(Equal("new-file-content"))
				})

				Context("when a get is issued before a replace", func() {
					var reader io.ReadCloser
					BeforeEach(func() {
						var err error
						reader, _, err = cache.Get(cacheKey)
						Expect(err).NotTo(HaveOccurred())
						Expect(reader).NotTo(BeNil())

						Expect(filenamesInDir(cacheDir)).To(HaveLen(1))
					})

					It("the old file is removed when closed", func() {
						reader.Close()
						Expect(filenamesInDir(cacheDir)).To(HaveLen(1))
					})
				})
			})
		})
	})

	Describe("Remove", func() {
		var cacheKey string
		var cacheInfo CachingInfoType

		BeforeEach(func() {
			cacheKey = "key"

			cacheInfo.LastModified = "1234"
			reader, err := cache.Add(cacheKey, sourceFile.Name(), 100, cacheInfo)
			Expect(err).NotTo(HaveOccurred())
			reader.Close()
		})

		Context("when the key does not exist", func() {
			It("does not fail", func() {
				Expect(func() { cache.Remove("bogus") }).NotTo(Panic())
			})
		})

		Context("when the key exists", func() {
			It("removes the file in the cache", func() {
				Expect(filenamesInDir(cacheDir)).To(HaveLen(1))
				cache.Remove(cacheKey)
				Expect(filenamesInDir(cacheDir)).To(HaveLen(0))
			})
		})

		Context("when a get is issued first", func() {
			It("removes the file after a close", func() {
				reader, _, err := cache.Get(cacheKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(reader).NotTo(BeNil())
				Expect(filenamesInDir(cacheDir)).To(HaveLen(1))

				cache.Remove(cacheKey)
				Expect(filenamesInDir(cacheDir)).To(HaveLen(1))

				reader.Close()
				Expect(filenamesInDir(cacheDir)).To(HaveLen(0))
			})
		})
	})
})

func createFile(filename string, content string) *os.File {
	sourceFile, err := ioutil.TempFile("", filename)
	Expect(err).NotTo(HaveOccurred())
	sourceFile.WriteString(content)
	sourceFile.Close()

	return sourceFile
}

func filenamesInDir(dir string) []string {
	entries, err := ioutil.ReadDir(dir)
	Expect(err).NotTo(HaveOccurred())

	result := []string{}
	for _, entry := range entries {
		result = append(result, entry.Name())
	}

	return result
}
