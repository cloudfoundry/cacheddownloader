// This file was generated by counterfeiter
package cacheddownloaderfakes

import (
	"io"
	"net/url"
	"sync"

	"code.cloudfoundry.org/cacheddownloader"
)

type FakeCachedDownloader struct {
	FetchStub        func(urlToFetch *url.URL, cacheKey string, checksum cacheddownloader.ChecksumInfoType, cancelChan <-chan struct{}) (stream io.ReadCloser, size int64, err error)
	fetchMutex       sync.RWMutex
	fetchArgsForCall []struct {
		urlToFetch *url.URL
		cacheKey   string
		checksum   cacheddownloader.ChecksumInfoType
		cancelChan <-chan struct{}
	}
	fetchReturns struct {
		result1 io.ReadCloser
		result2 int64
		result3 error
	}
	FetchAsDirectoryStub        func(urlToFetch *url.URL, cacheKey string, checksum cacheddownloader.ChecksumInfoType, cancelChan <-chan struct{}) (dirPath string, size int64, err error)
	fetchAsDirectoryMutex       sync.RWMutex
	fetchAsDirectoryArgsForCall []struct {
		urlToFetch *url.URL
		cacheKey   string
		checksum   cacheddownloader.ChecksumInfoType
		cancelChan <-chan struct{}
	}
	fetchAsDirectoryReturns struct {
		result1 string
		result2 int64
		result3 error
	}
	CloseDirectoryStub        func(cacheKey, directoryPath string) error
	closeDirectoryMutex       sync.RWMutex
	closeDirectoryArgsForCall []struct {
		cacheKey      string
		directoryPath string
	}
	closeDirectoryReturns struct {
		result1 error
	}
	CacheLocationStub        func() string
	cacheLocationMutex       sync.RWMutex
	cacheLocationArgsForCall []struct{}
	cacheLocationReturns     struct {
		result1 string
	}
	SaveStateStub        func() error
	saveStateMutex       sync.RWMutex
	saveStateArgsForCall []struct{}
	saveStateReturns     struct {
		result1 error
	}
	RecoverStateStub        func() error
	recoverStateMutex       sync.RWMutex
	recoverStateArgsForCall []struct{}
	recoverStateReturns     struct {
		result1 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeCachedDownloader) Fetch(urlToFetch *url.URL, cacheKey string, checksum cacheddownloader.ChecksumInfoType, cancelChan <-chan struct{}) (stream io.ReadCloser, size int64, err error) {
	fake.fetchMutex.Lock()
	fake.fetchArgsForCall = append(fake.fetchArgsForCall, struct {
		urlToFetch *url.URL
		cacheKey   string
		checksum   cacheddownloader.ChecksumInfoType
		cancelChan <-chan struct{}
	}{urlToFetch, cacheKey, checksum, cancelChan})
	fake.recordInvocation("Fetch", []interface{}{urlToFetch, cacheKey, checksum, cancelChan})
	fake.fetchMutex.Unlock()
	if fake.FetchStub != nil {
		return fake.FetchStub(urlToFetch, cacheKey, checksum, cancelChan)
	} else {
		return fake.fetchReturns.result1, fake.fetchReturns.result2, fake.fetchReturns.result3
	}
}

func (fake *FakeCachedDownloader) FetchCallCount() int {
	fake.fetchMutex.RLock()
	defer fake.fetchMutex.RUnlock()
	return len(fake.fetchArgsForCall)
}

func (fake *FakeCachedDownloader) FetchArgsForCall(i int) (*url.URL, string, cacheddownloader.ChecksumInfoType, <-chan struct{}) {
	fake.fetchMutex.RLock()
	defer fake.fetchMutex.RUnlock()
	return fake.fetchArgsForCall[i].urlToFetch, fake.fetchArgsForCall[i].cacheKey, fake.fetchArgsForCall[i].checksum, fake.fetchArgsForCall[i].cancelChan
}

func (fake *FakeCachedDownloader) FetchReturns(result1 io.ReadCloser, result2 int64, result3 error) {
	fake.FetchStub = nil
	fake.fetchReturns = struct {
		result1 io.ReadCloser
		result2 int64
		result3 error
	}{result1, result2, result3}
}

func (fake *FakeCachedDownloader) FetchAsDirectory(urlToFetch *url.URL, cacheKey string, checksum cacheddownloader.ChecksumInfoType, cancelChan <-chan struct{}) (dirPath string, size int64, err error) {
	fake.fetchAsDirectoryMutex.Lock()
	fake.fetchAsDirectoryArgsForCall = append(fake.fetchAsDirectoryArgsForCall, struct {
		urlToFetch *url.URL
		cacheKey   string
		checksum   cacheddownloader.ChecksumInfoType
		cancelChan <-chan struct{}
	}{urlToFetch, cacheKey, checksum, cancelChan})
	fake.recordInvocation("FetchAsDirectory", []interface{}{urlToFetch, cacheKey, checksum, cancelChan})
	fake.fetchAsDirectoryMutex.Unlock()
	if fake.FetchAsDirectoryStub != nil {
		return fake.FetchAsDirectoryStub(urlToFetch, cacheKey, checksum, cancelChan)
	} else {
		return fake.fetchAsDirectoryReturns.result1, fake.fetchAsDirectoryReturns.result2, fake.fetchAsDirectoryReturns.result3
	}
}

func (fake *FakeCachedDownloader) FetchAsDirectoryCallCount() int {
	fake.fetchAsDirectoryMutex.RLock()
	defer fake.fetchAsDirectoryMutex.RUnlock()
	return len(fake.fetchAsDirectoryArgsForCall)
}

func (fake *FakeCachedDownloader) FetchAsDirectoryArgsForCall(i int) (*url.URL, string, cacheddownloader.ChecksumInfoType, <-chan struct{}) {
	fake.fetchAsDirectoryMutex.RLock()
	defer fake.fetchAsDirectoryMutex.RUnlock()
	return fake.fetchAsDirectoryArgsForCall[i].urlToFetch, fake.fetchAsDirectoryArgsForCall[i].cacheKey, fake.fetchAsDirectoryArgsForCall[i].checksum, fake.fetchAsDirectoryArgsForCall[i].cancelChan
}

func (fake *FakeCachedDownloader) FetchAsDirectoryReturns(result1 string, result2 int64, result3 error) {
	fake.FetchAsDirectoryStub = nil
	fake.fetchAsDirectoryReturns = struct {
		result1 string
		result2 int64
		result3 error
	}{result1, result2, result3}
}

func (fake *FakeCachedDownloader) CloseDirectory(cacheKey string, directoryPath string) error {
	fake.closeDirectoryMutex.Lock()
	fake.closeDirectoryArgsForCall = append(fake.closeDirectoryArgsForCall, struct {
		cacheKey      string
		directoryPath string
	}{cacheKey, directoryPath})
	fake.recordInvocation("CloseDirectory", []interface{}{cacheKey, directoryPath})
	fake.closeDirectoryMutex.Unlock()
	if fake.CloseDirectoryStub != nil {
		return fake.CloseDirectoryStub(cacheKey, directoryPath)
	} else {
		return fake.closeDirectoryReturns.result1
	}
}

func (fake *FakeCachedDownloader) CloseDirectoryCallCount() int {
	fake.closeDirectoryMutex.RLock()
	defer fake.closeDirectoryMutex.RUnlock()
	return len(fake.closeDirectoryArgsForCall)
}

func (fake *FakeCachedDownloader) CloseDirectoryArgsForCall(i int) (string, string) {
	fake.closeDirectoryMutex.RLock()
	defer fake.closeDirectoryMutex.RUnlock()
	return fake.closeDirectoryArgsForCall[i].cacheKey, fake.closeDirectoryArgsForCall[i].directoryPath
}

func (fake *FakeCachedDownloader) CloseDirectoryReturns(result1 error) {
	fake.CloseDirectoryStub = nil
	fake.closeDirectoryReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCachedDownloader) CacheLocation() string {
	fake.cacheLocationMutex.Lock()
	fake.cacheLocationArgsForCall = append(fake.cacheLocationArgsForCall, struct{}{})
	fake.recordInvocation("CacheLocation", []interface{}{})
	fake.cacheLocationMutex.Unlock()
	if fake.CacheLocationStub != nil {
		return fake.CacheLocationStub()
	} else {
		return fake.cacheLocationReturns.result1
	}
}

func (fake *FakeCachedDownloader) CacheLocationCallCount() int {
	fake.cacheLocationMutex.RLock()
	defer fake.cacheLocationMutex.RUnlock()
	return len(fake.cacheLocationArgsForCall)
}

func (fake *FakeCachedDownloader) CacheLocationReturns(result1 string) {
	fake.CacheLocationStub = nil
	fake.cacheLocationReturns = struct {
		result1 string
	}{result1}
}

func (fake *FakeCachedDownloader) SaveState() error {
	fake.saveStateMutex.Lock()
	fake.saveStateArgsForCall = append(fake.saveStateArgsForCall, struct{}{})
	fake.recordInvocation("SaveState", []interface{}{})
	fake.saveStateMutex.Unlock()
	if fake.SaveStateStub != nil {
		return fake.SaveStateStub()
	} else {
		return fake.saveStateReturns.result1
	}
}

func (fake *FakeCachedDownloader) SaveStateCallCount() int {
	fake.saveStateMutex.RLock()
	defer fake.saveStateMutex.RUnlock()
	return len(fake.saveStateArgsForCall)
}

func (fake *FakeCachedDownloader) SaveStateReturns(result1 error) {
	fake.SaveStateStub = nil
	fake.saveStateReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCachedDownloader) RecoverState() error {
	fake.recoverStateMutex.Lock()
	fake.recoverStateArgsForCall = append(fake.recoverStateArgsForCall, struct{}{})
	fake.recordInvocation("RecoverState", []interface{}{})
	fake.recoverStateMutex.Unlock()
	if fake.RecoverStateStub != nil {
		return fake.RecoverStateStub()
	} else {
		return fake.recoverStateReturns.result1
	}
}

func (fake *FakeCachedDownloader) RecoverStateCallCount() int {
	fake.recoverStateMutex.RLock()
	defer fake.recoverStateMutex.RUnlock()
	return len(fake.recoverStateArgsForCall)
}

func (fake *FakeCachedDownloader) RecoverStateReturns(result1 error) {
	fake.RecoverStateStub = nil
	fake.recoverStateReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeCachedDownloader) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.fetchMutex.RLock()
	defer fake.fetchMutex.RUnlock()
	fake.fetchAsDirectoryMutex.RLock()
	defer fake.fetchAsDirectoryMutex.RUnlock()
	fake.closeDirectoryMutex.RLock()
	defer fake.closeDirectoryMutex.RUnlock()
	fake.cacheLocationMutex.RLock()
	defer fake.cacheLocationMutex.RUnlock()
	fake.saveStateMutex.RLock()
	defer fake.saveStateMutex.RUnlock()
	fake.recoverStateMutex.RLock()
	defer fake.recoverStateMutex.RUnlock()
	return fake.invocations
}

func (fake *FakeCachedDownloader) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ cacheddownloader.CachedDownloader = new(FakeCachedDownloader)
