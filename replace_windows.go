// “Copyright (c) 2015 Pivotal Software, Inc. All Rights Reserved.  Permission
// is hereby granted, free of charge, to any person obtaining a copy of this
// software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions: The above
// copyright notice and this permission notice shall be included in all copies
// or substantial portions of the Software.  THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
// CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
// SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE."

package cacheddownloader

import (
	"syscall"
	"unsafe"
)

// Replaces `dst' with `src' atomically. Under linux we only have to
// call os.Rename(), on windows os.Rename() will error if the
// destination exists already. The replace function serves as a
// unified interface on both platforms.
func replace(src, dst string) error {
	kernel32, err := syscall.LoadLibrary("kernel32.dll")
	if err != nil {
		return err
	}
	defer syscall.FreeLibrary(kernel32)
	moveFileExUnicode, err := syscall.GetProcAddress(kernel32, "MoveFileExW")
	if err != nil {
		return err
	}

	srcString, err := syscall.UTF16PtrFromString(src)
	if err != nil {
		return err
	}

	dstString, err := syscall.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}

	srcPtr := uintptr(unsafe.Pointer(srcString))
	dstPtr := uintptr(unsafe.Pointer(dstString))

	MOVEFILE_REPLACE_EXISTING := 0x1
	flag := uintptr(MOVEFILE_REPLACE_EXISTING)

	_, _, callErr := syscall.Syscall(uintptr(moveFileExUnicode), 3, srcPtr, dstPtr, flag)
	if callErr != 0 {
		return callErr
	}

	return nil
}
