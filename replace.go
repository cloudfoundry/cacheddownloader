// +build !windows

package cacheddownloader

import "os"

func replace(src, dst string) error {
	return os.Rename(src, dst)
}
