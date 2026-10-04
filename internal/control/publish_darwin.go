//go:build darwin

package control

import "golang.org/x/sys/unix"

// 发布动作本身拒绝覆盖，包括空目录和符号链接；不做检查后普通 rename。
func publishNoReplace(from, to string) error {
	return unix.RenamexNp(from, to, unix.RENAME_EXCL)
}
