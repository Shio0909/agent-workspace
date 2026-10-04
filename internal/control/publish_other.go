//go:build !darwin && !linux

package control

import "errors"

// 不支持原子不覆盖发布的平台拒绝操作，不能退回可能覆盖数据的 rename。
func publishNoReplace(from, to string) error {
	return errors.New("atomic no-replace publication requires Linux or macOS")
}
