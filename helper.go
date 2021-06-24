package main

import (
	"path/filepath"
	"strconv"
	"strings"
)

func fileNameWithoutExtension(fileName string) string {
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

func fileNameWithoutSuffix(fileName string, suffix string) string {
	return strings.TrimSuffix(fileName, suffix)
}

func convertIndex(index string) (int64, error) {
	i, err := strconv.ParseInt(index, 10, 64)
	if err != nil {
		return -1, err
	}
	return i, nil
}
