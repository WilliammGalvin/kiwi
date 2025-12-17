package data

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

type DataManager struct {
	DataStorageDirPath string
	Symbols            []string
}

func NewDataManager(dataStorageDirPath string) (*DataManager, error) {
	info, err := os.Stat(dataStorageDirPath)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", dataStorageDirPath)
	}

	return &DataManager{
		DataStorageDirPath: dataStorageDirPath,
		Symbols:            []string{},
	}, nil
}

func isValidSymbol(sym string) bool {
	matched, _ := regexp.MatchString(`^[A-Z]{4}$`, sym)
	return matched
}

func (manager *DataManager) CollectSymbols() ([]string, error) {
	entries, err := os.ReadDir(manager.DataStorageDirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	var symbols []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !strings.HasSuffix(strings.ToLower(filename), ".csv") {
			continue
		}

		symbol := strings.TrimSuffix(filename, filepath.Ext(filename))
		if !isValidSymbol(symbol) {
			continue
		}

		symbols = append(symbols, symbol)
	}

	manager.Symbols = symbols
	return symbols, nil
}

func (manager *DataManager) GetSymbolPath(symbol string) string {
	return filepath.Join(manager.DataStorageDirPath, symbol+".csv")
}

func (manager *DataManager) HasSymbol(symbol string) bool {
	return slices.Contains(manager.Symbols, symbol)
}
