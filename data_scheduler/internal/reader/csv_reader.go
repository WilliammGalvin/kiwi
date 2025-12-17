package reader

import (
	"encoding/csv"
	"fmt"
	"os"
)

type CSVReader struct {
	FilePath      string
	Headers       []string
	Delimiter     rune
	HasReachedEOF bool
	file          *os.File
	csvReader     *csv.Reader
	index         int
}

func NewCSVReader(filePath string) *CSVReader {
	return &CSVReader{
		FilePath:      filePath,
		Delimiter:     ',',
		HasReachedEOF: false,
		index:         0,
	}
}

func NewCSVReaderWithDelimiter(filePath string, delimiter rune) *CSVReader {
	return &CSVReader{
		FilePath:      filePath,
		Delimiter:     delimiter,
		HasReachedEOF: false,
		index:         0,
	}
}

func (reader *CSVReader) OpenFile() error {
	file, err := os.Open(reader.FilePath)
	if err != nil {
		return err
	}

	reader.file = file
	reader.csvReader = csv.NewReader(file)
	reader.csvReader.Comma = reader.Delimiter
	return nil
}

func (reader *CSVReader) CloseFile() error {
	if reader.file != nil {
		return reader.file.Close()
	}

	return nil
}

func (reader *CSVReader) ReadHeaders() ([]string, error) {
	headers, err := reader.csvReader.Read()
	if err != nil {
		return nil, err
	}

	reader.Headers = headers
	reader.index++
	return headers, nil
}

func (reader *CSVReader) VerifyHeaders(validHeaders []string) bool {
	if len(validHeaders) != len(reader.Headers) {
		return false
	}

	counts := make(map[string]int)
	for _, s := range validHeaders {
		counts[s]++
	}

	for _, s := range validHeaders {
		counts[s]++
	}

	for _, s := range reader.Headers {
		counts[s]--
		if counts[s] < 0 {
			return false
		}
	}

	return true
}

func (reader *CSVReader) ReadRow() ([]string, error) {
	if reader.HasReachedEOF {
		return nil, fmt.Errorf("already reached EOF")
	}

	record, err := reader.csvReader.Read()
	if err != nil {
		if err.Error() == "EOF" {
			reader.HasReachedEOF = true
			return nil, nil
		}

		return nil, err
	}

	reader.index++
	return record, nil
}
