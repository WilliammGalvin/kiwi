package reader

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

type CSVReader struct {
	FilePath      string
	Headers       []string
	Delimiter     rune
	HasReachedEOF bool
	Index         int
	file          *os.File
	csvReader     *csv.Reader
}

func NewCSVReader(filePath string) *CSVReader {
	return &CSVReader{
		FilePath:      filePath,
		Delimiter:     ',',
		HasReachedEOF: false,
		Index:         0,
	}
}

func NewCSVReaderWithDelimiter(filePath string, delimiter rune) *CSVReader {
	return &CSVReader{
		FilePath:      filePath,
		Delimiter:     delimiter,
		HasReachedEOF: false,
		Index:         0,
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
	reader.Index++
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
			log.Printf("Symbol %s didn't match\n", s)
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

	reader.Index++
	return record, nil
}
