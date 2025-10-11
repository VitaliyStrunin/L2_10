package chunk

import (
	"bufio"
	"errors"
	"os"
	"runtime"
)

func ReadChunks(filename string, maxChunkSize int) (<-chan []string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	chunkChan := make(chan []string, 1)
	go func() {
		defer close(chunkChan)

		scanner := bufio.NewScanner(file)
		var chunk []string
		var currentSize int
		gcCounter := 0
		for scanner.Scan() {
			line := scanner.Text()
			estimatedSize := len(line) + 16
			if len(chunk) > 0 && currentSize+estimatedSize > maxChunkSize {
				chunkChan <- chunk
				chunk = nil
				gcCounter++
				if gcCounter == 3 {
					runtime.GC() // из-за того, что память не очищается немедленно, реальное потребление RAM сильно увеличивается, приходится вызывать GC вручную
					gcCounter = 0
				}
				currentSize = 0
			}

			if chunk == nil {
				chunk = make([]string, 0, 1000)
			}
			chunk = append(chunk, line)
			currentSize += estimatedSize
		}

		if len(chunk) > 0 {
			chunkChan <- chunk
		}

		err := file.Close()
		if errors.Is(err, os.ErrClosed) {
			return
		}
	}()
	return chunkChan, nil
}
