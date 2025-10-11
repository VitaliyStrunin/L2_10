package sorter

import (
	"StringSort/internal/sorter/chunk"
	"StringSort/internal/sorter/util"
	"StringSort/internal/tempfiles"
	"bufio"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
)

func SortLines(lines []string, options util.Options) {
	sort.Slice(lines, func(i, j int) bool {
		a, b := lines[i], lines[j]
		return util.Compare(a, b, options)
	})
}

func ExternalSortLines(filename string, options util.Options, files *tempfiles.RegisteredTempFiles) error {
	defer files.Cleanup()
	tempFiles, err := sortChunksParallel(filename, options, runtime.NumCPU(), files)
	if err != nil {
		return err
	}
	err = mergeSortedFiles(tempFiles, options)
	if err != nil {
		return err
	}

	return nil
}

func sortChunksParallel(filename string, options util.Options, maxGoroutines int, files *tempfiles.RegisteredTempFiles) ([]string, error) {
	maxChunkSize := util.GetSafeAvailableRAM() / (maxGoroutines * 2)
	chunkChan, err := chunk.ReadChunks(filename, maxChunkSize)
	if err != nil {
		return nil, err
	}
	tempfileChan := make(chan string, maxGoroutines)
	errorsChan := make(chan error, 1)
	sortChan := make(chan struct{}, maxGoroutines)
	done := make(chan struct{})
	defer close(errorsChan)
	defer close(sortChan)

	var tempFiles []string
	var wg sync.WaitGroup

	go func() {
		for f := range tempfileChan {
			tempFiles = append(tempFiles, f)
		}
		close(done)
	}()

	for currentChunk := range chunkChan {
		wg.Add(1)
		sortChan <- struct{}{}
		go func(data []string) {
			defer func() {
				<-sortChan
			}()
			defer wg.Done()
			SortLines(data, options)
			tempFile, err := os.CreateTemp("", "sort_chunk_*.tmp")
			files.Register(tempFile.Name())
			if err != nil {
				errorsChan <- err
				return
			}
			writer := bufio.NewWriterSize(tempFile, 4*1024*1024)
			for _, line := range data {
				_, writeError := writer.WriteString(line + "\n")
				if writeError != nil {
					closeError := tempFile.Close()
					if closeError != nil {
						errorsChan <- closeError
						return
					}
					errorsChan <- writeError
					return
				}
			}
			flushErr := writer.Flush()
			if flushErr != nil {
				errorsChan <- flushErr
				return
			}
			closeErr := tempFile.Close()
			if closeErr != nil {
				errorsChan <- closeErr
				return
			}
			tempfileChan <- tempFile.Name()
			fmt.Println("Отсортирован чанк")
		}(currentChunk)
	}
	wg.Wait()
	close(tempfileChan)
	<-done
	select {
	case err := <-errorsChan:
		return nil, err
	default:
	}
	wg.Wait()
	return tempFiles, nil
}
