package main

import (
	"bufio"
	"container/heap"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/shirou/gopsutil/v3/mem"
	"github.com/spf13/pflag"
)

var (
	numberRegex         = regexp.MustCompile(`^[-+]?(?:\d+\.?\d*|\.\d+)`)
	globalOptions       Options
	registeredTempFiles *RegisteredTempFiles
)

type RegisteredTempFiles struct {
	files []string
	mutex sync.Mutex
}

func (rf *RegisteredTempFiles) Register(file string) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	rf.files = append(rf.files, file)
}

func (rf *RegisteredTempFiles) Cleanup() {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	for _, file := range rf.files {
		_ = os.Remove(file)
	}
	rf.files = make([]string, 0)
}

type MergeItem struct {
	line     string
	scanner  *bufio.Scanner
	filename string
}

type MinHeap []*MergeItem

func (h *MinHeap) Len() int { return len(*h) }

func (h *MinHeap) Less(i, j int) bool {
	a, b := (*h)[i].line, (*h)[j].line
	options := globalOptions
	if options.IgnoreBlanks {
		a = strings.TrimRight(a, " \t")
		b = strings.TrimRight(b, " \t")
	}
	if options.SortColumn > 0 {
		fieldsA := strings.Split(a, "\t")
		fieldsB := strings.Split(b, "\t")
		columnIndex := options.SortColumn - 1
		if columnIndex < len(fieldsA) {
			a = fieldsA[columnIndex]
		} else {
			a = ""
		}
		if columnIndex < len(fieldsB) {
			b = fieldsB[columnIndex]
		} else {
			b = ""
		}
	}
	less := false
	switch {
	case options.Numeric:
		numberFromA := extractFirstNumber(a)
		numberFromB := extractFirstNumber(b)
		if numberFromA < numberFromB {
			less = true
		}
	case options.Month:
		monthFromA := extractMonth(a)
		monthFromB := extractMonth(b)
		if monthFromA < monthFromB {
			less = true
		}
	default:
		less = a < b
	}

	if options.Reverse {
		less = !less
	}

	return less
}

func (h *MinHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}
func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(*MergeItem))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func mergeSortedFiles(tempFiles []string, options Options) error {
	setGlobalOptions(options)
	var h MinHeap
	var files []*os.File
	var lastLine string
	isFirst := true
	for _, file := range tempFiles {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		files = append(files, f)
		scanner := bufio.NewScanner(f)
		if scanner.Scan() {
			heap.Push(&h, &MergeItem{
				scanner:  scanner,
				filename: file,
				line:     scanner.Text(),
			})
		}
	}
	for h.Len() > 0 {
		minItem := heap.Pop(&h).(*MergeItem)
		if options.Unique {
			if isFirst {
				isFirst = false
				continue
			} else if minItem.filename != lastLine {
				fmt.Println(minItem.line)
			}
			lastLine = minItem.line
		} else {
			fmt.Println(minItem.line)

		}
		if minItem.scanner.Scan() {
			heap.Push(&h, &MergeItem{
				line:     minItem.scanner.Text(),
				scanner:  minItem.scanner,
				filename: minItem.filename,
			})
		}
	}
	for _, file := range files {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

type Options struct {
	SortColumn    int
	Numeric       bool
	Reverse       bool
	Unique        bool
	Month         bool
	IgnoreBlanks  bool
	CheckSorted   bool
	HumanReadable bool
}

func SortLines(lines []string, options Options) {
	sort.Slice(lines, func(i, j int) bool {
		a, b := lines[i], lines[j]
		if options.IgnoreBlanks {
			a = strings.TrimRight(a, " \t")
			b = strings.TrimRight(b, " \t")
		}
		if options.SortColumn > 0 {
			fieldsA := strings.Split(a, "\t")
			fieldsB := strings.Split(b, "\t")
			columnIndex := options.SortColumn - 1 // приводим к нумерации столбцов с нуля
			if columnIndex < len(fieldsA) {
				a = fieldsA[columnIndex]
			} else {
				a = ""
			}
			if columnIndex < len(fieldsB) {
				b = fieldsB[columnIndex]
			} else {
				b = ""
			}
		}
		less := false
		switch {
		case options.Numeric:
			numberFromA := extractFirstNumber(a)
			numberFromB := extractFirstNumber(b)
			if numberFromA < numberFromB {
				less = true
			}
		case options.Month:
			monthFromA := extractMonth(a)
			monthFromB := extractMonth(b)
			if monthFromA < monthFromB {
				less = true
			}
		default:
			less = a < b
		}

		if options.Reverse {
			less = !less
		}

		return less
	})
}

func ExternalSortLines(filename string, options Options) error {
	defer registeredTempFiles.Cleanup()
	tempFiles, err := sortChunksParallel(filename, options, runtime.NumCPU())
	if err != nil {
		return err
	}
	err = mergeSortedFiles(tempFiles, options)
	if err != nil {
		return err
	}

	return nil
}

func sortChunksParallel(filename string, options Options, maxGoroutines int) ([]string, error) {
	maxChunkSize := getSafeAvailableRAM() / (maxGoroutines * 2)
	chunkChan, err := readChunks(filename, maxChunkSize)
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

	for chunk := range chunkChan {
		wg.Add(1)
		sortChan <- struct{}{}
		go func(data []string) {
			defer func() {
				<-sortChan
			}()
			defer wg.Done()
			SortLines(data, options)
			tempFile, err := os.CreateTemp("", "sort_chunk_*.tmp")
			registeredTempFiles.Register(tempFile.Name())
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
		}(chunk)
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

func readChunks(filename string, maxChunkSize int) (<-chan []string, error) {
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

func extractFirstNumber(a string) float64 {
	match := numberRegex.FindString(a)
	number, err := strconv.ParseFloat(match, 64)
	if err != nil {
		return 0
	}
	return number
}

func extractMonth(a string) int {
	months := map[string]int{
		"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
		"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
	}
	runes := []rune(a)
	if len(runes) < 3 {
		return 0
	}

	extracted := runes[:3]
	if num, ok := months[string(extracted)]; ok {
		return num
	} else {
		return 0
	}
}

func checkFlagsConficts(options Options) error {
	if options.Numeric && options.Month {
		return errors.New("options -Mn are incompatible")
	}
	if options.Numeric && options.HumanReadable {
		return errors.New("options -hn are incompatible")
	}
	if options.Month && options.HumanReadable {
		return errors.New("options -hM are incompatible")
	}
	return nil
}

func checkExternalSortNeeded(filename string) (bool, error) {
	/*Функция для проверки необходимости выполнения внешней сортировки.
	Если сортируемый файл занимает более половины свободной RAM, то возвращает true.
	*/
	ramAvailable := getSafeAvailableRAM()

	fileInfo, err := os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, errors.New("file does not exist")
		} else {
			return false, errors.New("failed to stat file")
		}
	}
	fileSize := int(fileInfo.Size())

	if ramAvailable/fileSize >= 2 {
		fmt.Println("file size small: ", fileSize/1024/1024/1024)
		fmt.Println("will be sorted in-memory")
		return false, nil
	} else {
		fmt.Println("file size is too big: ", fileSize/1024/1024/1024)
		fmt.Println("will be sorted externally")
		return true, nil
	}
}

func getSafeAvailableRAM() int {
	/*Возвращает количество свободной оперативной памяти с запасом в 50%*/
	virtualMemory, err := mem.VirtualMemory()
	if err != nil {
		log.Fatal("error while getting available ram: ", err)
	}
	ramAvailable := int(virtualMemory.Available * 7 / 10)
	return ramAvailable
}

func setGlobalOptions(options Options) {
	globalOptions = options
}

func main() {
	var options Options
	pflag.IntVarP(&options.SortColumn, "sort-key", "k", 0, "sort by column N")
	pflag.BoolVarP(&options.Numeric, "numeric", "n", false, "sort by numeric")
	pflag.BoolVarP(&options.Reverse, "reverse", "r", false, "sort in reverse order")
	pflag.BoolVarP(&options.Unique, "unique", "u", false, "sort by unique order")
	pflag.BoolVarP(&options.Month, "month", "M", false, "sort by month order")
	pflag.BoolVarP(&options.IgnoreBlanks, "ignore-blanks", "b", false, "ignore trailing blanks")
	pflag.BoolVarP(&options.CheckSorted, "check-sorted", "s", false, "check if input is sorted")
	pflag.BoolVarP(&options.HumanReadable, "human-readable", "h", false, "consider suffix")
	pflag.Parse()

	args := pflag.Args()
	if len(args) == 0 {
		log.Fatal("No input file specified")
	}

	registeredTempFiles = &RegisteredTempFiles{}
	signals := make(chan os.Signal, 1)
	done := make(chan struct{})
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	go func() {
		<-signals
		registeredTempFiles.Cleanup()
		close(done)
		return
	}()

	inputFile := args[0]
	err := checkFlagsConficts(options)
	if err != nil {
		log.Fatal(err)
	}

	externalSort, err := checkExternalSortNeeded(inputFile)
	if err != nil {
		log.Fatal(err)
	}

	if !externalSort {
		content, err := os.ReadFile(inputFile)
		if err != nil {
			log.Fatal("Error while reading input file: ", err)
		}
		lines := strings.Split(string(content), "\n")
		SortLines(lines, options)
		for _, line := range lines {
			fmt.Println(line)
		}
	} else {
		err := ExternalSortLines(inputFile, options)
		if err != nil {
			log.Fatal(err)
		}
	}
	<-done
}
