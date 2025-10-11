package main

import (
	"StringSort/internal/sorter"
	"StringSort/internal/sorter/util"
	"StringSort/internal/tempfiles"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/pflag"
)

func checkFlagsConficts(options util.Options) error {
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
	ramAvailable := util.GetSafeAvailableRAM()

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

func main() {
	var options util.Options
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

	registeredTempFiles := &tempfiles.RegisteredTempFiles{}
	signals := make(chan os.Signal, 1)
	done := make(chan struct{})
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signals
		registeredTempFiles.Cleanup()
		close(done)
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
		sorter.SortLines(lines, options)
		for _, line := range lines {
			fmt.Println(line)
		}
	} else {
		err := sorter.ExternalSortLines(inputFile, options, registeredTempFiles)
		if err != nil {
			log.Fatal(err)
		}
	}
	<-done
}
