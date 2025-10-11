package sorter

import (
	"StringSort/internal/sorter/util"
	"bufio"
	"container/heap"
	"fmt"
	"os"
)

type MergeItem struct {
	line     string
	scanner  *bufio.Scanner
	filename string
}

type MinHeap struct {
	items   []*MergeItem
	options util.Options
}

func (h *MinHeap) Len() int { return len(h.items) }

func (h *MinHeap) Less(i, j int) bool {
	a, b := h.items[i].line, h.items[j].line
	return util.Compare(a, b, h.options)
}

func (h *MinHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}
func (h *MinHeap) Push(x interface{}) {
	h.items = append(h.items, x.(*MergeItem))
}
func (h *MinHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}

func mergeSortedFiles(tempFiles []string, options util.Options) error {
	var h = MinHeap{
		options: options,
	}
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
