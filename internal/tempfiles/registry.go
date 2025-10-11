package tempfiles

import (
	"os"
	"sync"
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
