package util

import (
	"log"

	"github.com/shirou/gopsutil/v3/mem"
)

func GetSafeAvailableRAM() int {
	/*Возвращает количество свободной оперативной памяти с запасом в 50%*/
	virtualMemory, err := mem.VirtualMemory()
	if err != nil {
		log.Fatal("error while getting available ram: ", err)
	}
	ramAvailable := int(virtualMemory.Available * 7 / 10)
	return ramAvailable
}
