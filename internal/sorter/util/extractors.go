package util

import (
	"regexp"
	"strconv"
)

var numberRegex = regexp.MustCompile(`^[-+]?(?:\d+\.?\d*|\.\d+)`)

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
