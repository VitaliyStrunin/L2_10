package util

import "strings"

func Compare(a, b string, options Options) bool {
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
