package database

// Utilities

// function to reverse the given integer array
// func reverse(numbers []int) []int {

// 	var length = len(numbers) // getting length of an array

// 	for i := 0; i < length/2; i++ {
// 		temp := numbers[i]
// 		numbers[i] = numbers[length-i-1]
// 		numbers[length-i-1] = temp
// 	}

// 	return numbers
// }

// func removeColumn(slice []Column, s int) []Column {
// 	return append(slice[:s], slice[s+1:]...)
// }

func trimCols(cols []Column, pkey []PKey) []Column {
	var clist []int
	var ilist []int
	for k, c := range cols {
		clist = append(clist, k)
		for _, p := range pkey {
			if c.ColumnName == p.PKey {
				ilist = append(ilist, k)
			}
		}
	}

	var collist []int
	for _, c := range clist {
		if func(e int, ee []int) bool {
			for _, i := range ee {
				if e == i {
					return false
				}
			}
			return true
		}(c, ilist) {
			collist = append(collist, c)
		}
	}

	var columns []Column
	for _, c := range collist {
		columns = append(columns, cols[c])
	}
	return columns
}
