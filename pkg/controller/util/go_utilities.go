package util

// ContainsInMap returns true if the map m contains at least on of the string
//   presented as argument `vals`, otherwise false is returned
func ContainsInMap(m map[string]string, vals ...string) bool {
	for _, x := range m {
		for _, v := range vals {
			if x == v {
				return true
			}
		}
	}
	return false
}

// ContainsInList returns true if the string s is included in the list,
//   otherwise false is returned
func ContainsInList(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// RemoveFromList iterates over the list and removes all occurences
//   of the string s. The list without the s strings is returned.
func RemoveFromList(list []string, s string) []string {
	for i, v := range list {
		if v == s {
			list = append(list[:i], list[i+1:]...)
		}
	}
	return list
}
