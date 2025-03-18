package criteria

import (
	"github.com/viant/fluxor/service/dao"
)

func FilterByState(state string, parameters []*dao.Parameter) bool {
	switch len(parameters) {
	case 0:
		return true
	case 1:
		if parameters[0].Name == "State" {
			switch actual := parameters[0].Value.(type) {
			case string:
				return state == actual
			case []string:
				for _, s := range actual {
					if state == s {
						return true
					}
				}
				return false
			}
		}
	}
	return true
}
