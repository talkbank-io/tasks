package schedule

import (
	"fmt"
	//"github.com/elgs/cron"
)

type Onetime struct {

}

// Constructor
func NewOnetime() *Onetime {
	return &Onetime{}
}

func (schedule *Onetime) Print() {
	fmt.Println("Start onetime")
}