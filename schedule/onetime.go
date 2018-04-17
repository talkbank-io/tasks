package schedule

import (
	"fmt"
	"github.com/killer-djon/tasks/model"
	//"github.com/elgs/cron"
)

type Onetime struct {
	row model.ScheduleTask
	users []*model.Users
}

// Constructor
func NewOnetime(scheduleModel model.ScheduleTask, users []*model.Users) *Onetime {
	return &Onetime{
		row: scheduleModel,
		users: users,
	}
}

func (schedule *Onetime) Run() {
	for _, user := range schedule.users {
		fmt.Println(schedule.row.Id, user.Id)
	}

}