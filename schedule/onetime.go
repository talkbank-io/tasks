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

	lastRun := schedule.row.LastRun.UTC()
	fromDate := schedule.row.FromDatetime.UTC()

	if ( lastRun.Before(fromDate) ){
		for _, user := range schedule.users {
			fmt.Println(schedule.row.Id, user.Id, schedule.row.LastRun, schedule.row.FromDatetime)
		}
	}
}
/*
    if ($task->last_run >= $task->from_datetime) {
	\Log::debug("Task $taskId was completed.");
	return false;
    }

    // Что если запуск таска работает больше 1 минуты?
    // Временное решение, надеюсь.
    $task->last_run = Carbon::now();
    $task->save();

    $localDT = $task->from_datetime;
*/