package schedule

import (
	"fmt"
	//"strconv"
	"encoding/json"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/publisher"
	"github.com/elgs/cron"
)

type Onetime struct {
	row   model.ScheduleTask
	users []*model.Users
	pub   *publisher.Publisher
}

type QueueMessage struct {
	UserId int64
	TaskId int64
}

// Constructor
func NewOnetime(scheduleModel model.ScheduleTask, users []*model.Users) *Onetime {
	return &Onetime{
		row: scheduleModel,
		users: users,
	}
}

func (schedule *Onetime) Run(cronJob *cron.Cron, jobNumber int) {
	countPublishing := 0
	for _, user := range schedule.users {
		fmt.Println(schedule.row.Id, user.Id, schedule.row.LastRun, schedule.row.FromDatetime)

		var q_message []QueueMessage
		q_message = []QueueMessage{
			QueueMessage{
				UserId: user.Id,
				TaskId: schedule.row.Id,
			},
		}

		fmt.Println("Will be publis data:", q_message)

		message, err := json.Marshal(q_message)
		if err != nil {
			fmt.Println("error:", err)
		}



		channel := schedule.pub
		isPublish, err := channel.Publish(message)

		if err != nil {
			fmt.Println("error on publishing:", err)
		}

		if( isPublish == true ){
			countPublishing++
		}

		fmt.Println("Message will be publish:", isPublish)

	}

	if ( countPublishing == len(schedule.users) ){
		cronJob.RemoveFunc(jobNumber)
		fmt.Println("Cron job will be removed:", jobNumber)
	}
	//fmt.Printf("Len countpublishing: %d, count Users: %d, Of job number: %d", countPublishing, len(schedule.users), jobNumber)
}

func (schedule *Onetime) SetAmqp(pub *publisher.Publisher) {
	schedule.pub = pub
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