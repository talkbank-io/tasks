package schedule

import (
	//"reflect"
	"fmt"
	"github.com/elgs/cron"
	"encoding/json"
	"github.com/killer-djon/tasks/model"
)

type Template struct {
	Minute string
	Hour string
	Day string
	Month string
	Weekday string
}

type Recurrently struct {
	template *Template
	row model.ScheduleTask
}

// Constructor
func NewRecurrently(scheduleRow model.ScheduleTask) *Recurrently {

	var result Template
	json.Unmarshal([]byte(scheduleRow.Template), &result)

	return &Recurrently{
		row: scheduleRow,
		template: &result,
	}
}

// Запускам задачи по крону от каждой полученной
// записи из schedule_task
func (schedule *Recurrently) Print() {
	//minute, _ := strconv.Atoi(schedule.template.Minute)

	cronJob := cron.New()
	_, err := cronJob.AddFunc("* * * * *", func() {
		Action(schedule)
	} )

	/*fmt.Sprintf("%s %s %s %s %s",
		schedule.template.Minute,
		schedule.template.Hour,
		schedule.template.Day,
		schedule.template.Month,
		schedule.template.Weekday,
	)*/

	if ( err != nil ) {
		fmt.Println("Error to run crontjob recurrently: ", err)
	}

	cronJob.Start()
}

func Action(schedule *Recurrently) {
	//fmt.Printf("Boolean: %s, Type: %s", schedule.row.Delivery.Filter[0].Boolean, schedule.row.Delivery.Filter[0].Path)
	//var stringArray map[string]string
	//fmt.Println( reflect.TypeOf(schedule.row.Delivery.Filter) )

	/*var inInterface []map[string]interface{}
	inrec, _ := json.Marshal(schedule.row.Delivery.Filter)

	json.Unmarshal(inrec, &inInterface)

	//fmt.Println(inInterface)

	for _, item := range inInterface {
		fmt.Println(item.boolean)
	}*/
	for _, item := range schedule.row.Delivery.Filter {
		fmt.Println(item.Path, item.Op)
	}

}