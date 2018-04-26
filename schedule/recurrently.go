package schedule

import (
	//"reflect"
	"fmt"
	"encoding/json"
	"github.com/killer-djon/tasks/model"
	"github.com/killer-djon/tasks/publisher"
	"github.com/killer-djon/tasks/pgdb"
	"github.com/killer-djon/cron"
)

type Template struct {
	Minute  string
	Hour    string
	Day     string
	Month   string
	Weekday string
}

type Recurrently struct {
	template *Template
	row      model.ScheduleTask
	pub      *publisher.Publisher
	db       *pgdb.PgDB
}

// Constructor
func NewRecurrently(scheduleModel model.ScheduleTask, pub *publisher.Publisher, database *pgdb.PgDB) *Recurrently {

	var result Template
	json.Unmarshal([]byte(scheduleModel.Template), &result)

	return &Recurrently{
		template: &result,
		row: scheduleModel,
		pub: pub,
		db: database,
	}
}

// Запускам задачи по крону от каждой полученной
// записи из schedule_task
func (schedule *Recurrently) Run(publisherConfig map[string]interface{}, cronJob *cron.Cron) {
	//minute, _ := strconv.Atoi(schedule.template.Minute)


	/*fmt.Sprintf("%s %s %s %s %s",
		schedule.template.Minute,
		schedule.template.Hour,
		schedule.template.Day,
		schedule.template.Month,
		schedule.template.Weekday,
	)*/

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