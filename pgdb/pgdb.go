package pgdb

import (
	"fmt"
	"log"
	"time"
	"github.com/fatih/structs"
	"github.com/go-pg/pg"
	"github.com/killer-djon/tasks/model"
)

// Connect to database with options
func Connect(config map[string]string) *pg.DB {
	db := pg.Connect(&pg.Options{
		Network: "tcp",
		Addr:     config["host"]+":"+config["port"],
		User:     config["user"],
		Password: config["password"],
		Database: config["db"],
	})

	db.OnQueryProcessed(func(event *pg.QueryProcessedEvent) {
		query, err := event.FormattedQuery()
		if err != nil {
			panic(err)
		}

		log.Printf("%s %s", time.Since(event.StartTime), query)
	})

	return db
}

/*
public static function getCurrent()
    {
        $now = Carbon::now('UTC')->second(0);

        $query = ScheduleTask::select('schedule_task.*' , 'delivery.title')
            ->join('delivery', 'schedule_task.action_id', '=', 'delivery.id')
            ->where([
                ['schedule_task.is_active', true],
                ['schedule_task.from_datetime', '<=', $now]
            ])
            ->where(function ($query) use ($now) {
                return $query->whereNull('schedule_task.to_datetime')
                    ->orWhere('schedule_task.to_datetime', '>=', $now);
            });

        return $query->get();
    }
*/

// Select active Record massAction from DB
//, message map[string]interface {}
func SelectActiveScheduler(db *pg.DB, message map[string]interface {}) {
	schedule := new(model.SchedulerTask)

	var result []struct {
		SchedulerId int
		ActionId int
		DeliveryTitle string
		Type string
		Template string
	}

	err := db.Model(schedule).
		ColumnExpr("scheduler_task.id AS scheduler_id").
		ColumnExpr("scheduler_task.action_id AS action_id").
		ColumnExpr("scheduler_task.type AS type").
		ColumnExpr("scheduler_task.template AS template").
		ColumnExpr("delivery.title AS delivery_title").
		Join("JOIN talkbank_bots.delivery AS delivery ON delivery.id = scheduler_task.action_id").
		Where("scheduler_task.action_id = ?", message["action_id"]).
		Order("scheduler_task.created_at DESC").
		Select(&result)

	if err != nil {
		panic(err)
	}

	for _, val := range result {
		value := structs.Map(val)
		fmt.Println(value)
	}

}
