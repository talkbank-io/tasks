package pgdb

import (
	"fmt"
	"log"
	"time"
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
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

	// логируем искходный запрос SQL
	logSqlEvent(db)

	return db
}

func logSqlEvent(db *pg.DB) {
	db.OnQueryProcessed(func(event *pg.QueryProcessedEvent) {
		query, err := event.FormattedQuery()
		if err != nil {
			panic(err)
		}

		log.Printf("%s %s", time.Since(event.StartTime), query)
	})
}



// Select active Record massAction from DB
// And return []map[string] of the ResultSets
func SelectCurrentScheduler(db *pg.DB) ([]model.ScheduleTask, error) {
	var schedules []model.ScheduleTask
	timeNow := time.Now().UTC()

	err := db.Model(&schedules).
		ColumnExpr("schedule_task.*").
		ColumnExpr("delivery.title AS delivery__title").
		ColumnExpr("delivery.user_ids AS delivery__user_ids").
		Join("JOIN talkbank_bots.delivery AS delivery ON delivery.id = schedule_task.action_id").
		Where("schedule_task.is_active = ?", true).
		Where("schedule_task.from_datetime <= ?", timeNow).
		WhereGroup(func(q *orm.Query) (*orm.Query, error) {
		return q.
		WhereOr("schedule_task.to_datetime IS NULL").
			WhereOr("schedule_task.to_datetime >= ?", timeNow), nil
	}).
		Order("schedule_task.created_at DESC").
		Select()

	if err != nil {
		fmt.Println("Error to get data from scheduler_task")
		return nil, err
	}

	return schedules, nil
}


/*
public function getActiveUsers()
{
	$query = $this->entity->query();
        $query->selectRaw('distinct on (users.id) users.*');
        $query->join(MessengerUser::getFullTableName() . ' as m', 'users.id', 'm.user_id');
        $query->where('m.is_active', true);

        if (is_array($ids) && count($ids) > 0) {
            $query->whereIn('users.id', $ids);
        }

        if ($filter) {
            $query = $this->getQbByFilter($filter, $query);
        }

        $query->orderBy('users.id');

        return $query->get();
}

*/
