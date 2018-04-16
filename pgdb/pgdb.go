package pgdb

import (
	"fmt"
	"log"
	"time"
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/killer-djon/tasks/model"
)

var ScheduleRow []struct {
	Id int64
	Template string
	Type string
}

type PgDB struct {
	db        *pg.DB
	Schedules []model.ScheduleTask
}

// Create new Struct of PgDB
// and connect to database
func NewPgDB(config map[string]string) PgDB {
	pgmodel := new(PgDB)
	pgmodel.db = pg.Connect(&pg.Options{
		Network: "tcp",
		Addr:     config["host"] + ":" + config["port"],
		User:     config["user"],
		Password: config["password"],
		Database: config["db"],
	})

	pgmodel.logSqlEvent()
	return *pgmodel
}

// Initialize log event with SQL
func (pgmodel *PgDB) logSqlEvent() {
	pgmodel.db.OnQueryProcessed(func(event *pg.QueryProcessedEvent) {
		query, err := event.FormattedQuery()
		if err != nil {
			panic(err)
		}

		log.Printf("%s %s", time.Since(event.StartTime), query)
	})
}

// Select active Record massAction from DB
// And return []map[string] of the ResultSets
func (pgmodel *PgDB) SelectCurrentScheduler() ([]model.ScheduleTask, error) {
	timeNow := time.Now().UTC()

	err := pgmodel.db.Model(&pgmodel.Schedules).
		ColumnExpr("schedule_task.*").
		ColumnExpr("delivery.title AS delivery__title").
		ColumnExpr("delivery.user_ids AS delivery__user_ids").
		ColumnExpr("delivery.id AS delivery__id").
		ColumnExpr("delivery.filter AS delivery__filter").
		Join("INNER JOIN talkbank_bots.delivery AS delivery ON delivery.id = schedule_task.action_id").
		Where("schedule_task.is_active = ?", true).
		Where("schedule_task.from_datetime <= ?", timeNow).
		WhereGroup(func(q *orm.Query) (*orm.Query, error) {
		return q.
			WhereOr("schedule_task.to_datetime IS NULL").
			WhereOr("schedule_task.to_datetime >= ?", timeNow), nil
		}).
		Group("delivery__title", "delivery.user_ids", "delivery.id", "schedule_task.id").
		Order("schedule_task.id ASC").
		Select()

	if err != nil {
		fmt.Println("Error to get data from scheduler_task")
		return nil, err
	}

	return pgmodel.Schedules, nil

}

func (pgmodel *PgDB) GetActiveUsers() {

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
