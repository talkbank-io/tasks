package pgdb

import (
	"fmt"
	"log"
	"time"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/killer-djon/tasks/model"
	"strings"
	"regexp"
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
	pgmodel := &PgDB{}
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
	scheduleRepository := model.NewScheduleRepository()
	scheduleModel := scheduleRepository.GetTaskModel()

	err := pgmodel.db.Model(&scheduleModel).
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
		//Group("delivery__title", "delivery.user_ids", "delivery.id", "schedule_task.id").
		Order("schedule_task.id ASC").
		Select()

	if err != nil {
		fmt.Println("Error to get data from scheduler_task", err)
		return nil, err
	}

	return scheduleModel, nil

}

// Get users by params
func (pgmodel *PgDB) GetActiveUsers(userIds []int, filter []model.Filter) ([]*model.Users, error) {
	userRepository := model.NewUserRepository()
	userModel := userRepository.GetUserModel()

	query := pgmodel.db.Model(&userModel).
		ColumnExpr("distinct(users.id)").
		Column("users.*").
		Join("INNER JOIN talkbank_bots.messenger_users AS messenger_users ON messenger_users.user_id = users.id").
		Where("messenger_users.is_active = ?", true)

	if ( userIds[0] != 0 ){
		query = query.Where("users.id IN (?)", pg.In(userIds))
	}

	query = pgmodel.GetFilterQuery(query, filter)

	err := query.Select()

	if err != nil {
		fmt.Println("Error to get data from users", err)
		return nil, err
	}

	return userModel, nil

}


func (pgmodel *PgDB) GetFilterQuery(query *orm.Query, filters []model.Filter) *orm.Query {
	if ( filters != nil ){
		for _, filter := range filters {
			if ( filter.Path != "" ){
				pathRegex, _ := regexp.Compile("[^.*\\w]")
				path := pathRegex.ReplaceAllString(filter.Path, "")
				value := strings.ToLower(filter.Value)
				op := filter.Op
				boolean := filter.Boolean

				fmt.Printf("Path: %s, Value: %s, Op: %s, Boolean: %s\n\n", path, value, op, boolean)
			}

		}
	}

	return query
}

/*
if (!$qb) {
            $qb = $this->entity->select(['id', 'parameters',]);
        }


        foreach ($filters as $filter) {

            $path = preg_replace('/[^.*\w]/', '', $filter['path']);
            $value = mb_strtolower($filter['value']);
            $op = $filter['op'];
            $boolean = $filter['boolean'];

            if (preg_match("/([+-]?)(\d+) (months|days)/i", $value, $m)) {
                $shift = $m[1] . $m[2] . ' ' . $m[3];
                $value = date('Y-m-d', strtotime($shift));
            }

            $wherePath = $this->collectPathReference('users.parameters', $path);
            switch ($op) {
                case 'exist':
                    $qb->whereRaw($wherePath . ' IS NOT NULL', [], $boolean);
                    break;
                case 'not':
                    $qb->whereRaw($wherePath . ' IS NULL', [], $boolean);
                    break;
                case '=':
                    $qb->whereRaw($wherePath . ' ilike ?', [$value], $boolean);
                    break;
                case '!=':
                    $qb->whereRaw($wherePath . ' not ilike ?', [$value], $boolean);

                    break;
                case '>':
                    $qb->whereRaw($wherePath . ' >= ?', [$value], $boolean);
                    break;
                case '<':
                    $qb->whereRaw($wherePath . ' <= ?', [$value], $boolean);
                    break;
                case 'like':
                    // f*cking laravel! For like whereRaw escapes...
                    $qb->whereRaw('LOWER(' . $wherePath . ') ilike ?', ['%' . $value . '%'],
                        $boolean);
                    break;
            }
        }
*/
