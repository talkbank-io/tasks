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
	"strconv"
	"sort"

)

const TIME_FORMAT = "2006-01-02"

var ScheduleRow []struct {
	Id       int64
	Template string
	Type     string
}

type PgDB struct {
	db        *pg.DB
	Schedules []model.ScheduleTask
}

// Create new Struct of PgDB
// and connect to database
func NewPgDB(config map[string]string) *PgDB {
	pgmodel := &PgDB{}
	pgmodel.db = pg.Connect(&pg.Options{
		Network: "tcp",
		Addr:     config["host"] + ":" + config["port"],
		User:     config["user"],
		Password: config["password"],
		Database: config["db"],
	})

	pgmodel.logSqlEvent()
	return pgmodel
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

/*
SELECT schedule_task.*, delivery.title AS delivery__title, delivery.text AS delivery__text,
  delivery.user_ids AS delivery__user_ids, delivery.id AS delivery__id, delivery.filter AS delivery__filter
FROM talkbank_bots.schedule_task AS "schedule_task"
  INNER JOIN talkbank_bots.delivery AS delivery ON delivery.id = schedule_task.action_id
WHERE (schedule_task.is_active = TRUE) AND (schedule_task.from_datetime >= '2018-04-25 08:31:00+00:00:00')
      AND ((schedule_task.to_datetime IS NULL) OR (schedule_task.to_datetime <= schedule_task.from_datetime))
ORDER BY "schedule_task"."id" ASC
*/

// Select active Record massAction from DB
// And return []map[string] of the ResultSets
func (pgmodel *PgDB) SelectCurrentScheduler() ([]model.ScheduleTask, error) {

	timeNow, _ := time.Parse("2006-01-02 15:04:00", time.Now().UTC().Format("2006-01-02 15:04:00"))
	scheduleRepository := model.NewScheduleRepository()
	scheduleModel := scheduleRepository.GetTaskModel()

	err := pgmodel.db.Model(&scheduleModel).
		ColumnExpr("schedule_task.*").
		ColumnExpr("delivery.title AS delivery__title").
		ColumnExpr("delivery.text AS delivery__text").
		ColumnExpr("delivery.user_ids AS delivery__user_ids").
		ColumnExpr("delivery.id AS delivery__id").
		ColumnExpr("delivery.filter AS delivery__filter").
		Join("INNER JOIN talkbank_bots.delivery AS delivery ON delivery.id = schedule_task.action_id").
		Where("schedule_task.is_active = ?", true).
		Where("schedule_task.from_datetime >= ?", timeNow).
		WhereGroup(func(q *orm.Query) (*orm.Query, error) {
			return q.
				WhereOr("schedule_task.to_datetime IS NULL").
				WhereOr("schedule_task.to_datetime >= schedule_task.from_datetime"), nil
		}).
		Order("schedule_task.id ASC").
		Select()

	if err != nil {
		fmt.Println("Error to get data from scheduler_task", err)
		return nil, err
	}

	return scheduleModel, nil

}

func (pgmodel *PgDB) GetSchedulerById(modelId int) (*model.ScheduleTask, error) {
	scheduleModel := &model.ScheduleTask{Id: modelId}
	err := pgmodel.db.Model(scheduleModel).
		ColumnExpr("schedule_task.*").
		ColumnExpr("delivery.title AS delivery__title").
		ColumnExpr("delivery.text AS delivery__text").
		ColumnExpr("delivery.user_ids AS delivery__user_ids").
		ColumnExpr("delivery.id AS delivery__id").
		ColumnExpr("delivery.filter AS delivery__filter").
		Join("INNER JOIN talkbank_bots.delivery AS delivery ON delivery.id = schedule_task.action_id").
		Where("schedule_task.id = ?", modelId).
		Select()

	if err != nil {
		fmt.Println("Error to get data from scheduler_task", err)
		return nil, err
	}

	return scheduleModel, nil
}

// Get users by params
func (pgmodel *PgDB) GetActiveUsers(userIds string, filter []model.Filter) ([]*model.Users, error) {
	userRepository := model.NewUserRepository()
	userModel := userRepository.GetUserModel()

	query := pgmodel.db.Model(&userModel).
		ColumnExpr("distinct(users.id)").
		Column("users.*").
		Join("INNER JOIN talkbank_bots.messenger_users AS messenger_users ON messenger_users.user_id = users.id").
		Where("messenger_users.is_active = ?", true)

	if( userIds != "" ){
		users, ok := parseStringUserIds(userIds)
		var usersIn = []int{}
		if( ok == true ){
			for _, usersId := range users {
				if( len(usersId) > 1 ){
					sort.Ints(usersId)
					query = query.Where("users.id BETWEEN ? AND ?", usersId[0], usersId[1])
				}else{
					usersIn = append(usersIn, usersId[0])
				}
			}

			if( len(usersIn) > 0 ){
				query = query.Where("users.id IN (?)", pg.In(usersIn))
			}
		}
	}

	var i int
	_, i = pgmodel.GetFilterQuery(query.New(), filter)
	if( i > 0 ){
		query = query.WhereGroup(func(q *orm.Query) (*orm.Query, error){
			q, _ = pgmodel.GetFilterQuery(q, filter)

			return q, nil
		})
	}

	err := query.Order("users.id ASC").Select()

	if err != nil {
		fmt.Println("Error to get data from users", err)
		return nil, err
	}

	return userModel, nil

}

func parseStringUserIds(users string) ([][]int, bool) {

	var userIds = [][]int{}
	userStringIds := strings.Split(strings.Trim(users, " "), ",")

	if( len(userStringIds) > 0 && userStringIds[0] != "" ){
		for _, userId := range userStringIds {
			trimStringUserId := strings.Trim(userId, " ")
			valueRegex := regexp.MustCompile("^([\\d]+).*[^\\d]([\\d]+)")
			match := valueRegex.FindStringSubmatch(trimStringUserId)
			between := make([]int, 2)
			if( len(match) > 2 ){
				between[0], _ = strconv.Atoi(match[1])
				between[1], _ = strconv.Atoi(match[2])

				userIds = append(userIds, between)
			}else{
				userId, _ := strconv.Atoi(trimStringUserId)
				userIds = append(userIds, []int{userId})
			}
		}



		return userIds, true
	}

	return nil, false
}

func (pgmodel *PgDB) GetFilterQuery(query *orm.Query, filters []model.Filter) (*orm.Query, int) {
	i := 0
	for _, filter := range filters {
		if ( filter.Path != "" ) {
			pathRegex := regexp.MustCompile("[^.*\\w]")
			path := pathRegex.ReplaceAllString(filter.Path, "")

			valueRegex := regexp.MustCompile("([+-]?)(\\d+) (months|days)")
			value := strings.ToLower(filter.Value)
			match := valueRegex.FindStringSubmatch(value)

			if ( len(match) > 0 ) {
				if ( match[1] == "" ) {
					match[1] = "+"
				}
				timeStr, _ := strconv.Atoi(match[1] + match[2])
				timeNow := time.Now()

				timeModified := timeNow.AddDate(0, 0, timeStr)
				if (match[3] == "months") {
					timeModified = timeNow.AddDate(0, timeStr, 0)
				}

				value = timeModified.Format(TIME_FORMAT)
			}

			op := filter.Op
			boolean := filter.Boolean

			// Получаем структуру поля для параметров
			// в JSON строке
			pathField := pgmodel.collectPathReference(path)

			var whereString []string

			switch op {
			case "exist":
				whereString = append(whereString, pathField + " IS NOT NULL", boolean, value)
			case "not":
				whereString = append(whereString, pathField + " IS NULL", boolean, value)
			case "=":
				if ( value != "" ) {
					whereString = append(whereString, pathField + " = ?", boolean, value)
				}

			case "!=":
				if ( value != "" ) {
					whereString = append(whereString, pathField + " != ?", boolean, value)
				}
			case ">":
				if ( value != "" ) {
					whereString = append(whereString, pathField + " > ?", boolean, value)
				}
			case "<":
				if ( value != "" ) {
					whereString = append(whereString, pathField + " < ?", boolean, value)
				}
			case "like":
				if ( value != "" ) {
					value = "%" + value + "%"
					whereString = append(whereString, "LOWER(" + pathField + ") ILIKE ?", boolean, value)
				}
			}
			//fmt.Println("Length: ", whereString)

			if ( len(whereString) > 0 ) {
				if ( whereString[1] == "or" ) {
					query = query.WhereOr(whereString[0], value)
				} else {
					query = query.Where(whereString[0], value)
				}

				i++
			}
		}

	}

	return query, i
}

/**
 * Create string with params field
 *
 * @param string
 * @return string
 */
func (pgmodel *PgDB) collectPathReference(path string) string {
	paths := strings.Split(path, ".")
	lastFieldName := "users.parameters"
	if ( len(paths) > 1 ) {
		last := paths[len(paths) - 1] // Only get last element
		paths = paths[:len(paths) - 1] // Remove last element

		for _, pathItem := range paths {
			lastFieldName += " -> '" + pathItem + "'"
		}

		lastFieldName += " ->> '" + last + "'"
	} else {
		lastFieldName += " ->> '" + paths[0] + "'"
	}

	return lastFieldName
}

func FormatTime(t time.Time) string {
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
}