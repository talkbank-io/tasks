package pgdb

import (
	"fmt"
	"time"
	"crypto/sha256"
	"encoding/base64"
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/talkbank-io/tasks/model"
	"github.com/gorhill/cronexpr"
	"strings"
	"regexp"
	"strconv"
	"sort"

	"math/rand"
	"encoding/json"
)

const (
	TIME_FORMAT = "2006-01-02"
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

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

	return pgmodel
}


func (pgmodel *PgDB) SetHashAction(Id int, hash string) (string, error) {

	var hashString string
	_, err := pgmodel.db.Model(&model.Delivery{}).
		Set("action_hash = ?", hash).
		Where("id = ?", Id).
		Returning("action_hash").
		Update(&hashString)

	if( err != nil ) {
		fmt.Println("ERror on update item", Id, err)
		return "", err
	}

	return hashString, nil
}

func (pgmodel *PgDB) SetIsRunning(Id int, isRunning bool) {

	_, err := pgmodel.db.Model(&model.ScheduleTask{}).
		Set("is_running = ?", isRunning).
		Where("id = ?", Id).
		Update()

	if( err != nil ) {
		fmt.Println("ERror on update item", Id, err)
	}
}

func (pgmodel *PgDB) IncSentDelivery(pendingModel model.PendingTask, deliveryId, cnt int) {
	delivery, err := pgmodel.GetDeliveryById(deliveryId)

	if (err != nil) {
		fmt.Println("ERror to find delivery by ID", deliveryId, err)
	}

	delivery.Sent += cnt

	_, err = pgmodel.db.Model(delivery).
		WherePK().
		Update()

	if (err != nil) {
		fmt.Println("ERror to update delivery sent on pending task", delivery.Id, err)
	}

	_, err = pgmodel.db.Model(&pendingModel).WherePK().Delete()
	if (err != nil) {
		fmt.Println("Cant deliete pengind task", pendingModel.Id, err)
	}
}

func (pgmodel *PgDB) SaveStatistic(scheduleId, countUsersDelivery int) {
	scheduleItem, err := pgmodel.GetSchedulerById(scheduleId)

	if ( err != nil ) {
		fmt.Println("Cant find schedule by Id", scheduleId)
	}

	now := time.Now().UTC()

	is_active := true
	last_run := now
	is_running := false

	if ( scheduleItem.Type == "onetime" ) {
		is_active = false

		_, err = pgmodel.db.Model(scheduleItem).
			Set("last_run = ?", last_run).
			Set("is_active = ?", is_active).
			Set("is_running = ?", is_running).
			WherePK().
			Update()
	}else {
		var resultTemplate = make(map[string]string)
		json.Unmarshal([]byte(scheduleItem.Template), &resultTemplate)

		cronTemplate := fmt.Sprintf("%s %s %s %s %s",
			resultTemplate["minute"],
			resultTemplate["hour"],
			resultTemplate["day"],
			resultTemplate["month"],
			resultTemplate["weekday"],
		)


		// Fields to by  update on schedule item
		nextRun := cronexpr.MustParse(cronTemplate)

		referenceDateFrom := now
		if( scheduleItem.FromDatetime.After(now) ) {
			referenceDateFrom = scheduleItem.FromDatetime
		}

		next_run := nextRun.Next(referenceDateFrom).UTC()

		if ( next_run.After(scheduleItem.ToDatetime) ) {
			is_active = true
		}else{
			is_active = false
		}

		fmt.Printf("Next run time is=%v, by schedule Id=%d", next_run, scheduleItem.Delivery.Id)

		_, err = pgmodel.db.Model(scheduleItem).
			Set("last_run = ?", last_run).
			Set("is_active = ?", is_active).
			Set("is_running = ?", is_running).
			Set("next_run = ?", next_run).
			WherePK().
			Update()
	}

	if (err != nil) {
		fmt.Println("ERror on update item", scheduleId, err)
	}

	delivery, _ := pgmodel.GetDeliveryById(scheduleItem.Delivery.Id)

	delivery.CountUsers = countUsersDelivery
	delivery.LastSending = last_run
	delivery.Sent += countUsersDelivery

	_, err = pgmodel.db.Model(delivery).
		WherePK().
		Update()

	if (err != nil) {
		fmt.Println("ERror on update item", scheduleId, err)
	}
}

func (pgmodel *PgDB) GetDeliveryById(deliveryId int) (*model.Delivery, error) {
	deliveryModel := &model.Delivery{Id: deliveryId}

	err := pgmodel.db.Select(deliveryModel)

	if err != nil {
		fmt.Println("Error to get data from scheduler_task", err)
		return nil, err
	}

	return deliveryModel, nil
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
		ColumnExpr("delivery.action_hash AS delivery__action_hash").
		Join("INNER JOIN talkbank_bots.delivery AS delivery ON delivery.id = schedule_task.action_id").
		Where("schedule_task.id = ?", modelId).
		Select()

	if err != nil {
		fmt.Println("Error to get data from scheduler_task", err)
		return nil, err
	}

	return scheduleModel, nil
}


// Random unique string letters by n-bytes
func (pgmodel *PgDB) RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63() % int64(len(letterBytes))]
	}
	return string(b)
}

func (pgmodel *PgDB) SaveHash(scheduleId, deliveryId int) (string, error) {

	hash := sha256.New()
	hash.Write([]byte(time.Now().UTC().Format("2006-01-02 15:04:05")))
	hash.Write([]byte(strconv.Itoa(scheduleId)))
	hash.Write([]byte(pgmodel.RandStringBytesRmndr(16)))
	hash.Write([]byte(strconv.Itoa(deliveryId)))

	sum := hash.Sum(nil)
	stringHash := base64.URLEncoding.EncodeToString(sum)

	fmt.Println("New hash instance", stringHash, time.Now().UTC().Format("2006-01-02 15:04:05"))

	hashSet, err := pgmodel.SetHashAction(deliveryId, stringHash)

	if( err != nil ){
		fmt.Println("Error ocurred when update delivery data", err)
		return "", err
	}

	return hashSet, nil
}

// return PendingTask::where('planned', '<=', Carbon::now('UTC'))->get();
func (pgmodel *PgDB) SelectPendingTasks() ([]model.PendingTask, error) {
	now, _ := time.Parse("2006-01-02 15:04", time.Now().UTC().Format("2006-01-02 15:04"))

	scheduleRepository := model.NewScheduleRepository()
	pendingModel := scheduleRepository.GetPendingTaskModel()


	err := pgmodel.db.Model(&pendingModel).
		ColumnExpr("pending_task.*").
		ColumnExpr("schedule_task.id AS schedule_task__id").
		ColumnExpr("delivery.id AS delivery__id").
		ColumnExpr("delivery.text AS delivery__text").
		Join("INNER JOIN talkbank_bots.schedule_task AS schedule_task ON schedule_task.action_id = pending_task.action_id").
		Join("INNER JOIN talkbank_bots.delivery AS delivery ON delivery.id = schedule_task.action_id").
		Where("pending_task.planned <= ?", now).
		Select()

	if err != nil {
		fmt.Println("Error to get data from pending_task", err)
		return nil, err
	}

	return pendingModel, nil
}




/*
SELECT schedule_task.*, delivery.title AS delivery__title, delivery.text AS delivery__text, delivery.user_ids AS delivery__user_ids, delivery.id AS delivery__id, delivery.filter AS delivery__filter FROM talkbank_bots.schedule_task AS "schedule_task" INNER JOIN talkbank_bots.delivery AS delivery ON delivery.id = schedule_task.action_id WHERE (schedule_task.is_active = TRUE) AND (((schedule_task.type = 'onetime') AND (schedule_task.from_datetime >= '2018-04-26 18:29:00') AND ((schedule_task.to_datetime IS NULL) OR (schedule_task.to_datetime >= schedule_task.from_datetime))) OR ((schedule_task.type = 'recurrently') AND (schedule_task.from_datetime <= '2018-04-26 18:29:00') AND ((schedule_task.to_datetime IS NULL) OR ((schedule_task.to_datetime >= '2018-04-26 18:29:00') AND (schedule_task.to_datetime > schedule_task.from_datetime))))) ORDER BY "schedule_task"."id" ASC
*/

// Select active Record massAction from DB
// And return []map[string] of the ResultSets
func (pgmodel *PgDB) SelectCurrentScheduler() ([]model.ScheduleTask, error) {
	now, _ := time.Parse("2006-01-02 15:04:00", time.Now().UTC().Format("2006-01-02 15:04:00"))

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
		WhereGroup(func(q *orm.Query) (*orm.Query, error) {
			return q.
				WhereOrGroup(func(subQ1 *orm.Query) (*orm.Query, error){
					return subQ1.
						Where("schedule_task.type = ?", "onetime").
						Where("schedule_task.from_datetime >= ?", now).
						WhereGroup(func(subQ *orm.Query) (*orm.Query, error) {
							return subQ.
								WhereOr("schedule_task.to_datetime IS NULL").
								WhereOr("schedule_task.to_datetime >= schedule_task.from_datetime"), nil
						}), nil
				}).
				WhereOrGroup(func(subQ2 *orm.Query) (*orm.Query, error){
					return subQ2.
						Where("schedule_task.type = ?", "recurrently").
						WhereGroup(func(subGroup *orm.Query) (*orm.Query, error){
							return  subGroup.Where("schedule_task.from_datetime <= ?", now).
								WhereGroup(func(subQ *orm.Query) (*orm.Query, error) {
								return subQ.
								WhereOr("schedule_task.to_datetime IS NULL").
									WhereOrGroup(func(subQ1 *orm.Query) (*orm.Query, error){
									return subQ1.
										Where("schedule_task.to_datetime >= ?", now).
										Where("schedule_task.to_datetime > schedule_task.from_datetime"), nil
								}), nil
							}), nil
						}).
						WhereOrGroup(func(subGroup2 *orm.Query) (*orm.Query, error) {
							return subGroup2.
								Where("schedule_task.from_datetime >= ?", now).
								Where("schedule_task.from_datetime <= schedule_task.next_run").
								WhereGroup(func(toGroup *orm.Query) (*orm.Query, error) {
									return toGroup.
										WhereOr("schedule_task.to_datetime IS NULL").
										WhereOr("schedule_task.to_datetime >= schedule_task.next_run"), nil
								}), nil
						}), nil
				}), nil
		}).
		Order("schedule_task.id ASC").
		Select()

	if err != nil {
		fmt.Println("Error to get data from scheduler_task", err)
		return nil, err
	}

	return scheduleModel, nil

}

func (pgmodel *PgDB) GetUserDeliveryCountByHash(result map[string]int, scheduleId int, hash string) (int, error) {

	userDeliveryModel := &model.UserDelivery{}
	count, err := pgmodel.db.Model(userDeliveryModel).
		ColumnExpr("user_delivery.*").
		ColumnExpr("delivery.id AS delivery__id").
		ColumnExpr("delivery.delivery_hash AS delivery__hash").
		Join("INNER JOIN talkbank_bots.delivery AS delivery ON delivery.id = user_delivery.delivery_id").
		Where("user_delivery.delivery_hash = ?", hash).
		Count()

	if err != nil {
		fmt.Println("Error to get data from user_delivery", err)
		return 0, err
	}

	fmt.Println("Finded count by hash", result["lenUsers"], count, hash)
	return count, nil
}

// Get users by params
func (pgmodel *PgDB) GetUsersByFilter(userIds string) ([]*model.Users, error) {
	userRepository := model.NewUserRepository()
	userModel := userRepository.GetUserModel()

	query := pgmodel.db.Model(&userModel).
		ColumnExpr("distinct on (users.id) users.*").
		Join("INNER JOIN talkbank_bots.messenger_users AS messenger_users ON messenger_users.user_id = users.id").
		Where("messenger_users.is_active = ?", true)

	if( userIds != "" ){
		users, ok := parseStringUserIds(userIds)
		fmt.Println("Parsed users:", users)
		var usersIn = []int{}
		var usersBetween = [][]int{}
		if( ok == true ){
			for _, usersId := range users {
				if( len(usersId) > 1 ){
					// Созтавной диапазон пользователей
					sort.Ints(usersId)
					usersBetween = [][]int{{usersId[0], usersId[1]}}
				}else{
					usersIn = append(usersIn, usersId[0])
				}
			}
			if( len(usersBetween) > 0 ) {
				query = query.WhereGroup(func(q *orm.Query) (*orm.Query, error){
					for _, usersId := range usersBetween {
						q = q.WhereOr("users.id BETWEEN ? AND ?", usersId[0], usersId[1])
					}

					return q, nil
				})
			}


			if( len(usersIn) > 0 ){
				if( len(usersBetween) > 0 ) {
					query = query.WhereOr("users.id IN (?)", pg.In(usersIn))
				}else {
					query = query.Where("users.id IN (?)", pg.In(usersIn))
				}

			}
		}
	}

	err := query.Order("users.id ASC").Select()

	if err != nil {
		fmt.Println("Error to get data from users", err)
		return nil, err
	}
	return userModel, nil
}

func (pgmodel *PgDB) GetActiveUsers(userIds string, filter []model.Filter) ([]*model.Users, error) {
	userRepository := model.NewUserRepository()
	userModel := userRepository.GetUserModel()

	query := pgmodel.db.Model(&userModel).
		ColumnExpr("distinct on (users.id) users.*").
		Join("INNER JOIN talkbank_bots.messenger_users AS messenger_users ON messenger_users.user_id = users.id").
		Where("messenger_users.is_active = ?", true)

	if( userIds != "" ){
		users, ok := parseStringUserIds(userIds)
		fmt.Println("Parsed users:", users)
		var usersIn = []int{}
		var usersBetween = [][]int{}
		if( ok == true ){
			for _, usersId := range users {
				if( len(usersId) > 1 ){
					// Созтавной диапазон пользователей
					sort.Ints(usersId)
					usersBetween = [][]int{{usersId[0], usersId[1]}}
				}else{
					usersIn = append(usersIn, usersId[0])
				}
			}
			if( len(usersBetween) > 0 ) {
				query = query.WhereGroup(func(q *orm.Query) (*orm.Query, error){
					for _, usersId := range usersBetween {
						q = q.WhereOr("users.id BETWEEN ? AND ?", usersId[0], usersId[1])
					}

					return q, nil
				})
			}


			if( len(usersIn) > 0 ){
				if( len(usersBetween) > 0 ) {
					query = query.WhereOr("users.id IN (?)", pg.In(usersIn))
				}else {
					query = query.Where("users.id IN (?)", pg.In(usersIn))
				}

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

func parseStringAsArray(input string) ([]string, error) {
	array := strings.Split(strings.Trim(input, " "), ",")

	if( len(array) <= 0 ) {
		return nil, fmt.Errorf("Empty string after split= %s", input)
	}

	var output []string
	if( len(array) > 0 ) {
		for _, item := range array {
			output = append(output, strings.Trim(item, " "))
		}
	}

	return output, nil
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
			//value := strings.ToLower(filter.Value)
			value := filter.Value
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
			case "<=":
				if( value != "" ) {
					whereString = append(whereString, pathField + " <= ?", boolean, value)
				}
			case ">=":
				if( value != "" ) {
					whereString = append(whereString, pathField + " >= ?", boolean, value)
				}
			case "in":
				if( value != "" ) {
					whereString = append(whereString, pathField + " IN ("+value+")", boolean, value)

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