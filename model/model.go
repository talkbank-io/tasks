package model

import (
	"fmt"
	"time"

	//"github.com/go-pg/pg"
	//"github.com/go-pg/pg/orm"
)

// DB Model for table schedule_task
type SchedulerTask struct {
	tableName struct{} `sql:"talkbank_bots.schedule_task"`
	Id int64 `sql:"id"`
	ActionId int64
	Type  string `sql:"type:talkbank_bots.SCHEDULE_TASK_TYPE"`
	Category string
	Template  string
	FromDatetime time.Time
	ToDatetime time.Time
	IsActive  bool
	CreatedAt time.Time
	UpdatedAt time.Time
	NextDatetime time.Time
	LastRun time.Time
	NextRun time.Time
	StartTz string
}

// DB Model for table delivery
type Delivery struct {
	tableName struct{} `sql:"talkbank_bots.delivery"`
	Id int64 `sql:"id"`
	Text string
	Title string
	Description string
	TagId int
	CountUsers int `sql:"countUsers"`
	Sent int
	LastSending time.Time `sql:"lastSending"`
	CategoryId int64
	TimeCondition string `sql:"timeCondition"`
	ParametersCondition string `sql:"parametersCondition"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt time.Time
	UserIds string
	Filter []string `pg:",array"`
}

func (st SchedulerTask) String() string {
	return fmt.Sprintf("", st.Id, st.ActionId, st.IsActive)
}

func (del Delivery) String() string {
	return fmt.Sprintf("", del.Text, del.UserIds)
}