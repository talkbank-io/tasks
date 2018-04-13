package model

import (
	"time"
)

// DB Model for table schedule_task
type ScheduleTask struct {
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
	Delivery *Delivery
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