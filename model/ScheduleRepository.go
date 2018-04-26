package model

import (
	"time"
)

type Filter struct {
	Boolean string
	Path    string
	Op      string
	Value   string
}

// DB Model for table schedule_task
type ScheduleTask struct {
	tableName    struct{} `sql:"talkbank_bots.schedule_task"`
	Id           int `sql:"id"`
	ActionId     int
	Type         string `sql:"type:talkbank_bots.SCHEDULE_TASK_TYPE"`
	Category     string
	Template     string
	FromDatetime time.Time
	ToDatetime   time.Time
	IsActive     bool
	CreatedAt    time.Time `sql:"default:now()"`
	UpdatedAt    time.Time
	NextDatetime time.Time
	LastRun      time.Time
	NextRun      time.Time
	StartTz      string
	Delivery     *Delivery
}

// DB Model for table delivery
type Delivery struct {
	tableName           struct{} `sql:"talkbank_bots.delivery"`
	Id                  int `sql:"id"`
	Text                string
	Title               string
	Description         string
	TagId               int
	CountUsers          int `sql:"countUsers"`
	Sent                int
	LastSending         time.Time `sql:"lastSending"`
	CategoryId          int
	TimeCondition       string `sql:"timeCondition"`
	ParametersCondition string `sql:"parametersCondition"`
	CreatedAt           time.Time `sql:"default:now()"`
	UpdatedAt           time.Time
	DeletedAt           time.Time
	UserIds             string
	Filter              []Filter
	ActionHash          string
}

type ScheduleRepository struct {
	task    []ScheduleTask
	deliver []Delivery
}

func NewScheduleRepository() *ScheduleRepository {
	return &ScheduleRepository{}
}

func (scheduleModel *ScheduleRepository) GetTaskModel() []ScheduleTask {
	return scheduleModel.task
}
