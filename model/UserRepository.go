package model

import (
	"time"
)

type Parameters interface {

}

type Users struct{
	tableName struct{} `pg:",discard_unknown_columns"sql:"talkbank_bots.users"`
	Id int `sql:",pk"`
	Hash string `sql:"type:varchar(16),unique:users_hash_key"`
	FirstName string
	LastName string
	Phone  string `sql:"type:varchar(64)"`
	Parameters Parameters
	IsIdentified bool
	IsActivated bool
        SystemParameters string
	CreatedAt time.Time `sql:"default:now()"`
	UpdatedAt time.Time
	//UserMessenger []*UserMessenger
}

type UserMessenger struct{
	tableName struct{} `pg:",discard_unknown_columns"sql:"talkbank_bots.messenger_users"`
	Id int `sql:",pk"`
	Messenger string `sql:"type:varchar(32)"`
	IsMain bool
	ChatId string
	Username string
	FirstName string
	LastName string
	Session []string `pg:",array"`
	IsActive bool
	CreatedAt time.Time `sql:"default:now()"`
	UpdatedAt time.Time
	User *Users
	UserId int

}

type UserRepository struct {
	users []*Users
	userMessengers []*UserMessenger
}

func NewUserRepository() *UserRepository {
	return &UserRepository{}
}

func (userRepository *UserRepository) GetUserModel() []*Users {
	return userRepository.users
}

func (userRepository *UserRepository) GetUserMessengerModel() []*UserMessenger {
	return userRepository.userMessengers
}

