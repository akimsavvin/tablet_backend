package models

type Tablet struct {
	Id             string `json:"id" bson:"_id"`
	UserTelegramID int64  `json:"user_telegram_id"`
	Name           string `json:"name" bson:"name"`
	UseHour        int    `json:"use_hour" bson:"use_hour"`
	UseMinute      int    `json:"use_minute" bson:"use_minute"`
}

func NewTablet(id string, userTelegramID int64, name string, useHour, useMinute int) *Tablet {
	return &Tablet{id, userTelegramID, name, useHour, useMinute}
}
