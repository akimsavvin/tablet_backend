package dto

type CreateTabletDTO struct {
	UserTelegramID int64  `json:"user_telegram_id"`
	Name           string `json:"name"`
	UseHour        int    `json:"use_hour"`
	UseMinute      int    `json:"use_minute"`
}
