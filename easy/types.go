package main

import "time"

type Config struct {
	BatchSize       uint64        // размер батча
	MaxRetries      int           // максимальное количество retry-ев
	RetriesDelay    time.Duration // время ожидания между retry-ями
	CallTimeout     time.Duration // максимальное время выполнения запроса к бд
	FullCopyTimeout time.Duration // максимальное время выполнения функции CopyTable
}
