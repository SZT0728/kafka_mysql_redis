package Message

import "mysql_redis_kafka/models"

//消息包
type MessagePackage struct {
	Type string
	DataJson []byte
}

type MysqlSaveToRedis struct {
	Persons []models.Person
}



