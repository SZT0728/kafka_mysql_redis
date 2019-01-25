package models

type Person struct {
	Id int `db:id`
	Name string `db:"name"`
	Age int `db:"age"`
	Rmb int `db:"rmb"`
}
