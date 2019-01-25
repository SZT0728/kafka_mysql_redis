package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"mysql_redis_kafka/Message"
	"mysql_redis_kafka/models"
	"mysql_redis_kafka/redisPool"
	"strconv"
	"sync"
)

var producer sarama.SyncProducer
var consumer sarama.Consumer
var topic = "CacheConsistent"

var (
	wg  sync.WaitGroup
)
func main() {

	//创建一个生产者
	config := sarama.NewConfig()
	// 等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 随机的分区类型：返回一个分区器，该分区器每次选择一个随机分区
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// 是否等待成功和失败后的响应
	config.Producer.Return.Successes = true
	var err error
	// 使用给定代理地址和配置创建一个同步生产者
	producer, err = sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	//创建一个消费者
	consumer, err = sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	go ConsumerAction(consumer)

/*

	p1 := models.Person{1,"zhangsan",3,4}
	p2 := models.Person{2,"lisi",4,5}
	persons := []models.Person{p1,p2}

	obj := Message.MysqlSaveToRedis{persons}
	data,_ := json.Marshal(obj)
	value_msg := Message.MessagePackage{"Add",data}
	value,_ := json.Marshal(value_msg)
	topic_value := sarama.StringEncoder(value)


	var cmd string
	for{
		fmt.Scan(&cmd)
		SendMessage(topic,topic_value,producer)
	}
*/


		var cmd string
		for{
			fmt.Println("输入命令")
			fmt.Scan(&cmd)
			switch cmd {
			case "getall":
				getAll()
			default:
				fmt.Println("不能识别其他命令")
			}
			fmt.Println()
		}

}





func getAll()  {
	//从连接池当中获取链接
	conn := redisPool.Pool.Get()
	//先查看redis中是否有数据
	//conn,_ :=redisPool.Dial("tcp","localhost:6379")
	defer conn.Close()
	values, _ := redis.Values(conn.Do("lrange", "mlist",0,-1))

	if len(values) > 0 {
		//如果有数据
		fmt.Println("从redis获取数据")
		//从redis中直接获取
		for _,key := range values{
			pid :=string(key.([]byte))
			id ,_:= strconv.Atoi(pid)
			results,_ := redis.Bytes(conn.Do("GET",id))
			var p models.Person
			err := json.Unmarshal(results,&p)
			if err != nil {
				fmt.Println("json 反序列化出错")
			}else {
				fmt.Printf("name = %s\n",p.Name)
			}
		}
	}else {
		fmt.Println("从mysql中获取")

		//查询数据库
		db,err := sql.Open("mysql","root:Szt930708@tcp(localhost:3306)/mydb")
		if err != nil {
			fmt.Println("数据库打开失败",err)
		}
		defer db.Close()

		var persons []models.Person

		rows,_ := db.Query("select id,name,age,rmb from person")
		for rows.Next()  {
			var id int
			var name string
			var age int
			var rmb int
			rows.Scan(&id,&name,&age,&rmb)
			per := models.Person{id,name,age,rmb}
			persons = append(persons,per)

		}
		//发送消息
		obj := Message.MysqlSaveToRedis{persons}
		data,_ := json.Marshal(obj)
		value_msg := Message.MessagePackage{"Add",data}
		value,_ := json.Marshal(value_msg)
		topic_value := sarama.StringEncoder(value)
		SendMessage(topic,topic_value,producer)
	}
}

//消费者消费消息
func ConsumerAction(consumer sarama.Consumer)  {
	//Partitions(topic):该方法返回了该topic的所有分区id
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		panic(err)
	}
	for partition := range partitionList{
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		defer pc.AsyncClose()
		wg.Add(1)
		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			//Messages()该方法返回一个消费消息类型的只读通道，由代理产生
			for msg := range pc.Messages() {
				var messagePackage Message.MessagePackage
				err := json.Unmarshal(msg.Value,&messagePackage)
				if err != nil {
					fmt.Println("消息包序列化出问题了")
				}else {
					if messagePackage.Type == "Add" {
						var valueData  Message.MysqlSaveToRedis
						json.Unmarshal(messagePackage.DataJson,&valueData)
						SaveDataIntoRedis(valueData.Persons)
						fmt.Printf("消费者消费了Add\n")
					}
				}

			}
		}(pc)
	}
	wg.Wait()
	consumer.Close()
}

// 发送消息 
func SendMessage(topic string,value sarama.StringEncoder,producer sarama.SyncProducer)  {
	msg := &sarama.ProducerMessage {
		Topic: topic,//包含了消息的主题
		Partition: int32(10),//
		Key:        sarama.StringEncoder("key"),//
	}
	msg.Value = sarama.StringEncoder(value)
	partition,offset,err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("发送消息异常",err)
	}else {
		fmt.Printf("发送消息：分区:%d，偏移量:%d,主题:%s,消息:%s\n",partition,offset,topic,value)	
	}
}

//从mysql读取后存进缓存
func SaveDataIntoRedis(persons []models.Person)  {
	conn := redisPool.Pool.Get()
	defer conn.Close()
	for _,p := range persons{
		fmt.Println("--------",p.Name)
		p_byte,_ := json.Marshal(p)
		_,err1 := conn.Do("SETNX",p.Id,p_byte)
		_,err2 := conn.Do("lpush","mlist",p.Id)
		// 设置p_id的过期时间
		conn.Do("EXPIRE",p.Id,10)
		if err1 != nil || err2 != nil {
			fmt.Println("写入失败",err1,err2)
		}else {
			fmt.Println("写入成功")
		}
	}
	//设置存储了很多p_id的过期时间
	conn.Do("EXPIRE","mlist",10)

}






