package main

import (
	"fmt"

	"time"

	"github.com/GravityInfinite/go-sdk/src/gedata"
)

const ServerUrl = "https://backend.gravity-engine.com/event_center/api/v1/event/collect/?access_token=__XXX__"

const clientId = "_test_client_id_0"
const eventName = "$AdClick"

var trackProperties = func() map[string]interface{} {
	type B struct {
		Trigger string
		Time    time.Time
	}
	type A struct {
		Name  string
		Time  time.Time
		Event []B
	}

	customData := A{
		eventName,
		time.Now(),
		[]B{
			{Trigger: "Now We Support", Time: time.Now()},
			{Trigger: "User Custom Struct Data", Time: time.Now()},
		},
	}
	properties := map[string]interface{}{
		"channel":   "ge",
		"age":       1,
		"isSuccess": true,
		"birthday":  time.Now(),
		"object": map[string]interface{}{
			"key": "value",
		},
		"objectArr": []interface{}{
			map[string]interface{}{
				"key": "value",
			},
		},
		"arr":      []string{"test1", "test2", "test3"},
		"my_data":  customData,
		"time_now": time.Now(),
		"time_2":   "2022-12-12T22:22:22.333444555Z",
	}
	return properties
}()

func mainDebug() {

	// e.g. init consumer, you can choose different consumer

	consumer, err := generateDebugConsumer() // GEDebugConsumer
	//consumer, err := generateBatchConsumer() // GEBatchConsumer

	if err != nil {
		// consumer init error
		panic(err)
	}
	ge := gedata.New(consumer)

	defer func() {
		err = ge.Flush()
		if err != nil {
			fmt.Printf("GE flush error: %v", err.Error())
		}
		err = ge.Close()
		if err != nil {
			fmt.Printf("GE close error: %v\n", err.Error())
		}
	}()

	func() {
		for i := 0; i < 6; i++ {
			p := trackProperties
			p["idx"] = i
			err = ge.Track(clientId, eventName, p)
			if err != nil {
				fmt.Println(err)
			}
		}

	}()

	func() {
		properties := map[string]interface{}{"user_name": "xxx", "count": 1, "arr": []int{11, 22}}
		_ = properties
		err = ge.UserSet(clientId, trackProperties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"user_name": ""}
		err = ge.UserUnset(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"prop_set_once": "111"}
		err = ge.UserSetOnce(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"TotalRevenue": 100}
		err = ge.UserIncrement(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"TotalRevenue": 1}
		err = ge.UserNumMin(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"TotalRevenue": 1000}
		err = ge.UserNumMax(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"prop_list_type": []string{"a", "a"}}
		err = ge.UserAppend(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"prop_list_type": []string{"a", "b", "c", "c"}}
		err = ge.UserUniqAppend(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		err = ge.UserDelete(clientId)
		if err != nil {
			fmt.Println(err)
		}

	}()
}

func mainBatch() {
	// e.g. init consumer, you can choose different consumer

	//consumer, err := generateDebugConsumer() // GEDebugConsumer
	consumer, err := generateBatchConsumer() // GEBatchConsumer

	if err != nil {
		// consumer init error
		panic(err)
	}
	ge := gedata.New(consumer)

	defer func() {
		err = ge.Flush()
		if err != nil {
			fmt.Printf("GE flush error: %v", err.Error())
		}
		err = ge.Close()
		if err != nil {
			fmt.Printf("GE close error: %v\n", err.Error())
		}
	}()

	func() {
		for i := 0; i < 1000; i++ {
			p := trackProperties
			p["idx"] = i
			err = ge.Track(clientId, eventName, p)
			if err != nil {
				fmt.Println(err)
			}
		}

	}()

	func() {
		properties := map[string]interface{}{"user_name": "xxx", "count": 1, "arr": []int{11, 22}}
		err = ge.UserSet(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"user_name": ""}
		err = ge.UserUnset(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"prop_set_once": "111"}
		err = ge.UserSetOnce(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"TotalRevenue": 100}
		err = ge.UserIncrement(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"prop_list_type": []string{"a", "a"}}
		err = ge.UserAppend(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		properties := map[string]interface{}{"prop_list_type": []string{"a", "b", "c", "c"}}
		err = ge.UserUniqAppend(clientId, properties)
		if err != nil {
			fmt.Println(err)
		}

	}()

	func() {
		err = ge.UserDelete(clientId)
		if err != nil {
			fmt.Println(err)
		}

	}()

}

func main() {
	// enable console log
	gedata.SetLogLevel(gedata.GELogLevelDebug)
	func() {
		mainDebug()
		//mainBatch()
	}()
	fmt.Println("ok")
}

func generateBatchConsumer() (gedata.GEConsumer, error) {
	config := gedata.GEBatchConfig{
		ServerUrl: ServerUrl,
		AutoFlush: true,
		BatchSize: 100,
		Interval:  20,
		Compress:  true,
	}
	return gedata.NewBatchConsumerWithConfig(config)
}

func generateDebugConsumer() (gedata.GEConsumer, error) {
	return gedata.NewDebugConsumer(ServerUrl)
}
