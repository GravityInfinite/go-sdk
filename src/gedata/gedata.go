package gedata

import (
	"errors"
	"sync"
	"time"
)

const (
	Track          = "track"
	Profile        = "profile"
	UserSet        = "profile_set"
	UserSetOnce    = "profile_set_once"
	UserUnset      = "profile_unset"
	UserIncrement  = "profile_increment"
	UserNumMax     = "profile_number_max"
	UserNumMin     = "profile_number_min"
	UserAppend     = "profile_append"
	UserUniqAppend = "profile_uniq_append"
	UserDel        = "profile_delete"

	SdkVersion = "1.0.0"
	LibName    = "go"
)

/*
{'client_id': '_test_client_id_0', 'event_list': [{'type': 'profile', 'event': 'profile_set',
'time': 1765866851234, 'time_free': False, 'properties':
{'$name': 'user_$name2025-12-16 14:34:11.234384', 'count': 1, 'arr': ['111', '222']}}]}

request={'client_id': '_test_client_id_0', 'event_list': [{'type': 'profile', 'event': 'profile_increment',
'time': 1765866851757, 'time_free': False, 'properties': {'setnum1': 100}}]}
*/
type EventListItem struct {
	TimeFree   bool                   `json:"time_free"`
	Type       string                 `json:"type"`
	EventName  string                 `json:"event"`
	Time       int64                  `json:"time"`
	Properties map[string]interface{} `json:"properties"`
}

type Data struct {
	ClientId  string          `json:"client_id"`
	EventList []EventListItem `json:"event_list"` // "event_list,omitempty"
}

// GEConsumer define operation interface
type GEConsumer interface {
	Add(d Data) error
	Flush() error
	Close() error
	IsStringent() bool // check data or not.
}

type GEAnalytics struct {
	consumer GEConsumer
	mutex    *sync.RWMutex
}

// New init SDK
func New(c GEConsumer) GEAnalytics {
	geLogInfo("init SDK success")
	return GEAnalytics{
		consumer: c,
		mutex:    new(sync.RWMutex),
	}
}

// Track report ordinary event
func (ge *GEAnalytics) Track(clientId, eventName string, properties map[string]interface{}) error {
	return ge.track(clientId, eventName, properties)
}

func (ge *GEAnalytics) track(clientId, eventName string, properties map[string]interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			geLogError("%+v\nData: %+v", r, properties)
		}
	}()

	if len(eventName) == 0 {
		msg := "the event name must be provided"
		geLogError(msg)
		return errors.New(msg)
	}

	p := map[string]interface{}{}

	p["$lib"] = LibName
	p["$lib_version"] = SdkVersion
	mergeProperties(p, properties)

	return ge.add(clientId, Track, eventName, p)
}

// UserSet set user properties. would overwrite existing names.
func (ge *GEAnalytics) UserSet(clientId string, properties map[string]interface{}) error {
	return ge.user(clientId, UserSet, properties)
}

// UserUnset clear the user properties of users.
func (ge *GEAnalytics) UserUnset(clientId string, properties map[string]interface{}) error {
	if len(properties) == 0 {
		msg := "invalid params for UserUnset: properties is nil"
		geLogInfo(msg)
		return errors.New(msg)
	}
	return ge.user(clientId, UserUnset, properties)
}

// UserSetOnce set user properties, If such property had been set before, this message would be neglected.
func (ge *GEAnalytics) UserSetOnce(clientId string, properties map[string]interface{}) error {
	return ge.user(clientId, UserSetOnce, properties)
}

// UserIncrement to accumulate operations against the property.
func (ge *GEAnalytics) UserIncrement(clientId string, properties map[string]interface{}) error {
	return ge.user(clientId, UserIncrement, properties)
}

// UserAppend to add user properties of array type.
func (ge *GEAnalytics) UserAppend(clientId string, properties map[string]interface{}) error {
	return ge.user(clientId, UserAppend, properties)
}

// UserUniqAppend append user properties to array type by unique.
func (ge *GEAnalytics) UserUniqAppend(clientId string, properties map[string]interface{}) error {
	return ge.user(clientId, UserUniqAppend, properties)
}

// UserDelete delete a user, This operation cannot be undone.
func (ge *GEAnalytics) UserDelete(clientId string) error {
	return ge.user(clientId, UserDel, map[string]interface{}{"": ""})
}

func (ge *GEAnalytics) UserNumMax(clientId string, properties map[string]interface{}) error {
	return ge.user(clientId, UserNumMax, properties)
}

func (ge *GEAnalytics) UserNumMin(clientId string, properties map[string]interface{}) error {
	return ge.user(clientId, UserNumMin, properties)
}

func (ge *GEAnalytics) user(clientId, eventName string, properties map[string]interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			geLogError("%+v\nData: %+v", r, properties)
		}
	}()

	p := make(map[string]interface{})
	mergeProperties(p, properties)
	return ge.add(clientId, Profile, eventName, p)
}

// Flush report data immediately.
func (ge *GEAnalytics) Flush() error {
	return ge.consumer.Flush()
}

// Close and exit sdk
func (ge *GEAnalytics) Close() error {
	err := ge.consumer.Close()
	geLogInfo("SDK close")
	return err
}

func (ge *GEAnalytics) add(clientId, dataType, eventName string, properties map[string]interface{}) error {
	item := EventListItem{
		Type:       dataType,
		EventName:  eventName,
		Time:       time.Now().UnixMilli(),
		Properties: properties,
	}
	data := Data{
		ClientId:  clientId,
		EventList: []EventListItem{item},
	}

	//err := formatProperties(&data, ge)
	//if err != nil {
	//	return err
	//}

	return ge.consumer.Add(data)
}

// Deprecated: please use GEConsumer
type Consumer interface {
	GEConsumer
}
