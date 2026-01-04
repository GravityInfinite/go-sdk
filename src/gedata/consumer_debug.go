package gedata

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// GEDebugConsumer The data is reported one by one, and when an error occurs, the log will be printed on the console.
type GEDebugConsumer struct {
	serverUrl string // serverUrl
	writeData bool   // is archive to GE
}

// NewDebugConsumer init GEDebugConsumer
func NewDebugConsumer(serverUrl string) (GEConsumer, error) {
	return NewDebugConsumerWithWriter(serverUrl, true)
}

func NewDebugConsumerWithWriter(serverUrl string, writeData bool) (GEConsumer, error) {
	// enable console log
	SetLogLevel(GELogLevelDebug)

	if len(serverUrl) <= 0 {
		msg := fmt.Sprint("ServerUrl not be empty")
		geLogError(msg)
		return nil, errors.New(msg)
	}

	c := &GEDebugConsumer{serverUrl: serverUrl, writeData: writeData}

	geLogInfo("Mode: debug consumer,serverUrl: %s", c.serverUrl)

	return c, nil
}

func (c *GEDebugConsumer) Add(d Data) error {

	jsonBytes, err := json.Marshal(d)
	if err != nil {
		return err
	}

	jsonStr := string(jsonBytes)

	geLogInfo("%v", jsonStr)
	geLogDebug("send len(EventList)： %v", len(d.EventList))

	return c.send(jsonStr)
}

func (c *GEDebugConsumer) Flush() error {
	geLogInfo("flush data")
	return nil
}

func (c *GEDebugConsumer) Close() error {
	geLogInfo("debug consumer close")
	return nil
}

func (c *GEDebugConsumer) IsStringent() bool {
	return true
}

func (c *GEDebugConsumer) send(data string) error {

	postData := strings.NewReader(data)
	contentType := "application/json"
	resp, err := http.Post(c.serverUrl, contentType, postData)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		result := map[string]interface{}{}
		err = json.Unmarshal(body, &result)
		geLogDebug("send result: %v", result)
		if err != nil {
			return err
		}
		if uint64(result["code"].(float64)) != 0 {
			msg := fmt.Sprintf("send to receiver failed with return content:  %s", string(body))
			geLogError(msg)
			return errors.New(msg)
		} else {
			geLogInfo("send success: %v", result)
		}
	} else {
		return errors.New(fmt.Sprintf("Unexpected Sgetus Code: %d", resp.StatusCode))
	}
	return nil
}
