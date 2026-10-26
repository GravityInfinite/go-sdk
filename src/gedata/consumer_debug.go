package gedata

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// GEDebugConsumer The data is reported one by one, and when an error occurs, the log will be printed on the console.
type GEDebugConsumer struct {
	serverUrl  string // serverUrl
	writeData  bool   // is archive to GE
	httpClient *http.Client
	callback   func(err error, clientId string) // global error callback, invoked when report failed
}

// GEDebugConfig debug consumer config
type GEDebugConfig struct {
	ServerUrl string                           // serverUrl
	WriteData bool                             // is archive to GE
	Callback  func(err error, clientId string) // global error callback, invoked when report failed; receives (err, clientId)
}

// NewDebugConsumer init GEDebugConsumer
func NewDebugConsumer(serverUrl string) (GEConsumer, error) {
	return NewDebugConsumerWithWriter(serverUrl, true)
}

func NewDebugConsumerWithWriter(serverUrl string, writeData bool) (GEConsumer, error) {
	config := GEDebugConfig{
		ServerUrl: serverUrl,
		WriteData: writeData,
	}
	return NewDebugConsumerWithConfig(config)
}

// NewDebugConsumerWithConfig init GEDebugConsumer with config
func NewDebugConsumerWithConfig(config GEDebugConfig) (GEConsumer, error) {
	// enable console log
	SetLogLevel(GELogLevelDebug)

	if len(config.ServerUrl) <= 0 {
		msg := fmt.Sprint("ServerUrl must not be empty")
		geLogError(msg)
		return nil, errors.New(msg)
	}

	c := &GEDebugConsumer{
		serverUrl:  config.ServerUrl,
		writeData:  config.WriteData,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		callback:   config.Callback,
	}

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
	geLogDebug("send len(EventList): %v", len(d.EventList))

	return c.send(jsonStr, d.ClientId)
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

func (c *GEDebugConsumer) send(data string, clientId string) error {
	postData := strings.NewReader(data)
	req, err := http.NewRequest("POST", c.serverUrl, postData)
	if err != nil {
		c.reportError(err, clientId)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.reportError(err, clientId)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			c.reportError(readErr, clientId)
			return readErr
		}
		result := map[string]interface{}{}
		err = json.Unmarshal(body, &result)
		geLogDebug("send result: %v", result)
		if err != nil {
			c.reportError(err, clientId)
			return err
		}
		codeVal, ok := result["code"]
		if !ok {
			err := errors.New("send to receiver failed: response missing 'code' field")
			c.reportError(err, clientId)
			return err
		}
		codeFloat, ok := codeVal.(float64)
		if !ok {
			err := fmt.Errorf("send to receiver failed: unexpected type for 'code' field: %T", codeVal)
			c.reportError(err, clientId)
			return err
		}
		if uint64(codeFloat) != 0 {
			msg := fmt.Sprintf("send to receiver failed with return content: %s", string(body))
			geLogError(msg)
			err := errors.New(msg)
			c.reportError(err, clientId)
			return err
		}
		geLogInfo("send success: %v", result)
	} else {
		err := fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		c.reportError(err, clientId)
		return err
	}
	return nil
}

// reportError invoke the global error callback when report failed
func (c *GEDebugConsumer) reportError(err error, clientId string) {
	if err != nil && c.callback != nil {
		c.callback(err, clientId)
	}
}
