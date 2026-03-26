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
}

// NewDebugConsumer init GEDebugConsumer
func NewDebugConsumer(serverUrl string) (GEConsumer, error) {
	return NewDebugConsumerWithWriter(serverUrl, true)
}

func NewDebugConsumerWithWriter(serverUrl string, writeData bool) (GEConsumer, error) {
	// enable console log
	SetLogLevel(GELogLevelDebug)

	if len(serverUrl) <= 0 {
		msg := fmt.Sprint("ServerUrl must not be empty")
		geLogError(msg)
		return nil, errors.New(msg)
	}

	c := &GEDebugConsumer{
		serverUrl:  serverUrl,
		writeData:  writeData,
		httpClient: &http.Client{Timeout: 30 * time.Second},
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
	req, err := http.NewRequest("POST", c.serverUrl, postData)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return readErr
		}
		result := map[string]interface{}{}
		err = json.Unmarshal(body, &result)
		geLogDebug("send result: %v", result)
		if err != nil {
			return err
		}
		codeVal, ok := result["code"]
		if !ok {
			return errors.New("send to receiver failed: response missing 'code' field")
		}
		codeFloat, ok := codeVal.(float64)
		if !ok {
			return fmt.Errorf("send to receiver failed: unexpected type for 'code' field: %T", codeVal)
		}
		if uint64(codeFloat) != 0 {
			msg := fmt.Sprintf("send to receiver failed with return content: %s", string(body))
			geLogError(msg)
			return errors.New(msg)
		}
		geLogInfo("send success: %v", result)
	} else {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}
