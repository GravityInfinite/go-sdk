package gedata

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GEBatchConsumer upload data to GE by http
type GEBatchConsumer struct {
	serverUrl   string // serverUrl
	compress    bool   // is need compress
	bufferMutex *sync.RWMutex
	cacheMutex  *sync.RWMutex // cache mutex

	buffer        []Data
	batchSize     int      // flush event count each time
	cacheBuffer   [][]Data // buffer
	cacheCapacity int      // buffer max count
	HttpClient    *http.Client
}

type GEBatchConfig struct {
	ServerUrl     string       // serverUrl
	BatchSize     int          // flush event count each time
	Timeout       int          // http timeout (mill second)
	Compress      bool         // enable compress data
	AutoFlush     bool         // enable auto flush
	Interval      int          // auto flush spacing (second)
	CacheCapacity int          // cache event count
	HttpClient    *http.Client // Custom http client. Set this parameter when you want to use your own http client
}

const (
	DefaultTimeOut       = 30000
	DefaultBatchSize     = 20
	MaxBatchSize         = 200
	DefaultInterval      = 30
	DefaultCacheCapacity = 50
)

// NewBatchConsumer create GEBatchConsumer
func NewBatchConsumer(serverUrl string) (GEConsumer, error) {
	config := GEBatchConfig{
		ServerUrl: serverUrl,
		Compress:  true,
	}
	return initBatchConsumer(config)
}

// NewBatchConsumerWithBatchSize create GEBatchConsumer
// serverUrl
// batchSize: flush event count each time
func NewBatchConsumerWithBatchSize(serverUrl string, batchSize int) (GEConsumer, error) {
	config := GEBatchConfig{
		ServerUrl: serverUrl,
		Compress:  true,
		BatchSize: batchSize,
	}
	return initBatchConsumer(config)
}

// NewBatchConsumerWithCompress create GEBatchConsumer
// serverUrl
// compress: enable data compress
func NewBatchConsumerWithCompress(serverUrl string, compress bool) (GEConsumer, error) {
	config := GEBatchConfig{
		ServerUrl: serverUrl,
		Compress:  compress,
	}
	return initBatchConsumer(config)
}

func NewBatchConsumerWithConfig(config GEBatchConfig) (GEConsumer, error) {
	return initBatchConsumer(config)
}

func initBatchConsumer(config GEBatchConfig) (GEConsumer, error) {
	if config.ServerUrl == "" {
		msg := fmt.Sprint("ServerUrl not be empty")
		geLogInfo(msg)
		return nil, errors.New(msg)
	}
	u, err := url.Parse(config.ServerUrl)
	if err != nil {
		return nil, err
	}

	var batchSize int
	if config.BatchSize > MaxBatchSize {
		batchSize = MaxBatchSize
	} else if config.BatchSize <= 0 {
		batchSize = DefaultBatchSize
	} else {
		batchSize = config.BatchSize
	}

	var cacheCapacity int
	if config.CacheCapacity <= 0 {
		cacheCapacity = DefaultCacheCapacity
	} else {
		cacheCapacity = config.CacheCapacity
	}

	var timeout time.Duration
	if config.Timeout == 0 {
		timeout = time.Duration(DefaultTimeOut) * time.Millisecond
	} else {
		timeout = time.Duration(config.Timeout) * time.Millisecond
	}

	httpClient := config.HttpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	}

	c := &GEBatchConsumer{
		serverUrl:     u.String(),
		compress:      config.Compress,
		bufferMutex:   new(sync.RWMutex),
		cacheMutex:    new(sync.RWMutex),
		batchSize:     batchSize,
		buffer:        make([]Data, 0, batchSize),
		cacheCapacity: cacheCapacity,
		cacheBuffer:   make([][]Data, 0, cacheCapacity),
		HttpClient:    httpClient,
	}

	var interval int
	if config.Interval == 0 {
		interval = DefaultInterval
	} else {
		interval = config.Interval
	}
	if config.AutoFlush {
		go func() {
			ticker := time.NewTicker(time.Duration(interval) * time.Second)
			defer ticker.Stop()
			for {
				<-ticker.C
				_ = c.timerFlush()
			}
		}()
	}

	geLogInfo("Mode: batch consumer,  serverUrl: %s", c.serverUrl)

	return c, nil
}

func (c *GEBatchConsumer) Add(d Data) error {
	c.bufferMutex.Lock()
	c.buffer = append(c.buffer, d)
	c.bufferMutex.Unlock()

	if c.getBufferLength() >= c.batchSize || c.getCacheLength() > 0 {
		err := c.Flush()
		return err
	}

	return nil
}

func (c *GEBatchConsumer) timerFlush() error {
	geLogInfo("timer flush data")
	return c.innerFlush()
}

func (c *GEBatchConsumer) Flush() error {
	geLogInfo("flush data")
	return c.innerFlush()
}

func (c *GEBatchConsumer) innerFlush() error {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	c.bufferMutex.Lock()
	defer c.bufferMutex.Unlock()

	if len(c.buffer) == 0 && len(c.cacheBuffer) == 0 {
		geLogInfo("flush data: len(c.buffer) == 0 && len(c.cacheBuffer) == 0")

		return nil
	}

	defer func() {
		if len(c.cacheBuffer) > c.cacheCapacity {
			c.cacheBuffer = c.cacheBuffer[1:]
		}
	}()

	if len(c.cacheBuffer) == 0 || len(c.buffer) >= c.batchSize {
		c.cacheBuffer = append(c.cacheBuffer, c.buffer)
		c.buffer = make([]Data, 0, c.batchSize)
	}

	err := c.uploadEvents()

	return err
}

func (c *GEBatchConsumer) uploadEvents() error {
	buffer := c.cacheBuffer[0]
	clientIdMap := map[string][]EventListItem{}
	for _, i := range buffer {
		clientId := i.ClientId
		if _, ok := clientIdMap[clientId]; ok {
			clientIdMap[clientId] = append(clientIdMap[clientId], i.EventList...)
		} else {
			clientIdMap[clientId] = i.EventList
		}
	}
	for clientId, buffer := range clientIdMap {
		d := Data{
			ClientId:  clientId,
			EventList: buffer,
		}
		geLogDebug("send len(EventList)： %v", len(buffer))

		jsonBytes, err := json.Marshal(d)
		if err == nil {
			params := string(jsonBytes)
			for i := 0; i < 3; i++ {
				//statusCode, code, err := c.send(params, len(buffer))
				statusCode, code, err := c.send(params, 1)
				if statusCode == 200 {
					switch code {
					case 0:
						c.cacheBuffer = c.cacheBuffer[1:]
						geLogInfo("send success： %v", params)
						return nil
					default:
						msg := "unknown error"
						geLogError(msg)
						return fmt.Errorf(msg)
					}
				} else {
					if err != nil {
						geLogError(err.Error())
						return err
					} else {
						if i == 2 {
							msg := fmt.Sprintf("network error, but err is nil. Sgetus code is: %v", statusCode)
							geLogError(msg)
							return fmt.Errorf(msg)
						}
					}
				}
			}
		}
	}

	return nil
}

func (c *GEBatchConsumer) FlushAll() error {
	for c.getCacheLength() > 0 || c.getBufferLength() > 0 {
		if err := c.Flush(); err != nil {
			if !strings.Contains(err.Error(), "GEDataError") {
				return err
			}
		}
	}
	return nil
}

func (c *GEBatchConsumer) Close() error {
	geLogInfo("batch consumer close")
	return c.FlushAll()
}

func (c *GEBatchConsumer) IsStringent() bool {
	return false
}

func (c *GEBatchConsumer) send(data string, size int) (statusCode int, code int, err error) {
	var encodedData string
	var compressType = "gzip"
	if c.compress {
		encodedData, err = encodeData(data)
	} else {
		encodedData = data
		compressType = "none"
	}
	if err != nil {
		return 0, 0, err
	}
	postData := bytes.NewBufferString(encodedData)

	var resp *http.Response
	req, _ := http.NewRequest("POST", c.serverUrl, postData)
	req.Header.Set("user-agent", "ge-go-sdk")
	req.Header.Set("version", SdkVersion)
	//req.Header.Set("compress", compressType)
	req.Header.Set("Gravity-Content-Compress", compressType)
	req.Header["GE-Integration-Type"] = []string{LibName}
	req.Header["GE-Integration-Version"] = []string{SdkVersion}
	req.Header["GE-Integration-Count"] = []string{strconv.Itoa(size)}
	resp, err = c.HttpClient.Do(req)

	if err != nil {
		return 0, 0, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			geLogError("close response body error: %v", err)
		}
	}(resp.Body)

	if resp.StatusCode == http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		geLogDebug(string(body))

		var result struct {
			Code int
		}

		err = json.Unmarshal(body, &result)
		if err != nil {
			return resp.StatusCode, 1, err
		}

		return resp.StatusCode, result.Code, nil
	} else {
		return resp.StatusCode, -1, nil
	}
}

// Gzip
func encodeData(data string) (string, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)

	_, err := gw.Write([]byte(data))
	if err != nil {
		gw.Close()
		return "", err
	}
	gw.Close()

	return string(buf.Bytes()), nil
}

func (c *GEBatchConsumer) getBufferLength() int {
	c.bufferMutex.RLock()
	defer c.bufferMutex.RUnlock()
	return len(c.buffer)
}

func (c *GEBatchConsumer) getCacheLength() int {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	return len(c.cacheBuffer)
}
