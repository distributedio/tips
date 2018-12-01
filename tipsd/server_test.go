package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/shafreeck/tips"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertCodeOK(t testing.TB, code int) {
	assert.Equal(t, http.StatusOK, code, "Unexpected response status code.")
}

func assertCodeBadRequest(t testing.TB, code int) {
	assert.Equal(t, http.StatusBadRequest, code, "Unexpected response status code.")
}

func assertSnapBody(t testing.TB, body string, id int64) {
	var snap tips.Subscription
	json.Unmarshal([]byte(body), &snap)
	assert.Equal(t, snap.Acked.Index, id)
}

func assertBodyLen(t testing.TB, body string, llen int, payload string) {
	msgs := []*struct {
		Payload []byte
		ID      string
	}{}
	json.Unmarshal([]byte(body), &msgs)
	assert.Len(t, msgs, llen)
	if llen > 0 {
		assert.Equal(t, string(msgs[len(msgs)-1].Payload), payload)
	}
}

func EndMessageID(body string) string {
	msgs := []*struct {
		Payload []byte
		ID      string
	}{}
	json.Unmarshal([]byte(body), &msgs)
	return msgs[len(msgs)-1].ID
}

func assertCodeNotFound(t testing.TB, code int) {
	assert.Equal(t, http.StatusNotFound, code, "Unexpected response status code.")
}

func makeRequest(t testing.TB, url string, method string, reader io.Reader) (int, string) {
	req, err := http.NewRequest(method, url, reader)
	require.NoError(t, err, "Error constructing %s request.", method)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert},
	}
	client := &http.Client{Transport: tr}
	res, err := client.Do(req)
	require.NoError(t, err, "Error making %s request.", method)
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err, "Error reading request body.")

	return res.StatusCode, string(body)
}

//创建topic
//发送消息失败 ==== 无人订阅
//创建订阅关系
//查询订阅关系
//查询topic 订阅关系
//发送消息 ===== 10
//拉去消息1条
//拉去消息3 条
//回复ack
//拉去消息3 条
//创建snapshot
//拉去消息3 条
//获取snapshot
//继续拉取消息
//查找快照位置
//继续快照位置拉取消息
//发送消息失败 ==== 无人订阅
//删除快照
//销毁topic
func TestNormal(t *testing.T) {
	code, body := makeRequest(t, url+"/v1/topics/topic-normal", "PUT", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "topic-normal")

	code, body = makeRequest(t, url+"/v1/messages/topics/topic-normal", "POST", strings.NewReader(`{"messages":["h"]}`))
	assertCodeOK(t, code)
	//校验长度
	assert.Len(t, strings.Split(body, ","), 1)

	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "PUT", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "0")

	code, body = makeRequest(t, url+"/v1/topics/topic-normal", "GET", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "topic-normal")

	code, body = makeRequest(t, url+"/v1/messages/topics/topic-normal", "POST", strings.NewReader(`{"messages":["0","1","2","3","4","5","6","7","8","9"]}`))
	assertCodeOK(t, code)
	assert.Len(t, strings.Split(body, ","), 10)

	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(`{}`))
	assertCodeOK(t, code)
	assertBodyLen(t, body, 1, "0")

	method := fmt.Sprintf(`{"ack":true,"index":"%s","limit":3}`, EndMessageID(body))
	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(method))
	assertCodeOK(t, code)
	assertBodyLen(t, body, 3, "3")

	code, body = makeRequest(t, url+"/v1/messages/ack/topic-normal/subname-normal/"+EndMessageID(body), "POST", nil)
	assertCodeOK(t, code)

	method = fmt.Sprintf(`{"limit":3}`)
	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(method))
	assertCodeOK(t, code)
	assertBodyLen(t, body, 3, "6")

	code, body = makeRequest(t, url+"/v1/snapshots/shot/subname-normal/topic-normal", "PUT", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "shot")

	method = fmt.Sprintf(`{"limit":3}`)
	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(method))
	assertCodeOK(t, code)
	assertBodyLen(t, body, 3, "9")

	code, body = makeRequest(t, url+"/v1/snapshots/shot/subname-normal/topic-normal", "POST", nil)
	assertCodeOK(t, code)
	assertSnapBody(t, body, 6)
	// fmt.Println(body)
	//TODO

	method = fmt.Sprintf(`{"limit":3}`)
	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(method))
	assertCodeOK(t, code)
	assertBodyLen(t, body, 3, "9")

	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(method))
	assertCodeOK(t, code)
	assertBodyLen(t, body, 0, "0")

	code, body = makeRequest(t, url+"/v1/snapshots/shot/subname-normal/topic-normal", "DELETE", nil)
	assertCodeOK(t, code)

	code, body = makeRequest(t, url+"/v1/topics/topic-normal", "DELETE", nil)
	assertCodeOK(t, code)
}

func TestIllagel(t *testing.T) {
	code, body := makeRequest(t, url+"/v1/messages/topic-normal", "POST", strings.NewReader(`{"topic":"topic-nor","messages":["h"]}`))
	assertCodeNotFound(t, code)
	assert.Contains(t, body, "not found")

	code, body = makeRequest(t, url+"/v1/topics/topic-normal", "GET", nil)
	assertCodeNotFound(t, code)
	assert.Contains(t, body, "not found")

	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "GET", nil)
	assertCodeNotFound(t, code)
	assert.Contains(t, body, "not found")

	code, body = makeRequest(t, url+"/v1/snapshots/shot/subname-normale/hehe", "POST", nil)
	assertCodeNotFound(t, code)
	assert.Contains(t, body, "not found")

	code, body = makeRequest(t, url+"/v1/snapshots/shot/subname-normale/he", "DELETE", nil)
	assertCodeNotFound(t, code)
	assert.Contains(t, body, "not found")
}

func TestPull(t *testing.T) {
	code, body := makeRequest(t, url+"/v1/topics/topic-normal", "PUT", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "topic-normal")
	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "PUT", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "0")
	go func() {
		time.Sleep(time.Millisecond * 100)
		code, body = makeRequest(t, url+"/v1/messages/topics/topic-normal", "POST", strings.NewReader(`{"messages":["h"]}`))
		assertCodeOK(t, code)
	}()

	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(`{}`))
	assertCodeOK(t, code)
	assertBodyLen(t, body, 1, "h")

	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "DELETE", nil)
	assertCodeOK(t, code)

	code, body = makeRequest(t, url+"/v1/topics/topic-normal", "DELETE", nil)
	assertCodeOK(t, code)
}
