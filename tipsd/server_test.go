package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertCodeOK(t testing.TB, code int) {
	assert.Equal(t, http.StatusOK, code, "Unexpected response status code.")
}

func assertCodeBadRequest(t testing.TB, code int) {
	assert.Equal(t, http.StatusBadRequest, code, "Unexpected response status code.")
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

//1.创建topic
//2.发送消息失败 ==== 无人订阅
//3.创建订阅关系
//4.查询订阅关系
//5.查询topic 订阅关系
//6.发送消息 ===== 10
//7.拉去消息1条
//7.拉去消息3 条
//回复ack
//8.拉去消息3 条
//8.创建snapshot
//8.拉去消息3 条
//9.获取snapshot
//10.继续拉取消息
//11.查找快照位置
//11.继续快照位置拉取消息
//13.发送消息失败 ==== 无人订阅
//14.删除快照
//15.销毁topic
func TestNormal(t *testing.T) {
	code, body := makeRequest(t, url+"/v1/topics/topic-normal", "PUT", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "topic-normal")

	code, body = makeRequest(t, url+"/v1/messages/topic", "POST", strings.NewReader(`{"topic":"topic-normal","messages":["h"]}`))
	assertCodeOK(t, code)
	//校验长度
	assert.Len(t, strings.Split(body, ","), 1)

	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "PUT", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "0")

	code, body = makeRequest(t, url+"/v1/topics/topic-normal", "GET", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "topic-normal")

	code, body = makeRequest(t, url+"/v1/messages/topic", "POST", strings.NewReader(`{"topic":"topic-normal","messages":["0","1","2","3","4","5","6","7","8","9"]}`))
	assertCodeOK(t, code)
	assert.Len(t, strings.Split(body, ","), 10)

	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(`{}`))
	assertCodeOK(t, code)
	fmt.Println(body)
	//TODO
	// assert.Contains(t, body, "topic-normal")

	code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(`{"ack":true,"limit":3}`))
	assertCodeOK(t, code)
	fmt.Println(body)
	/*
		//TODO
		// assert.Contains(t, body, "topic-normal")
		//TOACK
		code, body = makeRequest(t, url+"/v1/subscriptions/subname-normal/topic-normal", "POST", strings.NewReader(body))
		assertCodeOK(t, code)
		assert.Len(t, strings.Split(body, ","), 10)

	*/
	code, body = makeRequest(t, url+"/v1/snapshots/shot/subname-normal/topic-normal", "PUT", nil)
	assertCodeOK(t, code)
	assert.Contains(t, body, "shot")

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

	code, body = makeRequest(t, url+"/v1/snapshots/shot/subname-topic/hehe", "POST", nil)
	assertCodeNotFound(t, code)
	assert.Contains(t, body, "not found")

	code, body = makeRequest(t, url+"/v1/snapshots/shot/subname-topic/he", "DELETE", nil)
	assertCodeNotFound(t, code)
	assert.Contains(t, body, "not found")
}
