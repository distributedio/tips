package main

import (
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

func makeRequest(t testing.TB, url string, method string, reader io.Reader) (int, string) {
	req, err := http.NewRequest(method, url, reader)
	require.NoError(t, err, "Error constructing %s request.", method)

	res, err := http.DefaultClient.Do(req)
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
//7.拉去消息3 条
//8.创建snapshot
//9.获取snapshot
//10.继续拉取消息
//11.查找快照位置
//11.继续快照位置拉取消息
//12.销毁消息
//13.发送消息失败 ==== 无人订阅
//14.删除快照
//15.销毁topic
func TestNormal(t *testing.T) {
	code, body := makeRequest(t, "/v1/topics/topic-normal", "PUT", nil)
	assertCodeOK(t, code)
	fmt.Println(body)
	code, body = makeRequest(t, "/v1/messages/topic", "POST", strings.NewReader(`{"topic":"/v1/topics/topic-normal","messages":["h"]}`))
	assertCodeOK(t, code)
	fmt.Println(body)
	//TODO body
}

//1.非法订阅
//2.非法取消订阅
//3.非法发送消息
//4.非法拉取消息
//5.非法销毁消息
func TestIllagel(t *testing.T) {

}
