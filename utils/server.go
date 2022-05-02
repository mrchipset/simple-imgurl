package utils

import (
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

type Api struct {
	session *MinioSession
}

type ListInfo struct {
	path string
}

func (api *Api) putObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	objName := c.Param("obj")
	contentType := c.ContentType()
	content, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Println(err)
		c.String(http.StatusForbidden, err.Error())
		return
	}

	success := api.session.PutObject(bucketName, objName, content, contentType)
	var statusCode int
	if success {
		statusCode = http.StatusOK
	} else {
		statusCode = http.StatusForbidden
	}
	c.String(statusCode, "")
}

func (api *Api) getObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	objName := c.Param("obj")
	obj, err := api.session.GetObject(bucketName, objName)
	if err != nil {
		log.Println(err)
		c.Data(http.StatusNotFound, "text/plain", []byte(err.Error()))
	}

	info, e := obj.Stat()
	if e != nil {
		log.Println(e)
		c.Data(http.StatusNotFound, "text/plain", []byte(e.Error()))
	}

	contentType := info.ContentType
	var buffer []byte
	buffer, err = ioutil.ReadAll(obj)
	if err != nil {
		log.Println(err)
		c.Data(http.StatusNotFound, "text/plain", []byte(err.Error()))

	}
	c.Data(http.StatusOK, contentType, buffer)
}

func (api *Api) delObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	objName := c.Param("obj")
	err := api.session.DeleteObject(bucketName, objName)
	if err != nil {
		log.Println(err)
		c.String(http.StatusForbidden, err.Error())
	}

	c.String(http.StatusOK, "")
}

func (api *Api) listObject(c *gin.Context) {
	bucketName := c.Param("bucket")
	// objName := c.Param("obj")
	objectInfos, err := api.session.ListObjects(bucketName)
	if err != nil {
		log.Println(err)
		c.String(http.StatusForbidden, err.Error())
	}

	c.JSON(http.StatusOK, objectInfos)
}

func RunMinioService() {

	endpoint := os.Getenv("WEB_SERVICE_ENDPOINT")
	router := gin.Default()
	api := router.Group("/api")
	{
		_api := &Api{}
		_api.session = CreateMinioSession()
		api.PUT("/obj/:bucket/:obj", _api.putObject)
		api.GET("/obj/:bucket/:obj", _api.getObject)
		api.DELETE("/obj/:bucket/:obj", _api.delObject)
		api.GET("/list/:bucket/:item_count/:page_id", _api.listObject)
	}
	router.Run(endpoint)

}
