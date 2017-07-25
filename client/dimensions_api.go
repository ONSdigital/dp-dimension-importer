package client

import (
	"encoding/json"
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"io/ioutil"
	"net/http"
	"github.com/ONSdigital/dp-dimension-importer/common"
)

// import API Get DimensionsKey URL format
const dimensionsHostFMT = "%s/instance/%s/dimensions"

// error messages
const okResponseMsg = "Get DimensionsKey success"
const getDimensionsErrMsg = "Get dimensions returned error status"
const unmarshallingErrMsg = "Unexpected error while unmarshalling response"
const unexpectedAPIErrMsg = "Unexpected error returned when calling Import API"
const hostConfigMissingMsg = "DimensionsClient requires an API host to be configured."

// errors
var instanceIDNotFoundErr = errors.New(getDimensionsErrMsg)
var internalServerErr = errors.New(getDimensionsErrMsg)
var unexpectedErr = errors.New(getDimensionsErrMsg)
var missingConfigErr = errors.New("DimensionsClient is missing required configuration.")

// debug data map keys
const urlKey = "url"
const statusCodeKey = "statusCode"
const errDetailsKey = "details"

// Host the host to get the dimensions from.
var Host string

// httpGet abstraction around http.Get to simplify testing / mocking.
var httpGet = http.Get

// respBodyReader abstraction around ioutil.ReadAll to simplify testing / mocking.
var respBodyReader = ioutil.ReadAll

// Get perform a HTTP GET request to the dp-import-api to retrieve the dataset dimenions for the specified instanceID
func GetDimensions(instanceID string) (*model.Dimensions, error) {
	if len(Host) == 0 {
		log.Debug(hostConfigMissingMsg, nil)
		return nil, missingConfigErr
	}
	if len(instanceID) == 0 {
		return nil, common.ErrInstanceIDRequired
	}

	url := fmt.Sprintf(dimensionsHostFMT, Host, instanceID)
	data := log.Data{
		common.InstanceIDKey: instanceID,
		urlKey:               url,
	}

	res, err := httpGet(url)
	if err != nil {
		data[errDetailsKey] = err.Error()
		log.Debug(unexpectedAPIErrMsg, data)
		return nil, err
	}

	data[statusCodeKey] = res.StatusCode
	defer res.Body.Close()

	switch res.StatusCode {
	case 200:
		log.Debug(okResponseMsg, data)
	case 404:
		log.Debug(getDimensionsErrMsg, data)
		return nil, instanceIDNotFoundErr
	case 500:
		log.Debug(getDimensionsErrMsg, data)
		return nil, internalServerErr
	default:
		log.Debug(getDimensionsErrMsg, data)
		return nil, unexpectedErr
	}

	body, err := respBodyReader(res.Body)
	if err != nil {
		return nil, err
	}

	var dims model.Dimensions
	err = json.Unmarshal(body, &dims)

	if err != nil {
		data[errDetailsKey] = err.Error()
		log.Debug(unmarshallingErrMsg, data)
		return nil, err
	}
	return &dims, nil
}
