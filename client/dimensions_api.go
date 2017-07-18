package client

import (
	"encoding/json"
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"io/ioutil"
	"net/http"
)

// import API Get Dimensions URL format
const dimensionsHostFMT = "%s/instance/%s/dimensions"

// error messages
const okResponseMsg = "200 Response returned from Import API"
const notFoundMsg = "404 Response returned from Import API"
const badReqMsg = "500 Response returned from Import API"
const unexpectedErrMsg = "Unexpected response status returned from Import API"
const unmarshallingErrMsg = "Unexpected error while unmarshalling response"
const unexpectedAPIErrMsg = "Unexpected error returned when calling Import API"
const hostConfigMissingMsg = "DimensionsClientImpl requires an API host to be configured."

// errors
const instanceIDRequiredErr = ClientError("instanceID is a mandatory field.")
const instanceIDNotFoundErr = ClientError("instanceID does not match any jobs")
const internalServerErr = ClientError("Failed to process the request due to an internal error")
const unexpectedErr = ClientError("Unexpected response status returned from Import API")
const missingConfigErr = ClientError("DimensionsClientImpl is missing required configuration.")

// debug data map keys
const instanceIDKey = "instanceID"
const urlKey = "url"
const statusCodeKey = "statusCode"
const errDetailsKey = "details"

// httpGet abstraction around http.Get to simplify testing / mocking.
var httpGet = http.Get

// respBodyReader abstraction around ioutil.ReadAll to simplify testing / mocking.
var respBodyReader = ioutil.ReadAll

// unmarshal abstraction around json.Unmarshal to simplify testing / mocking.
var unmarshal = json.Unmarshal

// DimensionsClientImpl provides implementation for making HTTP GET requests to the dp-import-api to retrieve the
// dataset dimensions for a given instanceID.
type DimensionsClientImpl struct {
	Host string
}

// Get perform a HTTP GET request to the dp-import-api to retrieve the dataset dimenions for the specified instanceID
func (i DimensionsClientImpl) Get(instanceID string) (*model.Dimensions, error) {
	if len(i.Host) == 0 {
		log.Debug(hostConfigMissingMsg, nil)
		return nil, missingConfigErr
	}
	if len(instanceID) == 0 {
		return nil, instanceIDRequiredErr
	}

	url := fmt.Sprintf(dimensionsHostFMT, i.Host, instanceID)
	data := log.Data{
		instanceIDKey: instanceID,
		urlKey:        url,
	}

	res, err := httpGet(url)
	defer res.Body.Close()

	if err != nil {
		data[errDetailsKey] = err.Error()
		log.Debug(unexpectedAPIErrMsg, data)
		return nil, err
	}

	switch res.StatusCode {
	case 200:
		log.Debug(okResponseMsg, data)
	case 404:
		log.Debug(notFoundMsg, data)
		return nil, instanceIDNotFoundErr
	case 500:
		log.Debug(badReqMsg, data)
		return nil, internalServerErr
	default:
		data[statusCodeKey] = res.StatusCode
		log.Debug(unexpectedErrMsg, data)
		return nil, unexpectedErr
	}

	body, err := respBodyReader(res.Body)
	if err != nil {
		return nil, err
	}

	var dims model.Dimensions
	err = unmarshal(body, &dims)

	if err != nil {
		data[errDetailsKey] = err.Error()
		log.Debug(unmarshallingErrMsg, data)
		return nil, err
	}
	return &dims, nil
}
