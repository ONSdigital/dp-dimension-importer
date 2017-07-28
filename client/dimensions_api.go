package client

import (
	"encoding/json"
	"fmt"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"io/ioutil"
	"net/http"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
)

// import API Get DimensionsKey URL format
const getDimensionsURIFMT = "%s/instances/%s/dimensions"
const putDimensionNodeIDURI = "%s/instances/%s/dimensions/%s/node_id/%s"

// error messages
const getDimensionsSuccessMsg = "Get Dimensions success"
const getDimensionsErrMsg = "Get dimensions returned error status"
const unmarshallingErrMsg = "Unexpected error while unmarshalling response"
const unexpectedAPIErrMsg = "Unexpected error returned when calling Import API"
const hostConfigMissingMsg = "DimensionsClient requires an API host to be configured"
const marshalDimensionErr = "Unexpected error while marshalling dimenison"
const createSetNodeIDReqErr = "Unexpecter error creating request struct"
const setDimensionNodeIDErr = "Set Dimension node_id returned error status"
const setDimensionNodeIDReqErr = "Error sending set Dimension node_id request"
const setDimNodeIDSuccessMsg = "Set Dimension node_id success"

// errors
var instanceIDNotFoundErr = errors.New(getDimensionsErrMsg)
var internalServerErr = errors.New(getDimensionsErrMsg)
var unexpectedErr = errors.New(getDimensionsErrMsg)
var missingConfigErr = errors.New("DimensionsClient is missing required configuration.")
var setDimNodeIdErr = errors.New(setDimensionNodeIDErr)
var errInstanceIDRequired = errors.New("instanceID is required but is empty")

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

var httpCli HTTPClient = &http.Client{}

// Host the host to get the dimensions from.
var Host string

// httpGet abstraction around http.Get to simplify testing / mocking.
var httpGet = http.Get

// respBodyReader abstraction around ioutil.ReadAll to simplify testing / mocking.
var respBodyReader = ioutil.ReadAll

type ImportAPI struct{}

// Get perform a HTTP GET request to the dp-import-api to retrieve the dataset dimenions for the specified instanceID
func (api ImportAPI) GetDimensions(instanceID string) ([]*model.Dimension, error) {
	if len(Host) == 0 {
		log.Debug(hostConfigMissingMsg, nil)
		return nil, missingConfigErr
	}
	if len(instanceID) == 0 {
		return nil, errInstanceIDRequired
	}

	url := fmt.Sprintf(getDimensionsURIFMT, Host, instanceID)
	data := log.Data{
		logKeys.InstanceID: instanceID,
		logKeys.URL:        url,
	}

	res, err := httpGet(url)
	if err != nil {
		data[logKeys.ErrorDetails] = err.Error()
		log.Debug(unexpectedAPIErrMsg, data)
		return nil, err
	}

	data[logKeys.RespStatusCode] = res.StatusCode
	defer res.Body.Close()

	switch res.StatusCode {
	case 200:
		log.Debug(getDimensionsSuccessMsg, data)
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

	var dims []*model.Dimension
	err = json.Unmarshal(body, &dims)

	if err != nil {
		data[logKeys.ErrorDetails] = err.Error()
		log.Debug(unmarshallingErrMsg, data)
		return nil, err
	}
	return dims, nil
}

func (api ImportAPI) SetDimensionNodeID(instanceID string, d *model.Dimension) error {
	if len(Host) == 0 {
		log.Debug(hostConfigMissingMsg, nil)
		return missingConfigErr
	}
	if len(instanceID) == 0 {
		return errInstanceIDRequired
	}

	logData := make(map[string]interface{}, 0)
	logData[logKeys.InstanceID] = instanceID
	logData[logKeys.DimensionsKey] = d.Dimension_ID
	logData[logKeys.NodeID] = d.NodeId

	url := fmt.Sprintf(putDimensionNodeIDURI, Host, instanceID, d.Dimension_ID, d.NodeId)
	logData[logKeys.URL] = url

	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		log.ErrorC(createSetNodeIDReqErr, err, nil)
		return err
	}

	resp, err := httpCli.Do(req)
	if err != nil {
		log.ErrorC(setDimensionNodeIDReqErr, err, logData)
		return err
	}

	defer resp.Body.Close()

	logData[logKeys.RespStatusCode] = resp.StatusCode
	switch resp.StatusCode {
	case 200:
		log.Debug(setDimNodeIDSuccessMsg, logData)
		return nil
	case 404:
		log.Debug(setDimensionNodeIDErr, logData)
		return setDimNodeIdErr
	case 500:
		log.Debug(setDimensionNodeIDErr, logData)
		return setDimNodeIdErr
	default:
		log.Debug(setDimensionNodeIDErr, logData)
		return setDimNodeIdErr
	}
}
