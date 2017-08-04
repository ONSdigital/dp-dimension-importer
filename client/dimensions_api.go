package client

import (
	"encoding/json"
	"errors"
	"fmt"
	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"io/ioutil"
	"net/http"
)

const (
	unmarshallingErr      = "Unexpected error while unmarshalling response"
	unexpectedAPIErr      = "Unexpected error returned when calling Import API: %v"
	hostConfigMissingErr  = "DimensionsClient requires an API host to be configured"
	marshalDimensionErr   = "Unexpected error while marshalling dimenison"
	instanceIDRequiredErr = "instanceID is required but is empty"

	getDimensionsURIFMT  = "%s/instances/%s/dimensions"
	getDimensionsSuccess = "Import-API Get Dimensions success: %v"
	getDimensionsErr     = "Get dimensions returned error status"

	createPutNodeIDReqErr  = "Unexpecter error creating request struct: %v"
	putDimensionNodeIDURI  = "%s/instances/%s/dimensions/%s/node_id/%s"
	putDimNodeIDSuccessLog = "Import-API PUT dimension node_id success: %v"
	putDimNodeIDReqErr     = "Error sending set Dimension node_id request: %v"
	putDimNodeIDErr        = "Set Dimension node_id returned error status: %v"
	dimensionNilErr        = "Dimension is required but was nil"
)

// HTTPClient interface for making HTTP requests.
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

// ImportAPI provides methods for getting dimensions for a given instanceID and updating the node_id of a specific dimension.
type ImportAPI struct{}

// GetDimensions perform a HTTP GET request to the dp-import-api to retrieve the dataset dimenions for the specified instanceID
func (api ImportAPI) GetDimensions(instanceID string) ([]*model.Dimension, error) {
	if len(Host) == 0 {
		err := errors.New(hostConfigMissingErr)
		log.ErrorC(hostConfigMissingErr, err, nil)
		return nil, err
	}
	if len(instanceID) == 0 {
		return nil, errors.New(instanceIDRequiredErr)
	}

	url := fmt.Sprintf(getDimensionsURIFMT, Host, instanceID)
	data := log.Data{
		logKeys.InstanceID: instanceID,
		logKeys.URL:        url,
	}

	res, err := httpGet(url)
	if err != nil {
		data[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(unexpectedAPIErr, err, data)
		return nil, err
	}

	data[logKeys.RespStatusCode] = res.StatusCode
	defer res.Body.Close()

	switch res.StatusCode {
	case 200:
		log.Debug(getDimensionsSuccess, data)
	case 404:
		log.Debug(getDimensionsErr, data)
		return nil, errors.New(getDimensionsErr)
	case 500:
		log.Debug(getDimensionsErr, data)
		return nil, errors.New(getDimensionsErr)
	default:
		log.Debug(getDimensionsErr, data)
		return nil, errors.New(getDimensionsErr)
	}

	body, err := respBodyReader(res.Body)
	if err != nil {
		return nil, err
	}

	var dims []*model.Dimension
	err = json.Unmarshal(body, &dims)

	if err != nil {
		data[logKeys.ErrorDetails] = err.Error()
		log.Debug(unmarshallingErr, data)
		return nil, err
	}
	return dims, nil
}

// PutDimensionNodeID make a HTTP put request to update the node_id of the specified dimension.
func (api ImportAPI) PutDimensionNodeID(instanceID string, d *model.Dimension) error {
	if len(Host) == 0 {
		err := errors.New(hostConfigMissingErr)
		log.ErrorC(hostConfigMissingErr, err, nil)
		return err
	}
	if len(instanceID) == 0 {
		return errors.New(instanceIDRequiredErr)
	}
	if d == nil {
		return errors.New(dimensionNilErr)
	}

	logData := make(map[string]interface{}, 0)
	logData[logKeys.InstanceID] = instanceID
	logData[logKeys.DimensionsKey] = d.Dimension_ID
	logData[logKeys.NodeID] = d.NodeId

	url := fmt.Sprintf(putDimensionNodeIDURI, Host, instanceID, d.Dimension_ID, d.NodeId)
	logData[logKeys.URL] = url

	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		logData[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(createPutNodeIDReqErr, err, logData)
		return err
	}

	resp, err := httpCli.Do(req)
	if err != nil {
		logData[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(putDimNodeIDReqErr, err, logData)
		return err
	}

	defer resp.Body.Close()
	logData[logKeys.RespStatusCode] = resp.StatusCode

	if resp.StatusCode == 200 {
		log.Debug(putDimNodeIDSuccessLog, logData)
		return nil
	}
	err = errors.New(putDimNodeIDErr)
	log.ErrorC(putDimNodeIDErr, err, logData)
	return err
}
