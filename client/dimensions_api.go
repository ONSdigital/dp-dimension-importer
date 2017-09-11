package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	logKeys "github.com/ONSdigital/dp-dimension-importer/common"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
	"io"
	"net/url"
)

//go:generate moq -out ../mocks/dimensions_api_generated_mocks.go -pkg mocks . HTTPClient ResponseBodyReader

const (
	unmarshallingErr      = "unexpected error while unmarshalling response"
	unexpectedAPIErr      = "unexpected error returned when calling dataset api"
	hostConfigMissingErr  = "dimensions client requires an api host to be configured"
	instanceIDRequiredErr = "instance id is required but is empty"

	getInstanceURIFMT  = "%s/instances/%s"
	getInstanceSuccess = "dataset api get instance success"
	getInstanceErr     = "get instance returned error status"

	getDimensionsURIFMT  = "%s/instances/%s/dimensions"
	getDimensionsSuccess = "dataset api get dimensions success"
	getDimensionsErr     = "get dimensions returned error status"

	createPutNodeIDReqErr = "unexpected error creating request struct"
	putDimensionNodeIDURI = "%s/instances/%s/dimensions/%s/options/%s/node_id/%s"
	putDimNodeIDReqErr    = "error sending set dimension node id request"
	putDimNodeIDErr       = "set dimension node id returned error status"
	dimensionNilErr       = "dimension is required but was nil"
	dimensionIDReqErr     = "dimension id is required but was empty"
	unauthorisedResponse  = "dataset api returned unauthorized response status"
	forbiddenResponse     = "dataset api returned forbidden response status"
	authTokenHeader       = "Internal-Token"
	readRespBodyErr       = "unexpected error while attempting to read response body"
	newReqErr             = "unexpected error while attempting to create new http request"
)

// ResponseBodyReader defines a http response body reader.
type ResponseBodyReader interface {
	Read(r io.Reader) ([]byte, error)
}

// HTTPClient interface for making HTTP requests.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// DatasetAPI provides methods for getting dimensions for a given instanceID and updating the node_id of a specific dimension.
type DatasetAPI struct {
	DatasetAPIHost      string
	DatasetAPIAuthToken string
	ResponseBodyReader  ResponseBodyReader
	HTTPClient          HTTPClient
}

func NewDatasetAPI(host string, authToken string, responseBodyReader ResponseBodyReader, httpClient HTTPClient) DatasetAPI {
	return DatasetAPI{
		DatasetAPIHost:      host,
		DatasetAPIAuthToken: authToken,
		ResponseBodyReader:  responseBodyReader,
		HTTPClient:          httpClient,
	}
}

// GetInstance returns instance data from the import API.
func (api DatasetAPI) GetInstance(instanceID string) (*model.Instance, error) {

	if len(api.DatasetAPIHost) == 0 {
		err := errors.New(hostConfigMissingErr)
		log.ErrorC(hostConfigMissingErr, err, nil)
		return nil, err
	}

	if len(instanceID) == 0 {
		return nil, errors.New(instanceIDRequiredErr)
	}

	url := fmt.Sprintf(getInstanceURIFMT, api.DatasetAPIHost, instanceID)
	data := log.Data{
		logKeys.InstanceID: instanceID,
		logKeys.URL:        url,
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.ErrorC(newReqErr, err, data)
		return nil, err
	}

	res, err := api.HTTPClient.Do(req)
	if err != nil {
		data[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(unexpectedAPIErr, err, data)
		return nil, err
	}

	data[logKeys.RespStatusCode] = res.StatusCode
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err := errors.New(getInstanceErr)
		log.ErrorC(getInstanceErr, err, data)
		return nil, err
	}

	body, err := api.ResponseBodyReader.Read(res.Body)
	if err != nil {
		log.ErrorC(readRespBodyErr, err, data)
		return nil, err
	}

	var instance *model.Instance
	JSONErr := json.Unmarshal(body, &instance)
	if JSONErr != nil {
		return nil, JSONErr
	}

	log.Debug(getInstanceSuccess, data)
	return instance, nil
}

// GetDimensions perform a HTTP GET request to the dp-import-api to retrieve the dataset dimenions for the specified instanceID
func (api DatasetAPI) GetDimensions(instanceID string) ([]*model.Dimension, error) {
	if len(api.DatasetAPIHost) == 0 {
		err := errors.New(hostConfigMissingErr)
		log.ErrorC(hostConfigMissingErr, err, nil)
		return nil, err
	}
	if len(instanceID) == 0 {
		return nil, errors.New(instanceIDRequiredErr)
	}

	url := fmt.Sprintf(getDimensionsURIFMT, api.DatasetAPIHost, instanceID)
	data := log.Data{
		logKeys.InstanceID: instanceID,
		logKeys.URL:        url,
	}

	var req *http.Request
	var err error
	if req, err = http.NewRequest(http.MethodGet, url, nil); err != nil {
		log.ErrorC(newReqErr, err, data)
		return nil, err
	}

	res, err := api.HTTPClient.Do(req)
	if err != nil {
		data[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(unexpectedAPIErr, err, data)
		return nil, err
	}

	data[logKeys.RespStatusCode] = res.StatusCode
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err := errors.New(getDimensionsErr)
		log.ErrorC(getDimensionsErr, err, data)
		return nil, err
	}

	body, err := api.ResponseBodyReader.Read(res.Body)
	if err != nil {
		log.ErrorC(readRespBodyErr, err, data)
		return nil, err
	}

	var dimensionsResult model.DimensionNodeResults
	err = json.Unmarshal(body, &dimensionsResult)

	if err != nil {
		data[logKeys.ErrorDetails] = err.Error()
		log.Debug(unmarshallingErr, data)
		return nil, err
	}

	log.Debug(getDimensionsSuccess, data)
	return dimensionsResult.Items, nil
}

// PutDimensionNodeID make a HTTP put request to update the node_id of the specified dimension.
func (api DatasetAPI) PutDimensionNodeID(instanceID string, d *model.Dimension) error {
	if len(api.DatasetAPIHost) == 0 {
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
	if len(d.DimensionID) == 0 {
		return errors.New(dimensionIDReqErr)
	}

	logData := make(map[string]interface{}, 0)
	logData[logKeys.InstanceID] = instanceID
	logData[logKeys.DimensionsKey] = d.DimensionID
	logData[logKeys.NodeID] = d.NodeID

	url := fmt.Sprintf(putDimensionNodeIDURI, api.DatasetAPIHost, instanceID, d.DimensionID, url.PathEscape(d.Value), d.NodeID)
	logData[logKeys.URL] = url

	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		logData[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(createPutNodeIDReqErr, err, logData)
		return err
	}
	req.Header.Set(authTokenHeader, api.DatasetAPIAuthToken)
	resp, err := api.HTTPClient.Do(req)
	if err != nil {
		logData[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(putDimNodeIDReqErr, err, logData)
		return err
	}

	defer resp.Body.Close()
	logData[logKeys.RespStatusCode] = resp.StatusCode

	if resp.StatusCode != 200 {

		switch resp.StatusCode {
		case 401:
			err = errors.New(unauthorisedResponse)
			log.ErrorC(unauthorisedResponse, err, logData)
			return err
		case 403:
			err = errors.New(forbiddenResponse)
			log.ErrorC(forbiddenResponse, err, logData)
			return err
		default:
			err = errors.New(putDimNodeIDErr)
			log.ErrorC(putDimNodeIDErr, err, logData)
			return err
		}
	}

	return nil
}
