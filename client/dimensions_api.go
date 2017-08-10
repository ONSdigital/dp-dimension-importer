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
	unmarshallingErr      = "Unexpected error while unmarshalling response"
	unexpectedAPIErr      = "Unexpected error returned when calling Import API"
	hostConfigMissingErr  = "DimensionsClient requires an API host to be configured"
	marshalDimensionErr   = "Unexpected error while marshalling dimenison"
	instanceIDRequiredErr = "instanceID is required but is empty"

	getDimensionsURIFMT  = "%s/instances/%s/dimensions"
	getDimensionsSuccess = "Import-API Get Dimensions success"
	getDimensionsErr     = "Get dimensions returned error status"

	createPutNodeIDReqErr  = "Unexpecter error creating request struct"
	putDimensionNodeIDURI  = "%s/instances/%s/dimensions/%s/options/%s/node_id/%s"
	putDimNodeIDSuccessLog = "Import-API PUT dimension node_id success"
	putDimNodeIDReqErr     = "Error sending set Dimension node_id request"
	putDimNodeIDErr        = "Set Dimension node_id returned error status"
	dimensionNilErr        = "Dimension is required but was nil"
	dimensionIDReqErr      = "DimensionID is required but was empty"
	unauthorisedResponse   = "Import API returned Unauthorized response status"
	forbiddenResponse      = "Import API returned Forbidden response status"
	authTokenHeader        = "Internal-Token"
	readRespBodyErr        = "Unexpected error while attempting to read response body"
	newReqErr              = "Unexpected error while attempting to create new http.Request"
)

// ResponseBodyReader defines a http response body reader.
type ResponseBodyReader interface {
	Read(r io.Reader) ([]byte, error)
}

// HTTPClient interface for making HTTP requests.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// ImportAPI provides methods for getting dimensions for a given instanceID and updating the node_id of a specific dimension.
type ImportAPI struct {
	ImportHost         string
	AuthToken          string
	ResponseBodyReader ResponseBodyReader
	HTTPClient         HTTPClient
}

// GetDimensions perform a HTTP GET request to the dp-import-api to retrieve the dataset dimenions for the specified instanceID
func (api ImportAPI) GetDimensions(instanceID string) ([]*model.Dimension, error) {
	if len(api.ImportHost) == 0 {
		err := errors.New(hostConfigMissingErr)
		log.ErrorC(hostConfigMissingErr, err, nil)
		return nil, err
	}
	if len(instanceID) == 0 {
		return nil, errors.New(instanceIDRequiredErr)
	}

	url := fmt.Sprintf(getDimensionsURIFMT, api.ImportHost, instanceID)
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

	var dims []*model.Dimension
	err = json.Unmarshal(body, &dims)

	if err != nil {
		data[logKeys.ErrorDetails] = err.Error()
		log.Debug(unmarshallingErr, data)
		return nil, err
	}

	log.Debug(getDimensionsSuccess, data)
	return dims, nil
}

// PutDimensionNodeID make a HTTP put request to update the node_id of the specified dimension.
func (api ImportAPI) PutDimensionNodeID(instanceID string, d *model.Dimension) error {
	if len(api.ImportHost) == 0 {
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

	url := fmt.Sprintf(putDimensionNodeIDURI, api.ImportHost, instanceID, d.DimensionID, url.PathEscape(d.Value), d.NodeID)
	logData[logKeys.URL] = url

	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		logData[logKeys.ErrorDetails] = err.Error()
		log.ErrorC(createPutNodeIDReqErr, err, logData)
		return err
	}
	req.Header.Set(authTokenHeader, api.AuthToken)
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
