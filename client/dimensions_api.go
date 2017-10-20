package client

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pkg/errors"

	"io"
	"net/url"

	"github.com/ONSdigital/dp-dimension-importer/logging"
	"github.com/ONSdigital/dp-dimension-importer/model"
	"github.com/ONSdigital/go-ns/log"
)

//go:generate moq -out ../mocks/dimensions_api_generated_mocks.go -pkg mocks . HTTPClient ResponseBodyReader

const (
	getInstanceURIFMT     = "%s/instances/%s"
	getDimensionsURIFMT   = "%s/instances/%s/dimensions"
	putDimensionNodeIDURI = "%s/instances/%s/dimensions/%s/options/%s/node_id/%s"
	authTokenHeader       = "Internal-Token"
)

var (
	logger             = logging.Logger{Prefix: "client.DatasetAPI"}
	errHostEmpty       = errors.New("validation error: api host is required but was empty")
	errInstanceIDEmpty = errors.New("validation error: instance id is required but is empty")
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

// GetInstance retrieve the specified instance from the Dataset API.
func (api DatasetAPI) GetInstance(instanceID string) (*model.Instance, error) {
	if len(api.DatasetAPIHost) == 0 {
		return nil, errHostEmpty
	}

	if len(instanceID) == 0 {
		return nil, errInstanceIDEmpty
	}

	url := fmt.Sprintf(getInstanceURIFMT, api.DatasetAPIHost, instanceID)

	resp, err := api.doRequest(http.MethodGet, url, http.StatusOK)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := api.ResponseBodyReader.Read(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "unexpected error while attempting to read response body")
	}

	var instance *model.Instance
	err = json.Unmarshal(body, &instance)
	if err != nil {
		return nil, errors.Wrap(err, "error while attempting to unmarshal reponse body into model.Instance")
	}

	logger.Info("GetInstance completed successfully", log.Data{"instance": instance})
	return instance, nil
}

// GetDimensions retrieve the dimensions of the specified instance from the Dataset API
func (api DatasetAPI) GetDimensions(instanceID string) ([]*model.Dimension, error) {
	if len(api.DatasetAPIHost) == 0 {
		return nil, errHostEmpty
	}
	if len(instanceID) == 0 {
		return nil, errInstanceIDEmpty
	}

	url := fmt.Sprintf(getDimensionsURIFMT, api.DatasetAPIHost, instanceID)

	resp, err := api.doRequest(http.MethodGet, url, http.StatusOK)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := api.ResponseBodyReader.Read(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "unexpected error while attempting to read response body")
	}

	var dimensionsResult model.DimensionNodeResults
	err = json.Unmarshal(body, &dimensionsResult)
	if err != nil {
		return nil, errors.Wrap(err, "errpr while attempting to unmarshal response body into model.DimensionNodeResults")
	}

	logger.Info("GetDimensions completed successfully", nil)
	return dimensionsResult.Items, nil
}

// PutDimensionNodeID make a HTTP put request to update the node_id of the specified dimension.
func (api DatasetAPI) PutDimensionNodeID(instanceID string, d *model.Dimension) error {
	if len(api.DatasetAPIHost) == 0 {
		return errHostEmpty
	}
	if len(instanceID) == 0 {
		return errInstanceIDEmpty
	}
	if d == nil {
		return errors.New("dimension is required but is nil")
	}
	if len(d.DimensionID) == 0 {
		return errors.New("dimension.id is required but is empty")
	}

	putNodeIDURL := fmt.Sprintf(putDimensionNodeIDURI, api.DatasetAPIHost, instanceID, d.DimensionID, url.PathEscape(d.Option), d.NodeID)

	_, err := api.doRequest(http.MethodPut, putNodeIDURL, http.StatusOK)
	return err
}

func (api DatasetAPI) doRequest(method string, url string, expectedStatus int) (*http.Response, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("unexpected error while attempting to create new http request: method: %s, url: %s", method, url))
	}

	req.Header.Set(authTokenHeader, api.DatasetAPIAuthToken)

	logger.Info("HTTPClient.Do sending HTTP Request", log.Data{"method": req.Method, "url": url})
	resp, err := api.HTTPClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("HTTPClient.Do returned an error when attempting to make request: method: %s, url: %s", method, url))
	}

	if resp.StatusCode != expectedStatus {
		return nil, errors.Errorf("incorrect status code: expected: %d, actual: %d, url: %s, method: %s", expectedStatus, resp.StatusCode, url, method)
	}

	logger.Info("HTTPClient.Do received a valid response", log.Data{"url": url, "method": method})
	return resp, nil
}
