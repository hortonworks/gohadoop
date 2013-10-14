package yarn_client

import (
	"github.com/hortonworks/gohadoop/hadoop_yarn"
	yarn_conf "github.com/hortonworks/gohadoop/hadoop_yarn/conf"
	"log"
	"sync"
)

type AMRMClient struct {
	applicationAttemptId *hadoop_yarn.ApplicationAttemptIdProto
	client               hadoop_yarn.ApplicationMasterProtocolService
	responseId           int32
}

type resource_to_request struct {
	capability    *hadoop_yarn.ResourceProto
	numContainers int32
}

var resourceRequests = struct {
	sync.RWMutex
	requests map[int32]map[string]*resource_to_request
}{requests: make(map[int32]map[string]*resource_to_request)}

func CreateAMRMClient(conf yarn_conf.YarnConfiguration, applicationAttemptId *hadoop_yarn.ApplicationAttemptIdProto) (*AMRMClient, error) {
	c, err := hadoop_yarn.DialApplicationMasterProtocolService(conf)
	return &AMRMClient{applicationAttemptId: applicationAttemptId, client: c}, err
}

func (c *AMRMClient) RegisterApplicationMaster(host string, port int32, url string) error {
	request := hadoop_yarn.RegisterApplicationMasterRequestProto{Host: &host, RpcPort: &port, TrackingUrl: &url, ApplicationAttemptId: c.applicationAttemptId}
	response := hadoop_yarn.RegisterApplicationMasterResponseProto{}
	return c.client.RegisterApplicationMaster(&request, &response)
}

func (c *AMRMClient) FinishApplicationMaster(finalStatus *hadoop_yarn.FinalApplicationStatusProto, message string, url string) error {
	request := hadoop_yarn.FinishApplicationMasterRequestProto{FinalApplicationStatus: finalStatus, Diagnostics: &message, TrackingUrl: &url, ApplicationAttemptId: c.applicationAttemptId}
	response := hadoop_yarn.FinishApplicationMasterResponseProto{}
	return c.client.FinishApplicationMaster(&request, &response)
}

func (c *AMRMClient) AddRequest(priority int32, resourceName string, capability *hadoop_yarn.ResourceProto, numContainers int32) error {
	resourceRequests.Lock()
	existingResourceRequests, exists := resourceRequests.requests[priority]
	if !exists {
		existingResourceRequests = make(map[string]*resource_to_request)
		resourceRequests.requests[priority] = existingResourceRequests
	}
	request, exists := existingResourceRequests[resourceName]
	if !exists {
		request = &resource_to_request{capability: capability, numContainers: numContainers}
		existingResourceRequests[resourceName] = request
	} else {
		request.numContainers += numContainers
	}
	resourceRequests.Unlock()

	return nil
}

func (c *AMRMClient) Allocate() (*hadoop_yarn.AllocateResponseProto, error) {
	// Increment responseId
	c.responseId++
	log.Println("ResponseId: ", c.responseId)

	asks := []*hadoop_yarn.ResourceRequestProto{}

	// Set up resource-requests
	resourceRequests.Lock()
	for priority, requests := range resourceRequests.requests {
		for host, request := range requests {
			log.Println("priority: ", priority)
			log.Println("host: ", host)
			log.Println("request: ", request)

			resourceRequest := hadoop_yarn.ResourceRequestProto{Priority: &hadoop_yarn.PriorityProto{Priority: &priority}, ResourceName: &host, Capability: request.capability, NumContainers: &request.numContainers}
			asks = append(asks, &resourceRequest)
		}
	}
	log.Println("AMRMClient.Allocate #asks: ", len(asks))

	// Clear
	resourceRequests.requests = make(map[int32]map[string]*resource_to_request)
	resourceRequests.Unlock()

	request := hadoop_yarn.AllocateRequestProto{ApplicationAttemptId: c.applicationAttemptId, Ask: asks, ResponseId: &c.responseId}
	response := hadoop_yarn.AllocateResponseProto{}
	err := c.client.Allocate(&request, &response)
	return &response, err
}
