package yarn_client

import (
	"github.com/xiocode/gohadoop/hadoop_yarn"
)

type AMNMClient struct {
	client hadoop_yarn.ContainerManagementProtocolService
}

func CreateAMNMClient(host string, port int) (*AMNMClient, error) {
	c, err := hadoop_yarn.DialContainerManagementProtocolService(host, port)
	return &AMNMClient{client: c}, err
}

func (c *AMNMClient) StartContainer(container *hadoop_yarn.ContainerProto, containerLaunchContext *hadoop_yarn.ContainerLaunchContextProto) error {
	request := hadoop_yarn.StartContainersRequestProto{StartContainerRequest: []*hadoop_yarn.StartContainerRequestProto{&hadoop_yarn.StartContainerRequestProto{ContainerLaunchContext: containerLaunchContext, Container: container}}}
	response := hadoop_yarn.StartContainersResponseProto{}
	return c.client.StartContainers(&request, &response)
}
