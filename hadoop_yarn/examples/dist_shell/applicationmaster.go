package main

import (
	"github.com/hortonworks/gohadoop/hadoop_yarn"
	"github.com/hortonworks/gohadoop/hadoop_yarn/conf"
	"github.com/hortonworks/gohadoop/hadoop_yarn/yarn_client"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func parseAppAttemptId() (*hadoop_yarn.ApplicationAttemptIdProto, error) {
	appAttemptIdString := os.Getenv("APPLICATION_ATTEMPT_ID")
	log.Println("APPLICATION_ATTEMPT_ID: ", appAttemptIdString)

	appAttemptIdStrComponents := strings.Split(appAttemptIdString, "_")

	clusterTimeStamp, err := strconv.ParseInt(appAttemptIdStrComponents[1], 10, 64)
	if err != nil {
		return nil, err
	}

	i, err := strconv.Atoi(appAttemptIdStrComponents[2])
	if err != nil {
		return nil, err
	}
	var applicationId int32 = int32(i)

	i, err = strconv.Atoi(appAttemptIdStrComponents[3])
	if err != nil {
		return nil, err
	}
	var attemptId int32 = int32(i)

	return &hadoop_yarn.ApplicationAttemptIdProto{ApplicationId: &hadoop_yarn.ApplicationIdProto{ClusterTimestamp: &clusterTimeStamp, Id: &applicationId}, AttemptId: &attemptId}, nil
}

func main() {
	var err error

	// Get applicationAttemptId from environment
	applicationAttemptId, err := parseAppAttemptId()
	if err != nil {
		log.Fatal("parseAppAttemptId: ", err)
	}

	// Create YarnConfiguration
	conf, _ := conf.NewYarnConfiguration()

	// Create AMRMClient
	rmClient, _ := yarn_client.CreateAMRMClient(conf, applicationAttemptId)
	log.Println("Created RM client: ", rmClient)

	// Register with ResourceManager
	log.Println("About to register application master.")
	err = rmClient.RegisterApplicationMaster("", -1, "")
	if err != nil {
		log.Fatal("rmClient.RegisterApplicationMaster ", err)
	}
	log.Println("Successfully registered application master.")

	// Add resource requests
	const numContainers = int32(1)
	memory := int32(128)
	resource := hadoop_yarn.ResourceProto{Memory: &memory}
	rmClient.AddRequest(1, "*", &resource, numContainers)

	// Now call ResourceManager.allocate
	allocateResponse, err := rmClient.Allocate()
	if err == nil {
		log.Println("allocateResponse: ", *allocateResponse)
	}
	log.Println("#containers allocated: ", len(allocateResponse.AllocatedContainers))

	numAllocatedContainers := int32(0)
	allocatedContainers := make([]*hadoop_yarn.ContainerProto, numContainers, numContainers)
	for numAllocatedContainers < numContainers {
		// Sleep for a while
		log.Println("Sleeping...")
		time.Sleep(3 * time.Second)
		log.Println("Sleeping... done!")

		// Try to get containers now...
		allocateResponse, err = rmClient.Allocate()
		if err == nil {
			log.Println("allocateResponse: ", *allocateResponse)
		}

		for _, container := range allocateResponse.AllocatedContainers {
			allocatedContainers[numAllocatedContainers] = container
			numAllocatedContainers++
		}

		log.Println("#containers allocated: ", len(allocateResponse.AllocatedContainers))
		log.Println("Total #containers allocated so far: ", numAllocatedContainers)
	}
	log.Println("Final #containers allocated: ", numAllocatedContainers)

	// Now launch containers
	containerLaunchContext := hadoop_yarn.ContainerLaunchContextProto{Command: []string{"/bin/date"}}
	log.Println("containerLaunchContext: ", containerLaunchContext)
	for _, container := range allocatedContainers {
		log.Println("Launching container: ", *container, " ", container.NodeId.Host, ":", container.NodeId.Port)
		nmClient, err := yarn_client.CreateAMNMClient(*container.NodeId.Host, int(*container.NodeId.Port))
		if err != nil {
			log.Fatal("hadoop_yarn.DialContainerManagementProtocolService: ", err)
		}
		log.Println("Successfully created nmClient: ", nmClient)
		err = nmClient.StartContainer(container, &containerLaunchContext)
		if err != nil {
			log.Fatal("nmClient.StartContainer: ", err)
		}
	}

	// Wait for Containers to complete
	numCompletedContainers := int32(0)
	for numCompletedContainers < numContainers {
		// Sleep for a while
		log.Println("Sleeping...")
		time.Sleep(3 * time.Second)
		log.Println("Sleeping... done!")

		allocateResponse, err = rmClient.Allocate()
		if err == nil {
			log.Println("allocateResponse: ", *allocateResponse)
		}

		for _, containerStatus := range allocateResponse.CompletedContainerStatuses {
			log.Println("Completed container: ", *containerStatus, " exit-code: ", *containerStatus.ExitStatus)
			numCompletedContainers++
		}
	}
	log.Println("Containers complete: ", numCompletedContainers)

	// Unregister with ResourceManager
	log.Println("About to unregister application master.")
	finalStatus := hadoop_yarn.FinalApplicationStatusProto_APP_SUCCEEDED
	err = rmClient.FinishApplicationMaster(&finalStatus, "done", "")
	if err != nil {
		log.Fatal("rmClient.RegisterApplicationMaster ", err)
	}
	log.Println("Successfully unregistered application master.")
}
