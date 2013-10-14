package main

import (
	"github.com/hortonworks/gohadoop/hadoop_yarn"
	yarn_conf "github.com/hortonworks/gohadoop/hadoop_yarn/conf"
	"log"
)

func main() {

	conf, _ := yarn_conf.NewYarnConfiguration()

	// Use the ApplicationClientProtocolService protocol
	appClient, err := hadoop_yarn.DialApplicationClientProtocolService(conf)
	if err != nil {
		log.Fatal("hadoop_yarn.DialApplicationClientProtocolService not found")
	}

	// ApplicationClientProtocol.getApplications
	applicationStates := []hadoop_yarn.YarnApplicationStateProto{hadoop_yarn.YarnApplicationStateProto_ACCEPTED, hadoop_yarn.YarnApplicationStateProto_RUNNING, hadoop_yarn.YarnApplicationStateProto_SUBMITTED}
	getAppsReqProto := hadoop_yarn.GetApplicationsRequestProto{ApplicationStates: applicationStates}
	getAppsResProto := hadoop_yarn.GetApplicationsResponseProto{}
	err = appClient.GetApplications(&getAppsReqProto, &getAppsResProto)
	if err != nil {
		log.Fatal("appClient.GetApplications failed", err)
	}
	log.Println("appClient.GetApplications response: ", getAppsResProto)

	// ApplicationClientProtocol.getNewApplication
	getNewAppReqProto := hadoop_yarn.GetNewApplicationRequestProto{}
	getNewAppResProto := hadoop_yarn.GetNewApplicationResponseProto{}

	err = appClient.GetNewApplication(&getNewAppReqProto, &getNewAppResProto)
	if err != nil {
		log.Fatal("appClient.GetNewApplication failed", err)
	}
	log.Println("appClient.GetNewApplication response: ", getNewAppResProto)
}
