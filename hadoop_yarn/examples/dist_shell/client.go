package main

import (
	hadoop_conf "github.com/hortonworks/gohadoop/hadoop_common/conf"
	hadoop_yarn "github.com/hortonworks/gohadoop/hadoop_yarn"
	yarn_conf "github.com/hortonworks/gohadoop/hadoop_yarn/conf"
	"github.com/hortonworks/gohadoop/hadoop_yarn/yarn_client"
	"log"
	"os"
	"time"
)

func main() {
	// Create YarnConfiguration
	conf, _ := yarn_conf.NewYarnConfiguration()

	// Create YarnClient
	yarnClient, _ := yarn_client.CreateYarnClient(conf)

	// Create new application to get ApplicationSubmissionContext
	_, asc, _ := yarnClient.CreateNewApplication()

	// Setup ContainerLaunchContext for the application
	clc := hadoop_yarn.ContainerLaunchContextProto{}
	clc.Command = []string{"go run /Users/acmurthy/dev/go/src/github.com/hortonworks/gohadoop/hadoop_yarn/examples/dist_shell/applicationmaster.go 1>/tmp/stdout 2>/tmp/stderr"}
	clc.Environment = getEnv()

	// Resource for ApplicationMaster
	var memory int32 = 1024
	resource := hadoop_yarn.ResourceProto{Memory: &memory}

	// Some useful information
	queue := "default"
	appName := "simple-go-yarn-app"
	appType := "GO_HADOOP"

	// Setup ApplicationSubmissionContext for the application
	asc.AmContainerSpec = &clc
	asc.Resource = &resource
	asc.ApplicationName = &appName
	asc.Queue = &queue
	asc.ApplicationType = &appType

	// Submit!
	err := yarnClient.SubmitApplication(asc)
	if err != nil {
		log.Fatal("yarnClient.SubmitApplication ", err)
	}
	log.Println("Successfully submitted application: ", asc.ApplicationId)

	appReport, err := yarnClient.GetApplicationReport(asc.ApplicationId)
	if err != nil {
		log.Fatal("yarnClient.GetApplicationReport ", err)
	}
	appState := appReport.GetYarnApplicationState()
	for appState != hadoop_yarn.YarnApplicationStateProto_FINISHED && appState != hadoop_yarn.YarnApplicationStateProto_KILLED && appState != hadoop_yarn.YarnApplicationStateProto_FAILED {
		log.Println("Application in state ", appState)
		time.Sleep(1 * time.Second)
		appReport, err = yarnClient.GetApplicationReport(asc.ApplicationId)
		appState = appReport.GetYarnApplicationState()
	}

	log.Println("Application finished in state: ", appState)
}

func getEnv() []*hadoop_yarn.StringStringMapProto {
	key := hadoop_conf.HADOOP_CONF_DIR
	value := os.Getenv(hadoop_conf.HADOOP_CONF_DIR)
	return []*hadoop_yarn.StringStringMapProto{&hadoop_yarn.StringStringMapProto{Key: &key, Value: &value}}
}
