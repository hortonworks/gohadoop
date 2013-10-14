package main

import (
	hadoop_conf "github.com/hortonworks/gohadoop/hadoop_common/conf"
	yarn_conf "github.com/hortonworks/gohadoop/hadoop_yarn/conf"
	"log"
)

func main() {

	conf, _ := hadoop_conf.NewConfigurationResources([]string{yarn_conf.YARN_DEFAULT, yarn_conf.YARN_SITE})

	fsName, _ := conf.Get("fs.default.name", "XXX")
	log.Println("fs.default.name = ", fsName)

	cores, _ := conf.GetInt("yarn.nodemanager.resource.cpu-cores", -1)
	log.Println("yarn.nodemanager.resource.cpu-cores = ", cores)
}
