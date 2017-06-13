package client

import (
	"fmt"
	"os"

	"github.com/denverdino/aliyungo/dns"
	"github.com/denverdino/aliyungo/ecs"
	"github.com/denverdino/aliyungo/slb"
	"github.com/golang/glog"
)

type Config struct {
	Global struct {
		AccessKeyID     string `json:"accessKeyID"`
		AccessKeySecret string `json:"accessKeySecret"`
		RegionID        string `json:"regionID"`
		ZoneID          string `json:"zoneID"`
	}
}

type AliyunClient struct {
	EcsClient *ecs.Client
	SlbClient *slb.Client
	DnsClient *dns.Client
	RegionID  string
	ZoneID    string
}

func NewAliyunClient(config Config) (*AliyunClient, error) {
	if config.Global.AccessKeyID == "" || config.Global.AccessKeySecret == "" ||
		config.Global.RegionID == "" || config.Global.ZoneID == "" {
		return nil, fmt.Errorf("Invalid fields in config file")
	}

	return &AliyunClient{
		EcsClient: ecs.NewClient(config.Global.AccessKeyID, config.Global.AccessKeySecret),
		SlbClient: slb.NewClient(config.Global.AccessKeyID, config.Global.AccessKeySecret),
		RegionID:  config.Global.RegionID,
		ZoneID:    config.Global.ZoneID,
	}, nil
}

func NewAliyunClientFormEnv() *AliyunClient {
	accessKeyID := os.Getenv("ALIYUN_ACCESS_KEY_ID")
	if accessKeyID == "" {
		glog.Fatalf("missing ALIYUN_ACCESS_KEY_ID environment variable")
	}
	accessKeySecret := os.Getenv("ALIYUN_ACCESS_KEY_SECRET")
	if accessKeySecret == "" {
		glog.Fatalf("missing ALIYUN_ACCESS_KEY_SECRET environment variable")
	}
	regionID := os.Getenv("ALIYUN_REGION_ID")
	if regionID == "" {
		glog.Fatalf("missing ALIYUN_REGION_ID environment variable")
	}
	zoneID := os.Getenv("ALIYUN_ZONE_ID")
	if zoneID == "" {
		glog.Fatalf("missing ALIYUN_ZONE_ID environment variable")
	}

	return &AliyunClient{
		EcsClient: ecs.NewClient(accessKeyID, accessKeySecret),
		SlbClient: slb.NewClient(accessKeyID, accessKeySecret),
		DnsClient: dns.NewClient(accessKeyID, accessKeySecret),
		RegionID:  regionID,
		ZoneID:    zoneID,
	}
}
