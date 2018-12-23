package leaderElection

import (
	"context"
	"errors"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"injester_test/settings"
	"log"
	"math"
	"net"
	"strings"
	"sync"
	"time"
)

var rootPath = "e3w_test/LeaderElection/"
var path = rootPath + "leader"
var Election leaderElection

type leaderElection struct {
	isleader  bool
	leader    string
	leaderIP  string
	ip        string
	worker    string
	client    *clientv3.Client
	kv        clientv3.KV
	closeCh   chan bool
	wg        sync.WaitGroup
	callbacks []func()
}

func NewElection() {
	le := leaderElection{}
	dialTimeout := 2 * time.Second
	client, err := clientv3.New(clientv3.Config{
		DialTimeout: dialTimeout,
		Endpoints: settings.Settings.Ectd,
	})
	if err != nil {
		panic(err)
	}
	le.worker = settings.Settings.Hostname
	le.ip, _ = GetExternalIP()
	fmt.Println("Worker is:" + le.worker)
	//settings.SettingStore{}
	le.client = client
	le.wg = sync.WaitGroup{}
	le.kv = clientv3.NewKV(le.client)
	Election = le
}

func (le *leaderElection) Register(cb func()) {
	le.callbacks = append(le.callbacks, cb)
}

func (le *leaderElection) Call() {
	fmt.Println("Leader is:" + le.leader)
	fmt.Println("Leader ip" + le.leaderIP)
	for _, v := range le.callbacks {
		v()
	}
}

func (le *leaderElection) Start() {
	fmt.Println("Leader Election is starting")
	le.putValue(le.ip)
	le.check()
	fmt.Println("Leader is:" + le.leader)
	fmt.Println("Leader ip" + le.leaderIP)
	le.closeCh = make(chan bool)
	le.wg.Add(1)
	go le.refresh()
	le.wg.Add(1)
	go le.observe()
	fmt.Println("Leader Election has started")
}

func (le *leaderElection) Stop() {
	fmt.Println("Leader Election is stopping, closing connection")
	le.closeCh <- true
	le.closeCh <- true
	close(le.closeCh)
	le.wg.Wait()

	le.cleanup()
	fmt.Println("Leader Election has stopped")
	le.client.Close()
}

func (le *leaderElection) IsLeader() bool {
	return le.isleader
}

func (le *leaderElection) LeaderIP() string {
	resps, err := le.client.Get(context.Background(), rootPath, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		fmt.Println(err)
		return ""
	}
	for _, v := range resps.Kvs {
		if strings.HasSuffix(string(v.Key), le.leader) {
			le.leaderIP = string(v.Value)
		}
	}

	return le.leaderIP
}

func (le *leaderElection) LeaderURI() string {
	return fmt.Sprintf("http://%s:8000", le.LeaderIP())
}

func (le *leaderElection) WhoIsLeader() string {
	resps, err := le.client.Get(context.Background(), rootPath, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		log.Fatal(err)
	} else {
		lastnum := int64(math.MaxInt64)
		aworker := ""
		ip := ""
		for _, v := range resps.Kvs {
			if v.ModRevision < lastnum {
				lastnum = v.ModRevision
				aworker = string(v.Key)
				ip = string(v.Value)
			}
		}

		if aworker != "" {
			if strings.HasSuffix(aworker, le.worker) {
				le.isleader = true
				le.putValue(le.ip)
				le.leader = le.worker
				fmt.Println("I AM Now LEADER")

				le.Call()
			} else {
				le.isleader = false
				le.leader = aworker
				le.putLeder(ip)
				le.leaderIP = ip
			}

		} else {
			le.isleader = false
		}
	}
	return le.leader
}

func (le *leaderElection) putLeder(ip string) {
	lease, err := le.client.Grant(context.Background(), 60)
	if err != nil {
		log.Fatal(err)
	}
	le.kv.Put(context.Background(), path, le.worker, clientv3.WithLease(lease.ID))
}

func (le *leaderElection) putValue(msg string) {
	lease, err := le.client.Grant(context.Background(), 60)
	if err != nil {
		log.Fatal(err)
	}
	_, err = le.kv.Put(context.Background(), rootPath+le.worker, msg, clientv3.WithLease(lease.ID))
	if le.isleader {
		le.kv.Put(context.Background(), path, le.worker, clientv3.WithLease(lease.ID))
	}

	if err != nil {
		log.Fatal(err)
	}
}

func (le *leaderElection) check() {
	resps, err := le.client.Get(context.Background(), path)
	if err != nil {
		log.Fatal(err)
	}
	if len(resps.Kvs) == 0 {
		le.WhoIsLeader()
		return
	}
	if string(resps.Kvs[0].Value) != le.worker {
		le.leader = string(resps.Kvs[0].Value)
		le.LeaderIP()
		le.isleader = false
	}
}

func (le *leaderElection) refresh() {
	fmt.Println("Starting Refresh")
	defer le.wg.Done()
	le.putValue(le.ip)
	le.check()
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			le.putValue(le.ip)
			le.check()
		case <-le.closeCh:
			fmt.Println("CLOSING REFRESH")
			return
		}
	}
}

//relinquish leadership and delete worker node in ectd
func (le *leaderElection) cleanup() {
	le.kv.Delete(context.Background(), rootPath+le.worker)
	if le.isleader {
		le.kv.Delete(context.Background(), path)
	}
}

func (le *leaderElection) observe() {
	defer le.wg.Done()
	watchChan := le.client.Watch(context.Background(), path, clientv3.WithPrefix())
	for {
		select {
		case watchResp := <-watchChan:
			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypeDelete {
					if strings.HasSuffix(string(event.Kv.Key), path) {
						le.WhoIsLeader()
					}
				}
			}
		case <-le.closeCh:
			fmt.Println("CLOSING OBSERVE")
			return
		}
	}
}

func GetExternalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("are you connected to the network?")
}
