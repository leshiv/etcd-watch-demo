package main

import (
	"github.com/coreos/go-etcd/etcd"
	"log"
	"sync"
	"time"
)

type WrappedEtcdClient struct {
	etcdClient *etcd.Client
}

func (w *WrappedEtcdClient) WatchEtcd(dir string, ch chan *etcd.Response, stop chan bool, handle func(*etcd.Response)) {

	watcher := func() {
		for {
			watchCh := make(chan *etcd.Response, 10)
			var err error
			go func() {
				for {
					select {
					case rs := <-watchCh:
						if rs != nil {
							println(rs.Node.Value)
							ch <- rs
						}else {
							//receives nil when etcd is not reachable
							time.Sleep(5 * time.Second)
						}
					case <-stop:
						break
					default:
						time.Sleep(time.Second)
					}
				}
			}()

			_, err = w.etcdClient.Watch(dir, 0, true, watchCh, stop)

			if err != nil {
				log.Printf("Error watching %s, %s, retry in 10s.", dir, err.Error())
				time.Sleep(10 * time.Second)
			}
		}
	}

	receiver := func() {
		for {
			res := <-ch
			if res != nil {
				handle(res)
			}
		}
		stop <- true
	}

	log.Printf("Watching %s.", dir)

	go watcher()
	go receiver()
}

type Registry struct {
	sync.RWMutex
	data map[string]string
}

func (r *Registry) Register(node *etcd.Node) {
	r.Lock()
	log.Printf("Register (%s,%s).", node.Key, node.Value)
	r.data[node.Key] = node.Value
	r.Unlock()
}

func (r *Registry) Unregister(node *etcd.Node) {
	r.Lock()
	if _, found := r.data[node.Key]; found {
		log.Printf("Unregister (%s,%s).", node.Key, node.Value)
		delete(r.data, node.Key)
	}
	r.Unlock()
}

func main() {
	wec := &WrappedEtcdClient{
		etcdClient: etcd.NewClient([]string{"http://127.0.0.1:4001"}),
	}
	reg := &Registry{
		data: make(map[string]string),
	}

	dir := "/foo"

	attempts := 3
	for attempts > 0 {
		resp, err := wec.etcdClient.Get(dir, false, true)
		if err == nil {
			reg.Register(resp.Node)
			break
		} else {
			log.Printf("Error getting %s, %s, retry in 1s.", dir, err.Error())
			attempts--
			time.Sleep(1000 * time.Millisecond)
		}
	}

	if attempts == 0 {
		log.Fatal("Exiting")
	}

	ch := make(chan *etcd.Response, 10)
	stop := make(chan bool, 1)

	handle := func(resp *etcd.Response) {
		setOp := []string{"create", "set", "update", "compareAndSwap"}
		deleteOp := []string{"delete", "expire", "compareAndDelete"}

		for _, op := range setOp {
			if op == resp.Action {
				if resp.PrevNode != nil {
					reg.Unregister(resp.PrevNode)
				}
				reg.Register(resp.Node)
			}
		}

		for _, op := range deleteOp {
			if op == resp.Action {
				reg.Unregister(resp.PrevNode)
			}
		}
	}

	wec.WatchEtcd(dir, ch, stop, handle)

	<-stop
	log.Println("Stopped.")
}
