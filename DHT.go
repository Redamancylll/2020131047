package main

import (
	"crypto/md5"
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"time"
)

const (
	BucketSize = 16
)

type Peer struct {
	id      string
	buckets []Bucket
	dht     DHT
}

type Bucket struct {
	nodes []*Peer
}

type DHT struct {
	buckets []Bucket
}

func NewDHT() *DHT {
	return &DHT{buckets: []Bucket{}}
}

func (d *DHT) setValue(key string, value string) bool {
	hash := d.hashValue(key)

	if key != hash {
		return false
	}

	ownNode := d.findOwnNode()
	if ownNode != nil && ownNode.dht.containsKey(key) {
		return true
	}

	if ownNode != nil {
		ownNode.dht.setValue(key, value)
	}

	nearestNodes := d.findNearestNodes(key)
	for _, node := range nearestNodes {
		node.dht.setValue(key, value)
	}

	return true
}

func (d *DHT) getValue(key string) string {
	ownNode := d.findOwnNode()
	if ownNode != nil && ownNode.dht.containsKey(key) {
		return ownNode.dht.getValue(key)
	}

	nearestNodes := d.findNearestNodes(key)
	for _, node := range nearestNodes {
		value := node.dht.getValue(key)
		if value != "" {
			return value
		}
	}

	return ""
}

func (d *DHT) containsKey(key string) bool {
	for _, bucket := range d.buckets {
		for _, peer := range bucket.nodes {
			if peer.id == key {
				return true
			}
		}
	}

	return false
}

func (d *DHT) findOwnNode() *Peer {
	if len(d.buckets) == 0 || len(d.buckets[0].nodes) == 0 {
		return nil
	}
	return d.buckets[0].nodes[0] // Assuming the first node in the first bucket is the own node
}

func (d *DHT) findNearestNodes(key string) []*Peer {
	ownNode := d.findOwnNode()
	if ownNode == nil {
		return []*Peer{}
	}

	distanceToKey := d.calculateDistance(ownNode.id, key)

	allNodes := make([]*Peer, 0)
	for _, bucket := range d.buckets {
		allNodes = append(allNodes, bucket.nodes...)
	}

	sortedNodes := make([]*Peer, len(allNodes))
	copy(sortedNodes, allNodes)
	d.sortPeerSlice(sortedNodes, func(p1, p2 *Peer) bool {
		distanceA := d.calculateDistance(p1.id, key)
		distanceB := d.calculateDistance(p2.id, key)
		if distanceA != distanceB {
			return distanceA < distanceB
		}
		return p1.id < p2.id
	})

	nearestNodes := make([]*Peer, 0)
	for _, node := range sortedNodes {
		distance := d.calculateDistance(node.id, key)
		if distance <= distanceToKey {
			nearestNodes = append(nearestNodes, node)
		}
	}

	return nearestNodes[:min(2, len(nearestNodes))]
}

func (d *DHT) calculateDistance(id1 string, id2 string) int {
	num1 := new(big.Int)
	num1.SetString(id1, 16)
	num2 := new(big.Int)
	num2.SetString(id2, 16)
	distance := new(big.Int).Xor(num1, num2)
	return int(distance.Int64())
}

func (d *DHT) hashValue(value string) string {
	hash := md5.Sum([]byte(value))
	return fmt.Sprintf("%x", hash)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	nodes := make([]*Peer, 0)
	// Initialize buckets for the peer
	// Initialize 100 nodes
	for i := 0; i < 100; i++ {
		peer := &Peer{
			id:      fmt.Sprintf("%d", i),
			buckets: make([]Bucket, 0),
			dht:     *NewDHT(),
		}

		for j := 0; j < 160; j += BucketSize {
			bucket := Bucket{nodes: make([]*Peer, 0)}
			for k := 0; k < BucketSize; k++ {
				if i*160+j+k >= 100*160 {
					continue
				}
				neighbor := &Peer{
					id:      fmt.Sprintf("%d", i*160+j+k),
					buckets: make([]Bucket, 0),
					dht:     *NewDHT(),
				}
				bucket.nodes = append(bucket.nodes, neighbor)
			}
			peer.buckets = append(peer.buckets, bucket)
		}
		nodes = append(nodes, peer)
	}

	// Generate 200 random keys and values
	keys := make([]string, 0)
	for i := 0; i < 200; i++ {
		key := generateRandomString()
		value := generateRandomString()
		keys = append(keys, key)
		randomNodeIndex := rand.Intn(len(nodes))
		randomNode := nodes[randomNodeIndex]
		randomNode.dht.setValue(key, value)
	}

	// Select 100 random keys and perform getValue operation
	selectedKeys := selectRandomElements(keys, 100)
	for _, key := range selectedKeys {
		randomNodeIndex := rand.Intn(len(nodes))
		randomNode := nodes[randomNodeIndex]
		value := randomNode.dht.getValue(key)
		fmt.Printf("Key: %s, Value: %s\n", key, value)
	}
}

func generateRandomString() string {
	length := getRandomInt(5, 10)
	result := make([]byte, length)
	characters := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	for i := 0; i < length; i++ {
		randomIndex := rand.Intn(len(characters))
		result[i] = characters[randomIndex]
	}
	return string(result)
}

func getRandomInt(min int, max int) int {
	return rand.Intn(max-min+1) + min
}

func selectRandomElements(arr []string, count int) []string {
	shuffled := make([]string, len(arr))
	copy(shuffled, arr)
	i := len(arr)
	for i > 0 {
		randomIndex := rand.Intn(i)
		i--
		shuffled[i], shuffled[randomIndex] = shuffled[randomIndex], shuffled[i]
	}
	return shuffled[:min(count, len(shuffled))]
}

func (d *DHT) sortPeerSlice(nodes []*Peer, by func(p1, p2 *Peer) bool) {
	ps := &peerSorter{
		nodes: nodes,
		by:    by,
	}
	ps.sort()
}

type peerSorter struct {
	nodes []*Peer
	by    func(p1, p2 *Peer) bool
}

func (s *peerSorter) sort() {
	sort.SliceStable(s.nodes, func(i, j int) bool {
		return s.by(s.nodes[i], s.nodes[j])
	})
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
