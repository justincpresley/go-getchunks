package main

import (
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/apex/log"
	enc "github.com/zjkmxy/go-ndn/pkg/encoding"
	eng "github.com/zjkmxy/go-ndn/pkg/engine/basic"
	ndn "github.com/zjkmxy/go-ndn/pkg/ndn"
	sec "github.com/zjkmxy/go-ndn/pkg/security"
	"github.com/zjkmxy/go-ndn/pkg/utils"
)

type Result int

const (
	Successful  Result = 0
	Unsucessful Result = 1
)

type entry struct {
	name    enc.Name
	retries uint
}
type ChunksClient struct {
	app        *eng.Engine
	name       enc.Name
	nextSeg    uint64
	numSegs    uint64
	numActive  *int64
	numDone    *int64
	numRetries uint
	window     int64
	intCfg     *ndn.InterestConfig
	retryChan  chan *entry
	result     chan Result
	failed     atomic.Bool
}

func NewChunksClient(app *eng.Engine, name enc.Name, numSegs uint, wSize int64) *ChunksClient {
	return &ChunksClient{
		app:        app,
		name:       name,
		numSegs:    uint64(numSegs),
		numActive:  new(int64),
		numDone:    new(int64),
		numRetries: 3,
		window:     wSize,
		intCfg: &ndn.InterestConfig{
			MustBeFresh: true,
			Lifetime:    utils.IdPtr(6 * time.Second),
		},
		retryChan: make(chan *entry, 5),
		result:    make(chan Result, 1),
	}
}
func (c *ChunksClient) GetChunks() {
	var temp *entry
	for {
		if c.failed.Load() {
			c.result <- Unsucessful
			return
		}
		for atomic.LoadInt64(c.numActive) < int64(c.window) && c.nextSeg < c.numSegs {
			select {
			case temp = <-c.retryChan:
			default:
				temp = &entry{
					name:    append(c.name, enc.NewSegmentComponent(c.nextSeg)),
					retries: c.numRetries,
				}
				c.nextSeg++
			}
			if !c.getChunk(temp) {
				c.result <- Unsucessful
				return
			}
		}
		if atomic.LoadInt64(c.numDone) == int64(c.numSegs) {
			c.result <- Successful
			return
		}
	}
}
func (c *ChunksClient) getChunk(e *entry) bool {
	atomic.AddInt64(c.numActive, 1)
	wire, _, finalName, err := c.app.Spec().MakeInterest(e.name, c.intCfg, nil, nil)
	if err != nil {
		return false
	}
	err = c.app.Express(finalName, c.intCfg, wire,
		func(result ndn.InterestResult, data ndn.Data, rawData, sigCovered enc.Wire, nackReason uint64) {
			if result == ndn.InterestResultData {
				c.handleData(e.name, data)
				atomic.AddInt64(c.numDone, 1)
			} else if result == ndn.InterestResultTimeout && e.retries != 0 {
				e.retries--
				c.retryChan <- e
			} else {
				c.failed.Store(true)
			}
			atomic.AddInt64(c.numActive, -1)
		})
	if err != nil {
		return false
	}
	return true
}
func (c *ChunksClient) handleData(n enc.Name, data ndn.Data) {
	return
}
func (c *ChunksClient) Result() chan Result {
	return c.result
}

var app *eng.Engine

func passAll(enc.Name, enc.Wire, ndn.Signature) bool {
	return true
}
func main() {
	nameStr := flag.String("n", "", "name to express")
	segs := flag.Uint("s", 1, "number of segments")
	window := flag.Int("w", 1, "window size")
	flag.Parse()

	if len(*nameStr) == 0 {
		fmt.Println("Usage: defaults.go -n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	timer := eng.NewTimer()
	face := eng.NewStreamFace("unix", "/var/run/nfd.sock", true)
	app = eng.NewEngine(face, timer, sec.NewSha256IntSigner(timer), passAll)
	log.SetLevel(log.InfoLevel)
	logger := log.WithField("module", "main")
	err := app.Start()
	if err != nil {
		logger.Errorf("Unable to start engine: %+v", err)
		return
	}
	defer app.Shutdown()

	name, _ := enc.NameFromStr(*nameStr)
	cc := NewChunksClient(app, name, *segs, int64(*window))
	cc.GetChunks()
	res := <-cc.Result()
	if res == Successful {
		fmt.Printf("Successful!\n")
	} else {
		fmt.Printf("Failure.\n")
	}
}
