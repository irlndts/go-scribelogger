package main

import (
	"fmt"
	"gitlab.srv.pv.km/go-libs/scribe"
	"time"
)

func main() {
	s := scribe.NewScribe(&scribe.ScribeConfig{
		Address:             "10.242.232.111:1463",
		ReconnectTimeoutSec: 20,
		MaxPacketSize:       50,
		EntryBufferSize:     1000,
	})

	time.Sleep(1 * time.Second)

	fmt.Println(s.IsOpen())
	s.Log("Debug", "Test Message")

}
