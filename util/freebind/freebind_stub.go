// +build !linux

package freebind

import (
	"net"
)

// Like net.Listen but enables IP_FREEBIND on the returned net.Listener
func Listen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}
