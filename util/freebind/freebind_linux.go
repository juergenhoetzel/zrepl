// +build linux
package freebind

import (
	"context"
	"net"
	"syscall"
)

var listenConfig = net.ListenConfig{
	Control: freeBind,
}

func freeBind(network, address string, c syscall.RawConn) error {
	var err, sockerr error
	err = c.Control(func(fd uintptr) {
		sockerr = syscall.SetsockoptInt(int(fd), syscall.SOL_IP, syscall.IP_FREEBIND, 1)
	})
	if err != nil {
		return err
	}
	return sockerr
}

// Like net.Listen but enables IP_FREEBIND on the returned net.Listener
func Listen(network, address string) (net.Listener, error) {
	return listenConfig.Listen(context.Background(), network, address)
}
