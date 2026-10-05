package sabnzbd_test

import (
	"context"
	"net/netip"
	"os"
	"testing"

	"github.com/Muxcore-Media/downloader-sabnzbd/internal/sabnzbd"
)

// publicResolver answers every name with a public address; unit tests never
// hit DNS.
type publicResolver struct{}

func (publicResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("93.184.215.14")}, nil
}

func TestMain(m *testing.M) {
	sabnzbd.SetResolver(publicResolver{})
	os.Exit(m.Run())
}
