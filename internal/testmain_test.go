package internal_test

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
	// Existing tests use plaintext listeners; the mesh TLS test re-enables TLS.
	_ = os.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	os.Exit(m.Run())
}
