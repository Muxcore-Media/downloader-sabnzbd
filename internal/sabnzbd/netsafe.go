package sabnzbd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Muxcore-Media/core/sdk/go/module/netguard"
)

// EnvIndexerHosts is a comma-separated host[:port] allow-list of trusted LAN
// indexer proxies (e.g. Prowlarr / a local Newznab) whose NZB links may
// resolve to private addresses. Everything else is held to the UserURL
// profile (NFR-SEC-009 / RULE-VAL-2).
const EnvIndexerHosts = "DOWNLOADER_INDEXER_HOSTS"

// resolver is the DNS hook (tests replace it).
var resolver netguard.Resolver

// BaseURLOptions are the netguard options for the operator-configured SABnzbd
// endpoint: LAN and loopback services are legitimate, metadata/link-local are
// not.
func BaseURLOptions() netguard.Options {
	return netguard.Options{AllowPrivate: true, AllowLoopback: true, Timeout: 30 * time.Second}
}

// ValidateBaseURL rejects a SABnzbd base URL that is not http(s) or that
// targets a cloud-metadata / link-local / unspecified address.
func ValidateBaseURL(raw string) error {
	return netguard.ValidateURL(raw, netguard.Integration, BaseURLOptions())
}

func newGuardedHTTPClient() *http.Client {
	return netguard.NewClient(netguard.Integration, BaseURLOptions())
}

func indexerHosts() []string {
	var out []string
	for _, p := range strings.Split(os.Getenv(EnvIndexerHosts), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func trustedIndexer(raw string) bool {
	hosts := indexerHosts()
	if len(hosts) == 0 {
		return false
	}
	opts := netguard.Options{AllowPrivate: true, AllowLoopback: true, AllowedHosts: hosts}
	return netguard.ValidateURL(raw, netguard.Integration, opts) == nil
}

func userOptions() netguard.Options {
	return netguard.Options{Timeout: 10 * time.Second, Resolver: resolver}
}

// validateNZBURLSyntax checks the URL without touching the network: single
// http(s) URL whose host passes the UserURL profile (or is a listed indexer).
func validateNZBURLSyntax(nzbURL string) error {
	if strings.TrimSpace(nzbURL) == "" {
		return fmt.Errorf("nzb_url required")
	}
	if strings.ContainsAny(nzbURL, "\r\n\x00") || strings.TrimSpace(nzbURL) != nzbURL {
		return fmt.Errorf("nzb_url must be a single URL")
	}
	u, err := url.Parse(nzbURL)
	if err != nil {
		return fmt.Errorf("nzb_url must be http(s): %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("nzb_url must be http(s)")
	}
	if trustedIndexer(nzbURL) {
		return nil
	}
	if err := netguard.ValidateURL(nzbURL, netguard.UserURL, userOptions()); err != nil {
		return fmt.Errorf("nzb_url rejected: %w", err)
	}
	return nil
}

// checkNZBURL is the live-mode check: syntax plus DNS resolution of the host,
// so a name pointing at a private/metadata address is refused before SABnzbd
// (which fetches the URL itself) is asked to download it.
func checkNZBURL(ctx context.Context, nzbURL string) error {
	if err := validateNZBURLSyntax(nzbURL); err != nil {
		return err
	}
	if trustedIndexer(nzbURL) {
		return nil
	}
	u, _ := url.Parse(nzbURL)
	host := u.Hostname()
	if _, err := netip.ParseAddr(host); err == nil {
		return nil
	}
	opts := userOptions()
	res := netguard.Resolver(net.DefaultResolver)
	if resolver != nil {
		res = resolver
	}
	rctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	addrs, err := res.LookupNetIP(rctx, "ip", host)
	if err != nil {
		return fmt.Errorf("nzb_url rejected: resolve %q: %w", host, err)
	}
	for _, a := range addrs {
		if err := netguard.CheckAddr(a, netguard.UserURL, opts); err != nil {
			return fmt.Errorf("nzb_url rejected: %w", err)
		}
	}
	return nil
}

// SetResolver replaces the DNS resolver used for NZB URL checks and returns a
// restore func. Intended for tests (offline CI has no DNS).
func SetResolver(r netguard.Resolver) (restore func()) {
	old := resolver
	resolver = r
	return func() { resolver = old }
}
