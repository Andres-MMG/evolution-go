package whatsmeow_service

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

type clientVersion struct {
	Major int
	Minor int
	Patch int
}

// The upstream payload reads version globals during the handshake, not NewClient.
// Resolve them before any client starts, and never write them during reconnects.
type whatsAppVersionInitializer struct {
	once    sync.Once
	version store.WAVersionContainer
	source  string
	err     error
}

var processWhatsAppVersion = &whatsAppVersionInitializer{}

func (i *whatsAppVersionInitializer) initialize(configured clientVersion, fetch func() (*clientVersion, error)) (store.WAVersionContainer, string, error) {
	i.once.Do(func() {
		i.version = store.GetWAVersion()
		i.source = "embedded"
		selected := configured
		if configured.Major != 0 && configured.Minor != 0 && configured.Patch != 0 {
			i.source = "configured"
		} else {
			fetched, err := fetch()
			if err != nil {
				i.err = err
				return
			}
			if fetched == nil {
				i.err = fmt.Errorf("WhatsApp Web version lookup returned no version")
				return
			}
			selected = *fetched
			i.source = "web"
		}
		i.version = store.WAVersionContainer{uint32(selected.Major), uint32(selected.Minor), uint32(selected.Patch)}
		// This updates the actual handshake payload and registration build hash.
		store.SetWAVersion(i.version)
		store.DeviceProps.Version.Primary = proto.Uint32(i.version[0])
		store.DeviceProps.Version.Secondary = proto.Uint32(i.version[1])
		store.DeviceProps.Version.Tertiary = proto.Uint32(i.version[2])
	})
	return i.version, i.source, i.err
}

const whatsAppVersionLookupTimeout = 10 * time.Second

func fetchWhatsAppWebVersion() (*clientVersion, error) {
	return fetchWhatsAppWebVersionWithClient(&http.Client{Timeout: whatsAppVersionLookupTimeout})
}

func fetchWhatsAppWebVersionWithClient(client *http.Client) (*clientVersion, error) {
	resp, err := client.Get("https://web.whatsapp.com/sw.js")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch WhatsApp Web version: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("WhatsApp Web version lookup returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read WhatsApp Web version response: %w", err)
	}
	patterns := []string{
		`"client_revision":\s*(\d+)`,
		`\\"client_revision\\":\s*(\d+)`,
		`client_revision\\?\\"?:[\s]*(\d+)`,
		`["']client_revision["'][\s]*:[\s]*(\d+)`,
	}
	for _, pattern := range patterns {
		matches := regexp.MustCompile(pattern).FindSubmatch(body)
		if len(matches) < 2 {
			continue
		}
		revision, err := strconv.ParseUint(string(matches[1]), 10, 32)
		if err == nil && revision > 0 {
			return &clientVersion{Major: 2, Minor: 3000, Patch: int(revision)}, nil
		}
	}
	return nil, fmt.Errorf("could not find a valid client revision in the WhatsApp Web response")
}
