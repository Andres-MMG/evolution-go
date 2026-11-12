package whatsmeow_service

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/evolution-foundation/evolution-go/pkg/config"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/whatsmeow/lifecycle"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	"google.golang.org/protobuf/proto"
)

func preserveVersionGlobals(t *testing.T) {
	t.Helper()
	version := store.GetWAVersion()
	payload := proto.Clone(store.BaseClientPayload).(*waWa6.ClientPayload)
	device := proto.Clone(store.DeviceProps).(*waCompanionReg.DeviceProps)
	t.Cleanup(func() {
		store.SetWAVersion(version)
		store.BaseClientPayload = payload
		store.DeviceProps = device
	})
}

func TestWhatsAppVersionFallbackIsFrozen(t *testing.T) {
	for _, tt := range []struct {
		name    string
		version *clientVersion
		err     error
	}{
		{"lookup error", nil, errors.New("synthetic lookup failure")},
		{"missing lookup result", nil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preserveVersionGlobals(t)
			before := store.GetWAVersion()
			payload := proto.Clone(store.BaseClientPayload)
			device := proto.Clone(store.DeviceProps)
			var initializer whatsAppVersionInitializer
			calls := 0
			version, source, err := initializer.initialize(clientVersion{}, func() (*clientVersion, error) {
				calls++
				return tt.version, tt.err
			})
			if err == nil || source != "embedded" || version != before {
				t.Fatalf("fallback: version=%v source=%s err=%v", version, source, err)
			}
			if !proto.Equal(payload, store.BaseClientPayload) || !proto.Equal(device, store.DeviceProps) {
				t.Fatal("failed lookup changed existing payload or device properties")
			}
			initializer.initialize(clientVersion{2, 3000, 1054270629}, func() (*clientVersion, error) {
				t.Fatal("lookup retried after initialization")
				return nil, nil
			})
			if calls != 1 || store.GetWAVersion() != before {
				t.Fatal("later initialization changed the frozen fallback")
			}
			assertHandshakeVersion(t, before)
		})
	}
}

func TestWhatsAppVersionConcurrentInitializationAndReconnect(t *testing.T) {
	preserveVersionGlobals(t)
	var initializer whatsAppVersionInitializer
	var calls atomic.Int32
	want := store.WAVersionContainer{2, 3000, 1054270628}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 24 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			version, source, err := initializer.initialize(clientVersion{}, func() (*clientVersion, error) {
				calls.Add(1)
				return &clientVersion{2, 3000, 1054270628}, nil
			})
			if err != nil || source != "web" || version != want {
				t.Errorf("concurrent initialization: version=%v source=%s err=%v", version, source, err)
				return
			}
			for range 20 {
				initializer.initialize(clientVersion{2, 3000, 1054270629}, func() (*clientVersion, error) {
					calls.Add(1)
					return nil, errors.New("unexpected later lookup")
				})
				assertHandshakeVersion(t, want)
			}
		}()
	}
	close(start)
	workers.Wait()
	if calls.Load() != 1 {
		t.Fatalf("lookups=%d, want exactly one", calls.Load())
	}
}

func TestWhatsAppVersionPreservesConfiguredUint32Conversion(t *testing.T) {
	preserveVersionGlobals(t)
	var initializer whatsAppVersionInitializer
	version, source, err := initializer.initialize(clientVersion{-1, 3000, 4294967297}, func() (*clientVersion, error) {
		t.Fatal("complete configured version unexpectedly fetched")
		return nil, nil
	})
	want := store.WAVersionContainer{4294967295, 3000, 1}
	if err != nil || source != "configured" || version != want {
		t.Fatalf("legacy conversion: version=%v source=%s err=%v", version, source, err)
	}
	assertHandshakeVersion(t, want)
}

type versionRoundTripper func(*http.Request) (*http.Response, error)

func (f versionRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failingVersionBody struct{}

func (failingVersionBody) Read([]byte) (int, error) { return 0, errors.New("synthetic read failure") }
func (failingVersionBody) Close() error             { return nil }

func TestFetchWhatsAppWebVersion(t *testing.T) {
	for _, tt := range []struct {
		name           string
		status         int
		body           string
		transportError bool
		readError      bool
		wantError      bool
	}{
		{"plain revision", 200, `{"client_revision":1054270628}`, false, false, false},
		{"escaped revision", 200, `{\"client_revision\":1054270628}`, false, false, false},
		{"single quotes", 200, `{'client_revision' : 1054270628}`, false, false, false},
		{"zero revision", 200, `{"client_revision":0}`, false, false, true},
		{"missing revision", 200, `not a version document`, false, false, true},
		{"overflow revision", 200, `{"client_revision":4294967296}`, false, false, true},
		{"HTTP failure with valid looking body", 503, `{"client_revision":1054270628}`, false, false, true},
		{"transport failure", 0, "", true, false, true},
		{"body read failure", 200, "", false, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preserveVersionGlobals(t)
			beforeVersion := store.GetWAVersion()
			beforePayload := proto.Clone(store.BaseClientPayload)
			beforeDevice := proto.Clone(store.DeviceProps)
			calls := 0
			client := &http.Client{Transport: versionRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != http.MethodGet || r.URL.String() != "https://web.whatsapp.com/sw.js" {
					t.Errorf("unexpected lookup request: %s %s", r.Method, r.URL)
				}
				if tt.transportError {
					return nil, errors.New("synthetic transport failure")
				}
				var body io.ReadCloser = io.NopCloser(strings.NewReader(tt.body))
				if tt.readError {
					body = failingVersionBody{}
				}
				return &http.Response{StatusCode: tt.status, Body: body, Header: make(http.Header)}, nil
			})}
			var initializer whatsAppVersionInitializer
			version, source, err := initializer.initialize(clientVersion{}, func() (*clientVersion, error) {
				return fetchWhatsAppWebVersionWithClient(client)
			})
			if calls != 1 || (err != nil) != tt.wantError {
				t.Fatalf("lookup: calls=%d version=%v err=%v", calls, version, err)
			}
			if tt.wantError {
				if source != "embedded" || version != beforeVersion || !proto.Equal(beforePayload, store.BaseClientPayload) || !proto.Equal(beforeDevice, store.DeviceProps) {
					t.Fatal("failed HTTP lookup changed the embedded handshake or device properties")
				}
			} else {
				want := store.WAVersionContainer{2, 3000, 1054270628}
				if source != "web" || version != want {
					t.Fatalf("unexpected parsed version: %v (source: %s)", version, source)
				}
				assertHandshakeVersion(t, want)
			}
		})
	}
}

func TestServiceConstructorInitializesVersionBeforeClients(t *testing.T) {
	preserveVersionGlobals(t)
	oldInitializer, oldTransport := processWhatsAppVersion, http.DefaultTransport
	processWhatsAppVersion = &whatsAppVersionInitializer{}
	t.Cleanup(func() { processWhatsAppVersion, http.DefaultTransport = oldInitializer, oldTransport })
	lookups := 0
	http.DefaultTransport = versionRoundTripper(func(r *http.Request) (*http.Response, error) {
		lookups++
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > whatsAppVersionLookupTimeout {
			t.Error("startup lookup did not have the bounded HTTP deadline")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"client_revision":1054270628}`)), Header: make(http.Header)}, nil
	})
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := &config.Config{LogDirectory: t.TempDir()}
	loggers := logger_wrapper.NewLoggerManager(cfg)
	defer loggers.GetLogger("poll-service").Close()
	clients := &lifecycle.Registry[*whatsmeow.Client]{}
	existing := &whatsmeow.Client{}
	clients.Store("existing-session", existing)
	for range 2 {
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS poll_votes").WillReturnResult(sqlmock.NewResult(0, 0))
		service := NewWhatsmeowService(nil, db, nil, nil, cfg, clients, nil, nil, nil, nil, "", nil, nil, loggers)
		if service == nil || clients.Load("existing-session") != existing {
			t.Fatal("constructor started or lost clients")
		}
		assertHandshakeVersion(t, store.WAVersionContainer{2, 3000, 1054270628})
		cfg.WhatsappVersionMajor, cfg.WhatsappVersionMinor, cfg.WhatsappVersionPatch = 2, 3000, 1054270629
	}
	if lookups != 1 {
		t.Fatalf("constructor lookups=%d, want one", lookups)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func assertHandshakeVersion(t *testing.T, want store.WAVersionContainer) {
	t.Helper()
	if got := store.GetWAVersion(); got != want {
		t.Errorf("announced version = %v, want %v", got, want)
	}
	jid := types.NewJID("10000000000", types.DefaultUserServer)
	registered := (&store.Device{ID: &jid}).GetClientPayload()
	identity := keys.NewKeyPair()
	fresh := (&store.Device{IdentityKey: identity, SignedPreKey: identity.CreateSignedPreKey(1)}).GetClientPayload()
	for name, payload := range map[string]*waWa6.ClientPayload{"registered": registered, "fresh": fresh} {
		version := payload.GetUserAgent().GetAppVersion()
		got := store.WAVersionContainer{version.GetPrimary(), version.GetSecondary(), version.GetTertiary()}
		if got != want {
			t.Errorf("%s handshake version = %v, want %v", name, got, want)
		}
	}
	hash := want.Hash()
	if !bytes.Equal(fresh.GetDevicePairingData().GetBuildHash(), hash[:]) {
		t.Error("registration build hash does not match the selected handshake version")
	}
}

func TestWhatsAppVersionUpdatesHandshake(t *testing.T) {
	for _, tt := range []struct {
		name       string
		configured clientVersion
		wantFetch  int
	}{
		{"configured", clientVersion{2, 3000, 1054270628}, 0},
		{"fetched", clientVersion{}, 1},
		{"partial configuration fetches", clientVersion{2, 0, 1}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			preserveVersionGlobals(t)
			var initializer whatsAppVersionInitializer
			calls := 0
			got, _, err := initializer.initialize(tt.configured, func() (*clientVersion, error) {
				calls++
				return &clientVersion{2, 3000, 1054270628}, nil
			})
			if err != nil || calls != tt.wantFetch {
				t.Fatalf("lookup: calls=%d, err=%v", calls, err)
			}
			assertHandshakeVersion(t, got)
		})
	}
}
