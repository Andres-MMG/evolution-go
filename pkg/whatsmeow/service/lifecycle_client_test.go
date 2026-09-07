package whatsmeow_service

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConnectionContextKeepsEstablishedSocketAliveForLogout(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	ctx, closeSocket, started := connectionContext(parent)
	defer closeSocket()
	started()
	stop()
	if ctx.Err() != nil {
		t.Fatal("stop canceled socket before graceful logout")
	}
	closeSocket()
	if ctx.Err() == nil {
		t.Fatal("teardown did not release socket context")
	}
}

type teardownFake struct {
	logoutErr                        error
	loggedOut, disconnected, removed bool
}

func (f *teardownFake) IsConnected() bool              { return true }
func (f *teardownFake) IsLoggedIn() bool               { return true }
func (f *teardownFake) Logout(context.Context) error   { f.loggedOut = true; return f.logoutErr }
func (f *teardownFake) Disconnect()                    { f.disconnected = true }
func (f *teardownFake) RemoveEventHandler(uint32) bool { f.removed = true; return true }

func TestTeardownClientPreservesLogoutResultAndAlwaysCleans(t *testing.T) {
	remoteFailure := errors.New("remote logout rejected")
	for _, tc := range []struct {
		name      string
		logout    bool
		remoteErr error
	}{
		{"success", true, nil}, {"failure", true, remoteFailure}, {"disconnect-only", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &teardownFake{logoutErr: tc.remoteErr}
			workersDrained, registriesCleared, storageClosed := false, false, false
			err := teardownClient(client, 1, tc.logout, func() {
				if !client.disconnected {
					t.Error("cleanup ran before disconnect")
				}
				workersDrained, registriesCleared, storageClosed = true, true, true
			})
			if !errors.Is(err, tc.remoteErr) {
				t.Fatalf("logout result = %v", err)
			}
			if tc.remoteErr != nil {
				var remote *RemoteLogoutError
				if !errors.As(err, &remote) {
					t.Fatal("remote error lost its classification")
				}
			}
			if !client.removed || !client.disconnected || !workersDrained || !registriesCleared || !storageClosed || client.loggedOut != tc.logout {
				t.Fatal("teardown omitted required cleanup or changed logout intent")
			}
		})
	}
}

func TestConnectionContextCancelsInFlightStartup(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	ctx, closeSocket, _ := connectionContext(parent)
	defer closeSocket()
	stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel in-flight connection")
	}
}
