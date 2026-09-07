package instance_service

import (
	"context"
	"errors"
	"testing"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	instance_repository "github.com/evolution-foundation/evolution-go/pkg/instance/repository"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
)

type stopServiceFake struct {
	whatsmeow_service.WhatsmeowService
	err    error
	logout bool
}

func (f *stopServiceFake) StopInstance(_ string, logout bool) error { f.logout = logout; return f.err }

type stopRepositoryFake struct {
	instance_repository.InstanceRepository
	instance         *instance_model.Instance
	updated, deleted bool
}

func (f *stopRepositoryFake) GetInstanceByID(string) (*instance_model.Instance, error) {
	return f.instance, nil
}
func (f *stopRepositoryFake) Update(*instance_model.Instance) error { f.updated = true; return nil }
func (f *stopRepositoryFake) Delete(string) error                   { f.deleted = true; return nil }

func TestLogoutPropagatesStopFailureWithoutReportingSuccess(t *testing.T) {
	remoteFailure := errors.New("remote logout rejected")
	for _, tc := range []struct {
		name    string
		stopErr error
	}{
		{"success", nil}, {"remote-failure", &whatsmeow_service.RemoteLogoutError{Err: remoteFailure}}, {"timeout", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instance := &instance_model.Instance{Id: "one", Connected: true}
			repo := &stopRepositoryFake{instance: instance}
			stop := &stopServiceFake{err: tc.stopErr}
			sut := instances{instanceRepository: repo, whatsmeowService: stop}
			_, err := sut.Logout(instance)
			if !errors.Is(err, tc.stopErr) || !stop.logout {
				t.Fatalf("logout returned %v", err)
			}
			if repo.updated != (tc.stopErr == nil) {
				t.Fatal("failed logout was persisted as successful")
			}
		})
	}
}

func TestDeleteIgnoresOnlyRemoteLogoutFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stopErr error
		deleted bool
	}{
		{"success", nil, true},
		{"remote-failure", &whatsmeow_service.RemoteLogoutError{Err: errors.New("remote logout rejected")}, true},
		{"timeout", context.DeadlineExceeded, false},
		{"other-failure", errors.New("teardown failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &stopRepositoryFake{instance: &instance_model.Instance{Id: "one"}}
			stop := &stopServiceFake{err: tc.stopErr}
			sut := instances{instanceRepository: repo, whatsmeowService: stop}
			err := sut.Delete("one")
			if repo.deleted != tc.deleted || !stop.logout {
				t.Fatal("delete compatibility changed")
			}
			if tc.deleted && err != nil {
				t.Fatalf("best-effort delete returned %v", err)
			}
			if !tc.deleted && !errors.Is(err, tc.stopErr) {
				t.Fatalf("teardown error was swallowed: %v", err)
			}
		})
	}
}
