package embetcd

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/signalfx/golib/pointer"
)

func Test_WaitForStructChOrErrCh(t *testing.T) {
	closedStructCh := make(chan struct{})
	close(closedStructCh)
	ErrChWithErr := make(chan error, 1)
	ErrChWithErr <- fmt.Errorf("error on error channel")
	canceledCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	type args struct {
		readyCh <-chan struct{}
		errCh   <-chan error
		ctx     context.Context
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test readyCh returns",
			args: args{
				readyCh: closedStructCh,
				errCh:   make(chan error),
				ctx:     context.Background(),
			},
			wantErr: false,
		},
		{
			name: "test errCh returns",
			args: args{
				readyCh: make(chan struct{}),
				errCh:   ErrChWithErr,
				ctx:     context.Background(),
			},
			wantErr: true,
		},
		{
			name: "test cancelled context returns",
			args: args{
				readyCh: make(chan struct{}),
				errCh:   make(chan error),
				ctx:     canceledCtx,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := WaitForStructChOrErrCh(tt.args.ctx, tt.args.readyCh, tt.args.errCh); (err != nil) != tt.wantErr {
				t.Errorf("waitForReady() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_duration(t *testing.T) {
	type args struct {
		in  *time.Duration
		def time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			name: "expect default to be returned when incoming value is nil",
			args: args{
				def: time.Second * 5,
			},
			want: time.Second * 5,
		},
		{
			name: "expect default to be returned when incoming value is nil",
			args: args{
				in:  pointer.Duration(time.Second * 1),
				def: time.Second * 5,
			},
			want: time.Second * 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DurationOrDefault(tt.args.in, tt.args.def); got != tt.want {
				t.Errorf("duration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_URLToSTringSlice(t *testing.T) {
	urls := URLSToStringSlice([]url.URL{{Scheme: "http", Host: "test1:8080"}})
	if len(urls) < 1 || urls[0] != "http://test1:8080" {
		t.Errorf("expected 'http://test1:8080', but got %v", urls)
	}
}

func TestCancelContext(t *testing.T) {
	type args struct {
		cancel context.CancelFunc
	}

	_, nonNil := context.WithCancel(context.Background())

	tests := []struct {
		name string
		args args
	}{
		{
			name: "nil cancel",
			args: args{},
		},
		{
			name: "non-nil cancel",
			args: args{
				cancel: nonNil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CancelContext(tt.args.cancel)
		})
	}
}
