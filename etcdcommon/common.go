package etcdcommon

import (
	"context"
	"net/url"
	"time"
)

// WaitForStructChOrErrCh waits for the struct channel, error channel or context to return a value
func WaitForStructChOrErrCh(ctx context.Context, structCh <-chan struct{}, errCh <-chan error) error {
	// wait for the server to start or error out
	select {
	case <-structCh:
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DurationOrDefault returns the pointed duration or the specified default
func DurationOrDefault(in *time.Duration, def time.Duration) time.Duration {
	if in != nil {
		return *in
	}
	return def
}

// urls to string slice converts an array of url.URL to a slice of strings
func URLSToStringSlice(urls []url.URL) []string {
	strs := make([]string, 0, len(urls))
	for _, u := range urls {
		strs = append(strs, u.String())
	}
	return strs
}
