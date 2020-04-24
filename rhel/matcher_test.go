package rhel

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/quay/claircore"
	"github.com/quay/claircore/internal/matcher"
	"github.com/quay/claircore/internal/updater"
	vulnstore "github.com/quay/claircore/internal/vulnstore/postgres"
	"github.com/quay/claircore/libvuln/driver"
	"github.com/quay/claircore/pkg/distlock"
	"github.com/quay/claircore/test"
	"github.com/quay/claircore/test/integration"
	"github.com/quay/claircore/test/log"
)

func TestMatcherIntegration(t *testing.T) {
	integration.Skip(t)
	ctx, done := context.WithCancel(context.Background())
	defer done()
	ctx = log.TestLogger(ctx, t)
	store, teardown := vulnstore.TestStore(ctx, t)
	defer teardown()
	m := &Matcher{}
	fs, err := filepath.Glob("testdata/*.xml")
	if err != nil {
		t.Error(err)
	}
	us := make([]*updater.Controller, len(fs))
	for i, f := range fs {
		u, err := test.Updater(f)
		if err != nil {
			t.Error(err)
			continue
		}
		l := distlock.NewMockLocker(gomock.NewController(t))
		name := fmt.Sprintf("test-%s", filepath.Base(f))
		gomock.InOrder(
			l.EXPECT().TryLock(gomock.Any(), name),
			l.EXPECT().Unlock(gomock.Any()),
		)
		us[i] = updater.NewController(&updater.Opts{
			Name:     name,
			Updater:  u,
			Store:    store,
			Interval: -1 * time.Minute,
			Lock:     l,
		})
	}
	// force update
	wctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(len(us))
	for _, c := range us {
		c := c
		go func() {
			c.Update(wctx)
			wg.Done()
		}()
	}
	wg.Wait()
	f, err := os.Open(filepath.Join("testdata", "rhel-report.json"))
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer f.Close()
	var ir claircore.IndexReport
	if err := json.NewDecoder(f).Decode(&ir); err != nil {
		t.Fatalf("failed to decode IndexReport: %v", err)
	}
	vr, err := matcher.Match(ctx, &ir, []driver.Matcher{m}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(ioutil.Discard).Encode(&vr); err != nil {
		t.Fatalf("failed to marshal VR: %v", err)
	}
}
