package bulk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-dcp-redis/config"
	"github.com/Trendyol/go-dcp-redis/redis"
	"github.com/Trendyol/go-dcp-redis/redis/client"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	goredis "github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	// dcp.NewDcp initializes the logger in production.
	logger.InitDefaultLogger("error")
	os.Exit(m.Run())
}

// fakeHyrat registers a HYRAT.SET-like custom command on a miniredis server.
type fakeHyrat struct {
	values  map[string]string
	fail    func(key, value string) string
	order   []string
	delay   time.Duration
	callsAt atomic.Int64
	mu      sync.Mutex
}

func newFakeHyrat(t *testing.T, m *miniredis.Miniredis) *fakeHyrat {
	t.Helper()
	f := &fakeHyrat{values: map[string]string{}}
	err := m.Server().Register("HYRAT.SET", func(c *server.Peer, _ string, args []string) {
		f.callsAt.Add(1)
		if f.delay > 0 {
			time.Sleep(f.delay)
		}
		if len(args) != 2 {
			c.WriteError("ERR wrong number of arguments for 'hyrat.set' command")
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.fail != nil {
			if msg := f.fail(args[0], args[1]); msg != "" {
				c.WriteError(msg)
				return
			}
		}
		f.values[args[0]] = args[1]
		f.order = append(f.order, args[0]+"="+args[1])
		c.WriteInt(len(f.order))
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fakeHyrat) get(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.values[key]
}

func newTestBulk(t *testing.T, cfg config.Redis, commit func()) (*Bulk, *miniredis.Miniredis) {
	t.Helper()
	m := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	b := newBulk(cfg, c, nil, commit)
	b.retryMinBackoff = time.Millisecond
	b.retryMaxBackoff = 5 * time.Millisecond
	go b.runFlusher()
	t.Cleanup(b.Close)
	return b, m
}

type ackCounter struct{ n atomic.Int64 }

func (a *ackCounter) ctx() *models.ListenerContext {
	return &models.ListenerContext{Ack: func() { a.n.Add(1) }}
}

func waitDrained(b *Bulk) {
	b.mu.Lock()
	b.waitDrainedLocked()
	b.mu.Unlock()
}

func hyratSet(key, value string) redis.Model {
	return &redis.Raw{Operation: "HYRAT.SET", Key: key, Args: []interface{}{value}}
}

func TestRawCommandIsWrittenThroughPipeline(t *testing.T) {
	b, m := newTestBulk(t, config.Redis{}, nil)
	hyrat := newFakeHyrat(t, m)
	acks := &ackCounter{}

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("doc:%d", i)
		b.AddActions(acks.ctx(), time.Now(), "_default", []redis.Model{hyratSet(key, fmt.Sprintf(`{"v":%d}`, i))})
	}
	waitDrained(b)

	if got := acks.n.Load(); got != 500 {
		t.Fatalf("acked %d events, want 500", got)
	}
	for i := 0; i < 500; i++ {
		if got, want := hyrat.get(fmt.Sprintf("doc:%d", i)), fmt.Sprintf(`{"v":%d}`, i); got != want {
			t.Fatalf("doc:%d = %q, want %q", i, got, want)
		}
	}
	if b.GetMetric().BulkRequestSize == 0 {
		t.Error("BulkRequestSize metric was not recorded")
	}
}

func TestTypedAndRawCommands(t *testing.T) {
	b, m := newTestBulk(t, config.Redis{}, nil)
	if err := m.Set("old", "x"); err != nil {
		t.Fatal(err)
	}
	m.HSet("h2", "gone", "x")

	b.AddActions((&ackCounter{}).ctx(), time.Now(), "_default", []redis.Model{
		&redis.Set{Key: "s", Value: "v", TTL: time.Hour},
		&redis.Del{Key: "old"},
		&redis.HSet{Key: "h", Field: "f", Value: "hv", TTL: time.Minute},
		&redis.HDel{Key: "h2", Field: "gone"},
		&redis.Raw{Operation: "SET", Key: "raw-set", Value: "rv", TTL: time.Hour},
		&redis.Raw{Operation: "HDEL", Key: "h", Args: []interface{}{[]byte("missing")}},
		&redis.Raw{Operation: "ZADD", Key: "z", Args: []interface{}{7, "member"}},
		&redis.Raw{Operation: "HSET", Key: "skipped", Args: []interface{}{"only-field"}},
	})
	waitDrained(b)

	if got, _ := m.Get("s"); got != "v" || m.TTL("s") != time.Hour {
		t.Errorf("SET s = %q ttl %v, want v with 1h ttl", got, m.TTL("s"))
	}
	if m.Exists("old") {
		t.Error("DEL old did not delete the key")
	}
	if got := m.HGet("h", "f"); got != "hv" || m.TTL("h") != time.Minute {
		t.Errorf("HSET h f = %q ttl %v, want hv with 1m ttl", got, m.TTL("h"))
	}
	if m.Exists("h2") {
		t.Error("HDEL h2 gone did not delete the field")
	}
	if got, _ := m.Get("raw-set"); got != "rv" {
		t.Errorf("raw SET = %q, want rv", got)
	}
	if score, err := m.ZScore("z", "member"); err != nil || score != 7 {
		t.Errorf("ZADD z = %v (%v), want 7", score, err)
	}
	if m.Exists("skipped") {
		t.Error("HSET without a value must be skipped")
	}
}

func TestSameKeyKeepsDCPOrder(t *testing.T) {
	b, m := newTestBulk(t, config.Redis{BatchSizeLimit: 7}, nil)
	hyrat := newFakeHyrat(t, m)

	for i := 0; i < 200; i++ {
		b.AddActions((&ackCounter{}).ctx(), time.Now(), "_default", []redis.Model{hyratSet("k", fmt.Sprint(i))})
	}
	waitDrained(b)

	if got := hyrat.get("k"); got != "199" {
		t.Fatalf("k = %q, want the last write 199", got)
	}
	hyrat.mu.Lock()
	defer hyrat.mu.Unlock()
	for i, entry := range hyrat.order {
		if want := fmt.Sprintf("k=%d", i); entry != want {
			t.Fatalf("write %d = %q, want %q", i, entry, want)
		}
	}
}

func TestTransientErrorIsRetriedWithoutReorderingKey(t *testing.T) {
	b, m := newTestBulk(t, config.Redis{}, nil)
	hyrat := newFakeHyrat(t, m)
	var failures atomic.Int64
	hyrat.fail = func(key, value string) string {
		if key == "a" && value == "v1" && failures.Add(1) <= 2 {
			return "OOM hyrat mutation memory limit reached (hyrat.max-ram); retry later"
		}
		return ""
	}

	b.AddActions((&ackCounter{}).ctx(), time.Now(), "_default", []redis.Model{
		hyratSet("a", "v1"), hyratSet("b", "b1"), hyratSet("a", "v2"),
	})
	waitDrained(b)

	if got := hyrat.get("a"); got != "v2" {
		t.Fatalf("a = %q, want v2 (a retried write must not overwrite a newer one)", got)
	}
	if got := hyrat.get("b"); got != "b1" {
		t.Fatalf("b = %q, want b1", got)
	}
	if got := b.GetMetric().RetriedCommands; got != 4 {
		t.Errorf("RetriedCommands = %d, want 4 (a=v1 and a=v2, twice)", got)
	}
}

func TestRetriesAreBounded(t *testing.T) {
	m := miniredis.RunT(t)
	hyrat := newFakeHyrat(t, m)
	hyrat.fail = func(string, string) string { return "BUSY Redis-Hyrat is warming up storage" }
	c := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	b := newBulk(config.Redis{MaxRetries: 3}, c, nil, nil)
	b.retryMinBackoff = time.Millisecond

	err := b.execute(c, []*redis.Command{hyratSet("a", "v").Convert()})
	if err == nil || !strings.Contains(err.Error(), "giving up after 3 retries") {
		t.Fatalf("err = %v, want giving up after 3 retries", err)
	}
	if got := hyrat.callsAt.Load(); got != 4 {
		t.Errorf("HYRAT.SET called %d times, want 4", got)
	}
}

func TestNonRetryableErrorIsFatal(t *testing.T) {
	m := miniredis.RunT(t)
	c := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	b := newBulk(config.Redis{}, c, nil, nil)
	fatal := make(chan error, 1)
	b.fatal = func(err error) { fatal <- err }
	go b.runFlusher()
	defer b.Close()

	b.AddActions((&ackCounter{}).ctx(), time.Now(), "_default", []redis.Model{
		&redis.Raw{Operation: "NO.SUCH.COMMAND", Key: "k"},
	})

	select {
	case err := <-fatal:
		if !strings.Contains(err.Error(), `NO.SUCH.COMMAND "k"`) {
			t.Fatalf("err = %v, want operation and key in the error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("non-retryable error did not reach the fatal handler")
	}
}

func TestCommitOnlyAfterAckedEventsAreWritten(t *testing.T) {
	var (
		b     *Bulk
		hyrat *fakeHyrat
		acks  ackCounter
	)
	commits := make(chan int64, 1)
	b, m := newTestBulk(t, config.Redis{}, func() {
		n := acks.n.Load()
		for i := int64(0); i < n; i++ {
			if hyrat.get(fmt.Sprintf("doc:%d", i)) == "" {
				panic(fmt.Sprintf("checkpoint committed before doc:%d was written", i))
			}
		}
		commits <- n
	})
	hyrat = newFakeHyrat(t, m)
	hyrat.delay = 2 * time.Millisecond

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 300; i++ {
			b.AddActions(acks.ctx(), time.Now(), "_default", []redis.Model{hyratSet(fmt.Sprintf("doc:%d", i), "v")})
		}
	}()
	time.Sleep(20 * time.Millisecond)
	b.commit()
	<-done

	select {
	case n := <-commits:
		if n == 0 {
			t.Error("commit ran before any event was acknowledged")
		}
	default:
		t.Fatal("commit did not call the checkpoint function")
	}
}

func TestBackPressureBoundsPendingBatch(t *testing.T) {
	b, m := newTestBulk(t, config.Redis{BatchSizeLimit: 4}, nil)
	hyrat := newFakeHyrat(t, m)
	hyrat.delay = time.Millisecond

	var maxPending atomic.Int64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			b.mu.Lock()
			if n := int64(len(b.pending)); n > maxPending.Load() {
				maxPending.Store(n)
			}
			b.mu.Unlock()
		}
	}()
	for i := 0; i < 100; i++ {
		b.AddActions((&ackCounter{}).ctx(), time.Now(), "_default", []redis.Model{hyratSet(fmt.Sprint(i), "v")})
	}
	waitDrained(b)
	close(stop)

	if got := maxPending.Load(); got > 4 {
		t.Fatalf("pending batch reached %d commands, want <= batchSizeLimit 4", got)
	}
	if got := hyrat.get("99"); got != "v" {
		t.Fatalf("last event was not written")
	}
}

func TestOversizedEventIsAcceptedIntoEmptyBatch(t *testing.T) {
	b, _ := newTestBulk(t, config.Redis{BatchSizeLimit: 2, BatchByteSizeLimit: "1kb"}, nil)
	actions := make([]redis.Model, 0, 10)
	for i := 0; i < 10; i++ {
		actions = append(actions, &redis.Set{Key: fmt.Sprint(i), Value: strings.Repeat("x", 512)})
	}
	acks := &ackCounter{}
	b.AddActions(acks.ctx(), time.Now(), "_default", actions)
	waitDrained(b)
	if acks.n.Load() != 1 {
		t.Fatal("an event larger than the batch limits must still be accepted")
	}
}

func TestRebalancingDrainsCommitsAndRejects(t *testing.T) {
	var commits atomic.Int64
	b, m := newTestBulk(t, config.Redis{}, func() { commits.Add(1) })
	hyrat := newFakeHyrat(t, m)
	hyrat.delay = 5 * time.Millisecond
	b.started = true

	for i := 0; i < 20; i++ {
		b.AddActions((&ackCounter{}).ctx(), time.Now(), "_default", []redis.Model{hyratSet(fmt.Sprint(i), "v")})
	}
	b.PrepareStartRebalancing()

	for i := 0; i < 20; i++ {
		if hyrat.get(fmt.Sprint(i)) != "v" {
			t.Fatalf("event %d was not written before the streams stopped", i)
		}
	}
	if commits.Load() != 1 {
		t.Errorf("commits = %d, want 1 after draining", commits.Load())
	}

	rejected := &ackCounter{}
	b.AddActions(rejected.ctx(), time.Now(), "_default", []redis.Model{hyratSet("late", "v")})
	if rejected.n.Load() != 0 {
		t.Error("an event added while rebalancing must not be acknowledged")
	}

	b.PrepareEndRebalancing()
	accepted := &ackCounter{}
	b.AddActions(accepted.ctx(), time.Now(), "_default", []redis.Model{hyratSet("after", "v")})
	waitDrained(b)
	if accepted.n.Load() != 1 || hyrat.get("after") != "v" {
		t.Error("events must be accepted again after rebalancing ends")
	}
}

func TestCloseWritesPendingCommands(t *testing.T) {
	m := miniredis.RunT(t)
	hyrat := newFakeHyrat(t, m)
	hyrat.delay = 2 * time.Millisecond
	c := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	b := newBulk(config.Redis{}, c, nil, nil)
	go b.runFlusher()

	for i := 0; i < 50; i++ {
		b.AddActions((&ackCounter{}).ctx(), time.Now(), "_default", []redis.Model{hyratSet(fmt.Sprint(i), "v")})
	}
	b.Close()

	if got := hyrat.get("49"); got != "v" {
		t.Fatal("Close must write every acknowledged event")
	}
}

func TestCollectionsAreWrittenToTheirInstance(t *testing.T) {
	def := miniredis.RunT(t)
	eu := miniredis.RunT(t)
	defClient := goredis.NewClient(&goredis.Options{Addr: def.Addr()})
	euClient := goredis.NewClient(&goredis.Options{Addr: eu.Addr()})
	cfg := config.Redis{CollectionKeyMapping: []config.CollectionKeyMapping{{Collection: "products", Target: "eu"}}}
	b := newBulk(cfg, defClient, map[string]client.RedisClient{"eu": euClient}, nil)
	go b.runFlusher()
	defer b.Close()

	for i := 0; i < 50; i++ {
		b.AddActions((&ackCounter{}).ctx(), time.Now(), "products", []redis.Model{&redis.Set{Key: fmt.Sprint("p", i), Value: "eu"}})
		b.AddActions((&ackCounter{}).ctx(), time.Now(), "users", []redis.Model{&redis.Set{Key: fmt.Sprint("u", i), Value: "def"}})
	}
	waitDrained(b)

	if got, _ := eu.Get("p49"); got != "eu" || def.Exists("p49") {
		t.Errorf("products must only be written to the eu instance")
	}
	if got, _ := def.Get("u49"); got != "def" || eu.Exists("u49") {
		t.Errorf("users must only be written to the default instance")
	}
}

func TestRetrySet(t *testing.T) {
	cmds := []*redis.Command{
		{Operation: "SET", Key: "a"}, // 0 ok
		{Operation: "SET", Key: "b"}, // 1 failed
		{Operation: "SET", Key: "a"}, // 2 ok
		{Operation: "SET", Key: "b"}, // 3 ok, later write of a failed key
		{Operation: "SET", Key: "c"}, // 4 failed
	}
	errs := []error{nil, errors.New("x"), nil, nil, errors.New("x")}

	got := retrySet(cmds, errs)
	want := []*redis.Command{cmds[1], cmds[3], cmds[4]}
	if len(got) != len(want) {
		t.Fatalf("retrySet returned %d commands, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("retrySet[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// redisErr is a server error reply, like the errors go-redis returns for "-ERR ..." replies.
type redisErr string

func (e redisErr) Error() string { return string(e) }
func (redisErr) RedisError()     {}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		err  error
		name string
		want bool
	}{
		{name: "hyrat oom", err: redisErr("OOM hyrat mutation memory limit reached"), want: true},
		{name: "redis oom", err: redisErr("OOM command not allowed when used memory > 'maxmemory'."), want: true},
		{name: "busy", err: redisErr("BUSY Redis-Hyrat is warming up storage"), want: true},
		{name: "loading", err: redisErr("LOADING Redis is loading the dataset in memory"), want: true},
		{name: "tryagain", err: redisErr("TRYAGAIN Multiple keys request during rehashing of slot"), want: true},
		{name: "clusterdown", err: redisErr("CLUSTERDOWN The cluster is down"), want: true},
		{name: "readonly", err: redisErr("READONLY You can't write against a read only replica."), want: true},
		{name: "moved", err: redisErr("MOVED 3999 127.0.0.1:6381"), want: true},
		{name: "ask", err: redisErr("ASK 3999 127.0.0.1:6381"), want: true},
		{name: "eof", err: io.EOF, want: true},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "busygroup is not busy", err: redisErr("BUSYGROUP Consumer Group name already exists"), want: false},
		{name: "wrongtype", err: redisErr("WRONGTYPE Operation against a key holding the wrong kind of value"), want: false},
		{name: "syntax", err: redisErr("ERR syntax error"), want: false},
		{name: "closed client", err: goredis.ErrClosed, want: false},
		{name: "canceled", err: context.Canceled, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRetryableError(tt.err); got != tt.want {
				t.Errorf("isRetryableError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestBatchByteSizeLimitParsing(t *testing.T) {
	b := newBulk(config.Redis{BatchByteSizeLimit: "2kb"}, nil, nil, nil)
	if b.batchByteSizeLimit != 2048 {
		t.Errorf("batchByteSizeLimit = %d, want 2048", b.batchByteSizeLimit)
	}
	b = newBulk(config.Redis{}, nil, nil, nil)
	if b.batchByteSizeLimit != config.DefaultBatchByteSizeLimit || b.batchSizeLimit != config.DefaultBatchSizeLimit ||
		b.maxRetries != config.DefaultMaxRetries {
		t.Errorf("zero config must use the defaults, got %+v", b)
	}
	if b = newBulk(config.Redis{MaxRetries: -1}, nil, nil, nil); b.maxRetries != 0 {
		t.Errorf("maxRetries = %d, want 0 when disabled", b.maxRetries)
	}
}

func TestIsClusterClient(t *testing.T) {
	cluster := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	defer cluster.Close()
	standalone := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer standalone.Close()

	if !isClusterClient(cluster) {
		t.Error("cluster client not detected")
	}
	if isClusterClient(standalone) {
		t.Error("standalone client detected as cluster")
	}
}

type reloadSpyClient struct {
	client.RedisClient
	reloads atomic.Int64
}

func (r *reloadSpyClient) ReloadState(context.Context) {
	r.reloads.Add(1)
}

func TestClusterStateReloadOnFailoverError(t *testing.T) {
	m := miniredis.RunT(t)
	hyrat := newFakeHyrat(t, m)
	spy := &reloadSpyClient{RedisClient: goredis.NewClient(&goredis.Options{Addr: m.Addr()})}
	defer spy.Close()

	b := newBulk(config.Redis{MaxRetries: 5}, spy, nil, nil)
	b.retryMinBackoff = time.Millisecond
	b.retryMaxBackoff = 5 * time.Millisecond

	var attempts atomic.Int64
	hyrat.fail = func(string, string) string {
		if attempts.Add(1) == 1 {
			return "OOM hyrat mutation memory limit reached"
		}
		return ""
	}
	if err := b.execute(spy, []*redis.Command{hyratSet("k1", "v1").Convert()}); err != nil {
		t.Fatalf("execute failed on OOM retry: %v", err)
	}
	if got := spy.reloads.Load(); got != 0 {
		t.Fatalf("ReloadState called %d times on OOM back-pressure, want 0", got)
	}

	attempts.Store(0)
	hyrat.fail = func(string, string) string {
		if attempts.Add(1) == 1 {
			return "READONLY You can't write against a read only replica."
		}
		return ""
	}
	if err := b.execute(spy, []*redis.Command{hyratSet("k2", "v2").Convert()}); err != nil {
		t.Fatalf("execute failed on READONLY retry: %v", err)
	}
	if got := spy.reloads.Load(); got != 1 {
		t.Fatalf("ReloadState called %d times on READONLY failover error, want 1", got)
	}
}

// retryBudget sums the sleeps of maxRetries retries starting at the default min backoff.
func retryBudget(maxRetries int, backpressureOnly bool) time.Duration {
	total := time.Duration(0)
	backoff := defaultRetryMinBackoff
	for i := 0; i < maxRetries; i++ {
		total += backoff
		backoff = nextBackoff(backoff, defaultRetryMaxBackoff, backpressureOnly)
	}
	return total
}

func TestDefaultRetryBudget(t *testing.T) {
	// Sustained OOM / BUSY back-pressure must not crash the connector sooner than the old
	// defaults did (10 retries, 100ms..3s backoff = ~18s).
	if got := retryBudget(config.DefaultMaxRetries, true); got < 20*time.Second {
		t.Errorf("back-pressure retry budget = %v, want >= 20s", got)
	}
	// A Redis Cluster failover (node timeout + election) must be survivable.
	if got := retryBudget(config.DefaultMaxRetries, false); got < 60*time.Second {
		t.Errorf("failover retry budget = %v, want >= 60s", got)
	}
	if got := nextBackoff(800*time.Millisecond, defaultRetryMaxBackoff, true); got != time.Second {
		t.Errorf("back-pressure backoff = %v, want capped at 1s", got)
	}
	if got := nextBackoff(2*time.Second, defaultRetryMaxBackoff, false); got != defaultRetryMaxBackoff {
		t.Errorf("failover backoff = %v, want capped at %v", got, defaultRetryMaxBackoff)
	}
}
