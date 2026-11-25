package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   Kind
	}{
		{200, `{"message_id":"m1"}`, Delivered},
		{503, `{"code":"unavailable"}`, Temporary},
		{500, ``, Temporary},
		{408, ``, Temporary},
		{429, `{"code":"rate_limited"}`, RateLimited},
		{400, `{"code":"invalid_recipient","message":"mailbox does not exist"}`, Permanent},
		{410, `{"code":"device_token_unregistered"}`, Permanent},
		{401, ``, Permanent},
	}
	for _, c := range cases {
		r := Classify(c.status, "", []byte(c.body), 0)
		if r.Kind != c.want {
			t.Errorf("%d: got %s, want %s", c.status, r.Kind, c.want)
		}
		if r.Kind == Permanent && r.Action == "" {
			t.Errorf("%d: a permanent failure must carry an action", c.status)
		}
	}
	if r := Classify(429, "1.5", nil, 0); r.RetryAfter != 1500*time.Millisecond {
		t.Errorf("Retry-After: %v", r.RetryAfter)
	}
}

func TestTimeoutIsTemporary(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(300 * time.Millisecond) }))
	defer srv.Close()
	c := NewClient("email", srv.URL, 50*time.Millisecond)
	r := c.Send(context.Background(), Message{NotificationID: "n", Attempt: 1, Recipient: "a@example.com", Template: "welcome"})
	if r.Kind != Temporary || r.Code != "timeout" {
		t.Fatalf("got %+v", r)
	}
}

func TestRender(t *testing.T) {
	s, b, err := Render("otp", map[string]any{"code": 123456})
	if err != nil || s == "" || b != "Your code is 123456. It expires in 10 minutes." {
		t.Fatalf("%q %q %v", s, b, err)
	}
	if _, _, err := Render("nope", nil); err == nil {
		t.Fatal("unknown template accepted")
	}
}
