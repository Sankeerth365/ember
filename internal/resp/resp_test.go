package resp_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"ember/internal/resp"
)

func TestReadCommand(t *testing.T) {
	for _, tc := range []struct {
		name, wire string
		want       [][]byte
		err        error
	}{
		{"binary", "*2\r\n$4\r\nECHO\r\n$5\r\na\x00\r\nb\r\n", [][]byte{[]byte("ECHO"), []byte("a\x00\r\nb")}, nil},
		{"empty_argument", "*1\r\n$0\r\n\r\n", [][]byte{{}}, nil},
		{"inline_ping", "pInG\r\n", [][]byte{[]byte("PING")}, nil},
		{"eof", "", nil, io.EOF},
		{"empty_command", "*0\r\n", nil, resp.ErrProtocol},
		{"negative_bulk", "*1\r\n$-1\r\n", nil, resp.ErrProtocol},
		{"too_many_args", "*1025\r\n", nil, resp.ErrProtocol},
		{"oversize_bulk", "*1\r\n$16777217\r\n", nil, resp.ErrProtocol},
		{"truncated_header", "*1\r\n$3\r", nil, io.ErrUnexpectedEOF},
		{"truncated_bulk", "*1\r\n$3\r\nhi", nil, io.ErrUnexpectedEOF},
		{"bad_terminator", "*1\r\n$3\r\nabcxx", nil, resp.ErrProtocol},
		{"arbitrary_inline", "GET k\r\n", nil, resp.ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resp.NewDecoder(strings.NewReader(tc.wire)).ReadCommand()
			if !errors.Is(err, tc.err) || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %q, %v", got, err)
			}
		})
	}
}

func TestEncodeReply(t *testing.T) {
	var b bytes.Buffer
	e := resp.NewEncoder(&b)
	value := resp.Array(resp.Simple("OK"), resp.Null(), resp.Bulk(nil), resp.Bulk([]byte{0, '\n'}),
		resp.Integer(-5), resp.Error("ERR example"), resp.Array())
	if err := e.Write(value); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "*7\r\n+OK\r\n$-1\r\n$0\r\n\r\n$2\r\n\x00\n\r\n:-5\r\n-ERR example\r\n*0\r\n"
	if b.String() != want || value.Size() < b.Len() {
		t.Fatalf("encoded %q", b.String())
	}
	if err := e.Write(resp.Simple("invalid\r\n")); err == nil {
		t.Fatal("accepted newline in simple reply")
	}
}

func FuzzReadCommand(f *testing.F) {
	for _, seed := range []string{"", "PING\r\n", "*1\r\n$4\r\nPING\r\n", "*2\r\n$3\r\nSET\r\n$-1\r\n", "*1\r\n$16777217\r\n"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		d := resp.NewDecoder(bytes.NewReader(wire))
		for n := 0; n <= len(wire); n++ {
			args, err := d.ReadCommand()
			if err != nil {
				return
			}
			if len(args) == 0 || len(args) > resp.MaxArguments {
				t.Fatalf("argument count %d", len(args))
			}
			total := 0
			for _, arg := range args {
				if len(arg) > resp.MaxBulkBytes {
					t.Fatal("oversized bulk accepted")
				}
				total += len(arg)
			}
			if total > resp.MaxRequestBytes {
				t.Fatal("oversized request accepted")
			}
		}
		t.Fatal("decoder returned more successful commands than input bytes")
	})
}

func TestReadLargeBulkThenNextCommand(t *testing.T) {
	payload := bytes.Repeat([]byte{0, '\r', '\n', 255}, 25_000)
	header := fmt.Sprintf("*2\r\n$4\r\nECHO\r\n$%d\r\n", len(payload))
	wire := append([]byte(header), payload...)
	wire = append(wire, []byte("\r\nPING\r\n")...)
	d := resp.NewDecoder(bytes.NewReader(wire))
	args, err := d.ReadCommand()
	if err != nil || len(args) != 2 || string(args[0]) != "ECHO" || !bytes.Equal(args[1], payload) {
		t.Fatalf("large bulk: args=%d err=%v", len(args), err)
	}
	args, err = d.ReadCommand()
	if err != nil || len(args) != 1 || string(args[0]) != "PING" {
		t.Fatalf("pipelined command: %q, %v", args, err)
	}
	truncated := resp.NewDecoder(bytes.NewReader(wire[:len(header)+40_000]))
	if _, err := truncated.ReadCommand(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated large bulk: %v", err)
	}
}
