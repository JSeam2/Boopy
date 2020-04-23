package boopy

import (
	// "crypto/sha1"
	// "fmt"
	// "math/big"
	"reflect"
	"testing"

	"github.com/jseam2/boopy/api"
)

func Test_NewFingerTable(t *testing.T) {
	type args struct {
		node *api.Node
		m    int
	}
	tests := []struct {
		name string
		args args
		want fingerTable
	}{
		// TODO: Add test cases.
		// {"1", args{NewInode("8", "0.0.0.0:8083"), 1}, fingerTable},
		{
			"standard", args{NewInode("1", "0.0.0.0:8001"), 2}, []*fingerEntry{
				&fingerEntry{
					Id:   fingerID(NewInode("1", "0.0.0.0:8001").Id, 0, 2),
					Node: NewInode("1", "0.0.0.0:8001"),
				},
				&fingerEntry{
					Id:   fingerID(NewInode("1", "0.0.0.0:8001").Id, 1, 2),
					Node: NewInode("1", "0.0.0.0:8001"),
				},
			},
		},

		{
			"standard3", args{NewInode("1", "0.0.0.0:8002"), 3}, []*fingerEntry{
				&fingerEntry{
					Id:   fingerID(NewInode("1", "0.0.0.0:8002").Id, 0, 3),
					Node: NewInode("1", "0.0.0.0:8002"),
				},
				&fingerEntry{
					Id:   fingerID(NewInode("1", "0.0.0.0:8002").Id, 1, 3),
					Node: NewInode("1", "0.0.0.0:8002"),
				},
				&fingerEntry{
					Id:   fingerID(NewInode("1", "0.0.0.0:8002").Id, 2, 3),
					Node: NewInode("1", "0.0.0.0:8002"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newFingerTable(tt.args.node, tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newFingerTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_NewFingerEntry(t *testing.T) {
	type args struct {
		id   []byte
		node *api.Node
	}
	tests := []struct {
		name string
		args args
		want *fingerEntry
	}{
		// TODO: Add test cases.
		{
			"standard",
			args{[]byte{0, 0, 0}, NewInode("1", "0.0.0.0:8001")},
			&fingerEntry{Id: []byte{0, 0, 0}, Node: NewInode("1", "0.0.0.0:8001")},
		},

		{
			"standard1",
			args{[]byte{0, 0, 1}, NewInode("1", "0.0.0.0:8001")},
			&fingerEntry{Id: []byte{0, 0, 1}, Node: NewInode("1", "0.0.0.0:8001")},
		},

		{
			"standard2",
			args{[]byte{0, 0, 1}, NewInode("1", "0.0.0.0:8002")},
			&fingerEntry{Id: []byte{0, 0, 1}, Node: NewInode("1", "0.0.0.0:8002")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newFingerEntry(tt.args.id, tt.args.node); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newFingerEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_FingerID(t *testing.T) {
	type args struct {
		n []byte
		i int
		m int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
		{
			"standard",
			args{[]byte{1}, 2, 3},
			[]byte{5},
		},

		{
			"standard1",
			args{[]byte{2}, 2, 3},
			[]byte{6},
		},

		{
			"standard1",
			args{[]byte{1, 1}, 2, 3},
			[]byte{5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fingerID(tt.args.n, tt.args.i, tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fingerID() = %v, want %v", got, tt.want)
			}
		})
	}
}
