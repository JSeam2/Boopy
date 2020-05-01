package boopy

import (
	"testing"
	"time"
)

func Test_bytesEqual(t *testing.T) {
	type args struct {
		a []byte
		b []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"Standard", args{[]byte{1, 0, 0, 1}, []byte{1, 0, 0, 1}}, true},
		{"Empty", args{[]byte{}, []byte{}}, true},
		{"Middle1", args{[]byte{1, 1, 0, 1}, []byte{1, 0, 0, 1}}, false},
		{"Middle2", args{[]byte{1, 0, 1, 1}, []byte{1, 0, 0, 1}}, false},
		{"End", args{[]byte{0, 0, 0, 0}, []byte{0, 0, 0, 1}}, false},
		{"One", args{[]byte{1}, []byte{1}}, true},
		{"Larges", args{[]byte{127, 0, 0, 1}, []byte{255, 0, 0, 1}}, false},
		{"Garbage", args{[]byte{15, 25, 98, 54}, []byte{15, 25, 98, 54}}, true},
		{"Garbage2", args{[]byte{15, 26, 72, 54}, []byte{15, 26, 71, 64}}, false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bytesEqual(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("bytesEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isPowerOfTwo(t *testing.T) {
	type args struct {
		num int
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"1", args{1}, true},
		{"2", args{2}, true},
		{"0", args{0}, false},
		{"16", args{16}, true},
		{"-1", args{-1}, false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPowerOfTwo(tt.args.num); got != tt.want {
				t.Errorf("isPowerOfTwo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_randStabilize(t *testing.T) {
	type args struct {
		min time.Duration
		max time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{"<1 sec", args{min: (0 * time.Millisecond), max: (1000 * time.Millisecond)}, (1000 * time.Millisecond)},
		{"1 sec", args{min: (1 * time.Second), max: (1 * time.Second)}, (1000 * time.Millisecond)},
		// TODO: Add MORE test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := randStabilize(tt.args.min, tt.args.max); got > tt.want {
				t.Errorf("randStabilize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keyBetwIncludeRight(t *testing.T) {
	t.Parallel()

	type args struct {
		key []byte
		a   []byte
		b   []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"1", args{[]byte{1, 0, 0, 0}, []byte{0, 0, 0, 0}, []byte{1, 0, 0, 0}}, true},
		{"2", args{[]byte{1, 1, 1, 1}, []byte{1, 1, 1, 0}, []byte{1, 1, 1, 1}}, true},
		{"3", args{[]byte{1, 1, 1, 1, 1}, []byte{0}, []byte{1, 1, 1, 1}}, false},
		{"4", args{[]byte{1, 1, 1, 1, 1}, []byte{0}, []byte{1, 1, 1, 1, 1, 1}}, true},
		{
			"5",
			args{
				[]byte{4, 40, 171},
				[]byte{53, 106, 25, 43, 121, 19, 176, 76, 84, 87, 77, 24, 194, 141, 70, 230, 57, 84, 40, 171},
				[]byte{4, 40, 171},
			},
			true,
		},
		{"6", args{GetHashID("11"), GetHashID("1"), GetHashID("20")}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := keyBetwIncludeRight(tt.args.key, tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("keyBetwIncludeRight() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_between(t *testing.T) {
	type args struct {
		key []byte
		a   []byte
		b   []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"1", args{[]byte{1, 0, 0, 0}, []byte{0, 0, 0, 0}, []byte{1, 0, 0, 0}}, true},
		{"2", args{[]byte{1, 1, 1, 0}, []byte{1, 1, 0, 0}, []byte{1, 1, 1, 1}}, true},
		{"3", args{[]byte{1, 1, 1, 1, 1}, []byte{0}, []byte{1, 1, 1, 1}}, false},
		{"4", args{[]byte{1, 1, 1, 1, 1}, []byte{0}, []byte{1, 1, 1, 1, 1, 1}}, true},
		{"5", args{[]byte{0, 0, 0, 0}, []byte{0, 0, 0, 0}, []byte{1, 0, 0, 0}}, false},
		{"6", args{GetHashID("11"), GetHashID("1"), GetHashID("20")}, false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := between(tt.args.key, tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("between() = %v, want %v", got, tt.want)
			}
		})
	}
}
