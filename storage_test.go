package boopy

import (
	"crypto/sha1"
	"hash"
	"reflect"
	"testing"

	"github.com/jseam2/boopy/api"
)

func TestNewMapStore(t *testing.T) {
	type args struct {
		hashFunc func() hash.Hash
	}
	tests := []struct {
		name string
		args args
		want Storage
	}{
		//TODO: MORE TESTS
		// {"1", args{hashFunc: sha1.New}, },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMapStore(tt.args.hashFunc); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMapStore() = %v, want %v", got, tt.want)
			}
		})
	}
}
func shaSum(str string) []byte {
	h := sha1.New()
	h.Write([]byte(str))
	return h.Sum(nil)
}

func Test_mapStore_hashKey(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{"exists",
			fields{
				data: map[string]string{},
				Hash: sha1.New},
			args{key: "key1"},
			shaSum("key1"),
			false},
		{"nil",
			fields{
				data: map[string]string{},
				Hash: sha1.New},
			args{key: ""},
			shaSum(""),
			false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			got, err := a.hashKey(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("mapStore.hashKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapStore.hashKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapStore_Get(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// FYI: byte conversion for []byte(string)
		// converts each CHARACTER to its ASCII number representation
		// It doesn't convert the string to a number first.
		// i.e. 257 -> "2" + "5" + "7" not []byte{1,1}
		{"exists1",
			fields{map[string]string{"key1": "1", "key2": "2"}, nil},
			args{"key1"}, []byte("1"),
			false},
		{"exists2",
			fields{map[string]string{"key1": "1", "key2": "257"}, nil},
			args{"key2"}, []byte("257"),
			false},
		{"Not exist",
			fields{map[string]string{"key2": "2"}, nil},
			args{"key1"}, nil,
			true},
		{"Call on empty table",
			fields{make(map[string]string), nil},
			args{"key1"}, nil,
			true},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			got, err := a.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("mapStore.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapStore.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapStore_Set(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
	}
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"New value",
			fields{map[string]string{"key1": "1", "key2": "2"}, nil},
			args{"key3", "3"},
			false},
		{"Replace keys",
			fields{map[string]string{"key1": "1", "key2": "2"}, nil},
			args{"key2", "3"},
			false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			if err := a.Set(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("mapStore.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_mapStore_Delete(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"doesnt exist1",
			fields{map[string]string{"key1": "1", "key2": "2"}, nil},
			args{"key3"},
			false},
		{"doesnt exist2",
			fields{map[string]string{}, nil},
			args{"key3"},
			false},
		{"exist",
			fields{map[string]string{"key1": "1", "key2": "2"}, nil},
			args{"key2"},
			false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			if err := a.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("mapStore.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_mapStore_Between(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
	}
	type args struct {
		from []byte
		to   []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*api.KV
		wantErr bool
	}{

		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			got, err := a.Between(tt.args.from, tt.args.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("mapStore.Between() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mapStore.Between() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapStore_MDelete(t *testing.T) {
	type fields struct {
		data map[string]string
		Hash func() hash.Hash
	}
	type args struct {
		keys []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"single del",
			fields{map[string]string{"key1": "1", "key2": "2"}, nil},
			args{[]string{"key2"}},
			false},
		{"single del",
			fields{map[string]string{"key1": "1", "key2": "2", "key3": "3"}, nil},
			args{[]string{"key1", "key2"}},
			false},
		{"doesnt exist 1",
			fields{map[string]string{"key1": "1", "key2": "2"}, nil},
			args{[]string{"key3"}},
			false},
		{"doesnt exist 2",
			fields{map[string]string{}, nil},
			args{[]string{"key2", "key3"}},
			false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &mapStore{
				data: tt.fields.data,
				Hash: tt.fields.Hash,
			}
			if err := a.MDelete(tt.args.keys...); (err != nil) != tt.wantErr {
				t.Errorf("mapStore.MDelete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
