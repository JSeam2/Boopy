package boopy

import (
	"hash"
	"reflect"
	"testing"
	"crypto/sha1"

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
		fields{data: map[string]string{"key1":"1", "key2":"2"}, Hash: sha1.New},
	}
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
<<<<<<< HEAD
		{"exists1", fields{map[string]string{"key1": "1", "key2": "2"}, nil}, args{"key1"},[]byte{1}, nil},
		{"exists2", fields{map[string]string{"key1": "1", "key2": "257"}, nil}, args{"key2"},[]byte{1,1}, nil},
		{"doesnt exist", fields{map[string]string{"key2": "2"}, nil}, args{"key1"},nil, ERR_KEY_NOT_FOUND},
=======
		// {"exists",
		// map[string]string{"key1":"1", "key2":"2"},
		// args{"key1"},
		// []byte{}},
>>>>>>> b93cb08b45c39e795a06f158a979604a812fe272
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
		{"1", fields{map[string]string{"key1": "1", "key2": "2"}, nil}, args{"key3", "3"}, nil},
		{"2", fields{map[string]string{"key1": "1", "key2": "2"}, nil}, args{"key2", "3"}, nil},
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
		{"doesnt exist1", fields{map[string]string{"key1": "1", "key2": "2"}, nil}, args{"key3"}, nil},
		{"doesnt exist2", fields{map[string]string{}, nil}, args{"key3"}, nil},
		{"exist", fields{map[string]string{"key1": "1", "key2": "2"}, nil}, args{"key2"}, nil},
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
		{"single del", fields{map[string]string{"key1": "1", "key2": "2"}, nil}, args{"key2"}, nil},
		{"single del", fields{map[string]string{"key1": "1", "key2": "2", "key3":"3"}, nil}, args{"key1", "key2"}, nil},
		{"doesnt exist 1", fields{map[string]string{"key1": "1", "key2": "2"}, nil}, args{"key3"}, nil},
		{"doesnt exist2", fields{map[string]string{}, nil}, args{"key2","key3"}, nil},
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
