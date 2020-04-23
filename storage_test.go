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
	// AllFields := fields{data: map[string]string{"key0": "0", "key1": "1", "key2": "2", "key3": "3", "key4": "4", "key5": "5", "key6": "6", "key7": "7", "key8": "8", "key9": "9", "key10": "10", "key11": "11", "key12": "12", "key13": "13", "key14": "14", "key15": "15", "key16": "16", "key17": "17", "key18": "18", "key19": "19", "key20": "20", "key21": "21", "key22": "22", "key23": "23", "key24": "24", "key25": "25", "key26": "26", "key27": "27", "key28": "28", "key29": "29", "key30": "30", "key31": "31", "key32": "32", "key33": "33", "key34": "34", "key35": "35", "key36": "36", "key37": "37", "key38": "38", "key39": "39", "key40": "40", "key41": "41", "key42": "42", "key43": "43", "key44": "44", "key45": "45", "key46": "46", "key47": "47", "key48": "48", "key49": "49", "key50": "50", "key51": "51", "key52": "52", "key53": "53", "key54": "54", "key55": "55", "key56": "56", "key57": "57", "key58": "58", "key59": "59", "key60": "60", "key61": "61", "key62": "62", "key63": "63", "key64": "64", "key65": "65", "key66": "66", "key67": "67", "key68": "68", "key69": "69", "key70": "70", "key71": "71", "key72": "72", "key73": "73", "key74": "74", "key75": "75", "key76": "76", "key77": "77", "key78": "78", "key79": "79", "key80": "80", "key81": "81", "key82": "82", "key83": "83", "key84": "84", "key85": "85", "key86": "86", "key87": "87", "key88": "88", "key89": "89", "key90": "90", "key91": "91", "key92": "92", "key93": "93", "key94": "94", "key95": "95", "key96": "96", "key97": "97", "key98": "98", "key99": "99"},
	// 	Hash: sha1.New}
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
		{"No Match",
			fields{data: map[string]string{},
				Hash: sha1.New},
			args{from: []byte{181, 87, 158, 197, 170, 12, 199, 43, 240, 31, 152, 208, 193, 211, 9, 184, 147, 231, 64, 11},
				to: []byte{182, 88, 159, 198, 171, 13, 200, 44, 241, 32, 153, 209, 194, 212, 10, 185, 148, 232, 65, 12}},
			[]*api.KV{},
			false,
		},
		{"Exact Match",
			fields{data: map[string]string{"key1": "1"},
				Hash: sha1.New},
			args{from: []byte{181, 87, 158, 197, 170, 12, 199, 43, 240, 31, 152, 208, 193, 211, 9, 184, 147, 231, 64, 11},
				to: []byte{182, 88, 159, 198, 171, 13, 200, 44, 241, 32, 153, 209, 194, 212, 10, 185, 148, 232, 65, 12}},
			[]*api.KV{},
			// I'm not actually entirely sure this works correctly.
			false,
		},
		// 	{"IsBetween",
		// 	fields
		// },
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
