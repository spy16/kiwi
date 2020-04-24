package index_test

import (
	"reflect"
	"testing"

	"github.com/spy16/kiwi/index"
)

func Test_PackKV(t *testing.T) {
	k, v := []byte("hello"), []byte{0xA, 0xB, 0xC, 0xD}
	got := index.PackKV(k, v)
	want := append(k, v...)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PackKV() want=%#v got=%#v", want, got)
	}
}

func Test_UnpackKV(t *testing.T) {
	blob := []byte{'h', 'e', 'l', 'l', 'o', 0xA, 0xB, 0xC, 0xD}
	wantK, wantV := []byte("hello"), []byte{0xA, 0xB, 0xC, 0xD}

	gotK, gotV := index.UnpackKV(blob, 5)

	t.Logf("blob=%#v", blob)

	if !reflect.DeepEqual(gotK, wantK) {
		t.Errorf("UnpackKV() wantK=%#v gotK=%#v", wantK, gotK)
	}

	if !reflect.DeepEqual(gotV, wantV) {
		t.Errorf("UnpackKV() wantV=%#v gotV=%#v", wantV, gotV)
	}
}
