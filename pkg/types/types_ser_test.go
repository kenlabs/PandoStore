package types

import (
	"encoding/json"
	"github.com/ipfs/go-cid"
	"github.com/kenlabs/pando-store/pkg/types/cbortypes"
	"testing"
)

func TestTypeSerlialization(t *testing.T) {
	snap := &cbortypes.SnapShot{
		Update:       map[string]*cbortypes.Metalist{"acdesf": {MetaList: []cid.Cid{cid.Undef, cid.Undef, cid.Undef}}},
		StateRoot:    cid.Cid{},
		Height:       0,
		CreateTime:   0,
		PrevSnapShot: "",
	}

	res, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%s", res)
	t.Logf("%v", res)

	var unres interface{}
	err = json.Unmarshal(res, &unres)
	if err != nil {
		t.Fatal(err)
	}

	b := struct {
		Code int
		Msg  string
		Data interface{} `json:"DD"`
	}{Code: 200,
		Msg:  "hello",
		Data: unres}

	out, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", out)
	t.Logf("%v", b)
}
