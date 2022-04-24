package cbortypes

import "testing"

func TestSnapShot(t *testing.T) {
	c := &SnapShot{
		Update:       nil,
		Height:       0,
		CreateTime:   0,
		PrevSnapShot: "",
	}
	t.Log(c)
}
