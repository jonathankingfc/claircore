// Code generated by "stringer -type=ArchOp -linecomment"; DO NOT EDIT.

package claircore

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[OpEquals-1]
	_ = x[OpNotEquals-2]
}

const _ArchOp_name = "equalsnot equals"

var _ArchOp_index = [...]uint8{0, 6, 16}

func (i ArchOp) String() string {
	i -= 1
	if i >= ArchOp(len(_ArchOp_index)-1) {
		return "ArchOp(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _ArchOp_name[_ArchOp_index[i]:_ArchOp_index[i+1]]
}
