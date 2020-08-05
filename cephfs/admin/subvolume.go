package admin

// this is the internal type used to create JSON for ceph.
// See SubVolumeOptions for the type that users of the library
// interact with.
// note that the ceph json takes mode as a string.
type subVolumeFields struct {
	Prefix            string    `json:"prefix"`
	Format            string    `json:"format"`
	VolName           string    `json:"vol_name"`
	GroupName         string    `json:"group_name,omitempty"`
	SubName           string    `json:"sub_name"`
	Size              ByteCount `json:"size,omitempty"`
	Uid               int       `json:"uid,omitempty"`
	Gid               int       `json:"gid,omitempty"`
	Mode              string    `json:"mode,omitempty"`
	PoolLayout        string    `json:"pool_layout,omitempty"`
	NamespaceIsolated bool      `json:"namespace_isolated"`
}

// SubVolumeOptions are used to specify optional, non-identifying, values
// to be used when creating a new subvolume.
type SubVolumeOptions struct {
	Size              ByteCount
	Uid               int
	Gid               int
	Mode              int
	PoolLayout        string
	NamespaceIsolated bool
}

func (s *SubVolumeOptions) toFields(v, g, n string) *subVolumeFields {
	return &subVolumeFields{
		Prefix:            "fs subvolume create",
		Format:            "json",
		VolName:           v,
		GroupName:         g,
		SubName:           n,
		Size:              s.Size,
		Uid:               s.Uid,
		Gid:               s.Gid,
		Mode:              modeString(s.Mode, false),
		PoolLayout:        s.PoolLayout,
		NamespaceIsolated: s.NamespaceIsolated,
	}
}

// NoGroup should be used when an optional subvolume group name is not
// specified.
const NoGroup = ""

// CreateSubVolume sends a request to create a CephFS subvolume in a volume,
// belonging to an optional subvolume group.
func (fsa *FSAdmin) CreateSubVolume(volume, group, name string, o *SubVolumeOptions) error {
	if o == nil {
		o = &SubVolumeOptions{}
	}
	f := o.toFields(volume, group, name)
	return checkEmptyResponseExpected(fsa.marshalMgrCommand(f))
}

// command:
//   fs subvolume ls <vol_name> <group_name>

// ListSubVolumes returns a list of subvolumes belonging to the volume and
// optional subvolume group.
func (fsa *FSAdmin) ListSubVolumes(volume, group string) ([]string, error) {
	m := map[string]string{
		"prefix":   "fs subvolume ls",
		"vol_name": volume,
		"format":   "json",
	}
	if group != NoGroup {
		m["group_name"] = group
	}
	return parseListNames(fsa.marshalMgrCommand(m))
}

// command:
//   fs subvolume rm <vol_name> <sub_name> <group_name> <force>

// RemoveSubVolume will delete a CephFS subvolume in a volume and optional
// subvolume group.
func (fsa *FSAdmin) RemoveSubVolume(volume, group, name string) error {
	m := map[string]string{
		"prefix":   "fs subvolume rm",
		"vol_name": volume,
		"sub_name": name,
		"format":   "json",
	}
	if group != NoGroup {
		m["group_name"] = group
	}
	return checkEmptyResponseExpected(fsa.marshalMgrCommand(m))
}