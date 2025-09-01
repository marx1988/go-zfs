package zfs

import (
	"encoding/json"
	"fmt"
	"os/exec"
)

// ZFS zpool states, which can indicate if a pool is online, offline, degraded, etc.
//
// More information regarding zpool states can be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zpoolconcepts.7.html#Device_Failure_and_Recovery
const (
	ZpoolOnline   = "ONLINE"
	ZpoolDegraded = "DEGRADED"
	ZpoolFaulted  = "FAULTED"
	ZpoolOffline  = "OFFLINE"
	ZpoolUnavail  = "UNAVAIL"
	ZpoolRemoved  = "REMOVED"
)

// Zpool is a ZFS zpool.
// A pool is a top-level structure in ZFS, and can contain many descendent datasets.
type Zpool struct {
	Name          string
	Health        string
	Allocated     uint64
	Size          uint64
	Free          uint64
	Fragmentation uint64
	ReadOnly      bool
	Freeing       uint64
	Leaked        uint64
	DedupRatio    float64
}

// zpool is a helper function to wrap typical calls to zpool and ignores stdout.
func zpool(arg ...string) error {
	_, err := zpoolOutput(arg...)
	return err
}

// zpool is a helper function to wrap typical calls to zpool.
func zpoolOutput(arg ...string) ([][]string, error) {
	c := command{Command: "zpool"}
	return c.Run(arg...)
}

// GetZpool retrieves a single ZFS zpool by name.
func GetZpool(name string) (*Zpool, error) {
	args := zpoolArgs
	args = append(args, name)
	out, err := zpoolOutput(args...)
	if err != nil {
		return nil, err
	}

	z := &Zpool{Name: name}
	for _, line := range out {
		if err := z.parseLine(line); err != nil {
			return nil, err
		}
	}

	return z, nil
}

// Datasets returns a slice of all ZFS datasets in a zpool.
func (z *Zpool) Datasets() ([]*Dataset, error) {
	return Datasets(z.Name)
}

// Snapshots returns a slice of all ZFS snapshots in a zpool.
func (z *Zpool) Snapshots() ([]*Dataset, error) {
	return Snapshots(z.Name)
}

// CreateZpool creates a new ZFS zpool with the specified name, properties, and optional arguments.
//
// A full list of available ZFS properties and command-line arguments may be found in the ZFS manual:
// https://openzfs.github.io/openzfs-docs/man/7/zfsprops.7.html.
// https://openzfs.github.io/openzfs-docs/man/8/zpool-create.8.html
func CreateZpool(name string, properties map[string]string, args ...string) (*Zpool, error) {
	cli := make([]string, 1, 4)
	cli[0] = "create"
	if properties != nil {
		cli = append(cli, propsSlice(properties)...)
	}
	cli = append(cli, name)
	cli = append(cli, args...)
	if err := zpool(cli...); err != nil {
		return nil, err
	}

	return &Zpool{Name: name}, nil
}

// Destroy destroys a ZFS zpool by name.
func (z *Zpool) Destroy() error {
	err := zpool("destroy", z.Name)
	return err
}

// ZpoolVdev represents a vdev (virtual device) in a ZFS pool
type ZpoolVdev struct {
	Name           string                `json:"name"`
	VdevType       string                `json:"vdev_type"`
	GUID           string                `json:"guid"`
	Class          string                `json:"class"`
	State          string                `json:"state"`
	Path           string                `json:"path,omitempty"`
	PhysPath       string                `json:"phys_path,omitempty"`
	DevID          string                `json:"devid,omitempty"`
	AllocSpace     string                `json:"alloc_space,omitempty"`
	TotalSpace     string                `json:"total_space,omitempty"`
	DefSpace       string                `json:"def_space,omitempty"`
	RepDevSize     string                `json:"rep_dev_size,omitempty"`
	PhysSpace      string                `json:"phys_space,omitempty"`
	ReadErrors     string                `json:"read_errors"`
	WriteErrors    string                `json:"write_errors"`
	ChecksumErrors string                `json:"checksum_errors"`
	SlowIOs        string                `json:"slow_ios,omitempty"`
	Vdevs          map[string]*ZpoolVdev `json:"vdevs,omitempty"`
}

// ZpoolStatus represents the status information of a ZFS pool
type ZpoolStatus struct {
	Name       string                `json:"name"`
	State      string                `json:"state"`
	PoolGUID   string                `json:"pool_guid"`
	TXG        string                `json:"txg"`
	SPAVersion string                `json:"spa_version"`
	ZPLVersion string                `json:"zpl_version"`
	Vdevs      map[string]*ZpoolVdev `json:"vdevs"`
	ErrorCount string                `json:"error_count"`
}

// ZpoolStatusJSON represents the JSON output structure from 'zpool status --json'
type ZpoolStatusJSON struct {
	OutputVersion OutputVersion           `json:"output_version"`
	Pools         map[string]*ZpoolStatus `json:"pools"`
}

// OutputVersion represents version information in JSON output
type OutputVersion struct {
	Command   string `json:"command"`
	VersMajor int    `json:"vers_major"`
	VersMinor int    `json:"vers_minor"`
}

// ListZpools list all ZFS zpools accessible on the current system.
func ListZpools() ([]*Zpool, error) {
	args := []string{"list", "-Ho", "name"}
	out, err := zpoolOutput(args...)
	if err != nil {
		return nil, err
	}

	var pools []*Zpool

	for _, line := range out {
		z, err := GetZpool(line[0])
		if err != nil {
			return nil, err
		}
		pools = append(pools, z)
	}
	return pools, nil
}

// Status retrieves the status information of a ZFS pool using 'zpool status'
// Uses default parsable=false to show exact bytes without unit conversion
func (z *Zpool) Status() (*ZpoolStatus, error) {
	return GetZpoolStatus(z.Name, false)
}

// GetZpoolStatus retrieves the status information of a ZFS pool by name using JSON format
// parsable controls whether to show exact byte values (true) or human-readable units (false)
// When parsable is false (default), uses -p flag to show exact bytes without unit conversion
func GetZpoolStatus(name string, parsable bool) (*ZpoolStatus, error) {
	args := []string{"status", "--json"}
	if !parsable {
		args = append(args, "-p")
	}
	args = append(args, name)
	cmd := exec.Command("zpool", args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var jsonStatus ZpoolStatusJSON
	if err := json.Unmarshal(output, &jsonStatus); err != nil {
		return nil, err
	}

	status, exists := jsonStatus.Pools[name]
	if !exists {
		return nil, fmt.Errorf("pool %s not found in status output", name)
	}

	return status, nil
}

// ListPoolStatus retrieves the status information for all ZFS pools using JSON format
// parsable controls whether to show exact byte values (true) or human-readable units (false)
// When parsable is false (default), uses -p flag to show exact bytes without unit conversion
func ListPoolStatus(parsable bool) ([]*ZpoolStatus, error) {
	args := []string{"status", "--json"}
	if !parsable {
		args = append(args, "-p")
	}
	cmd := exec.Command("zpool", args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var jsonStatus ZpoolStatusJSON
	if err := json.Unmarshal(output, &jsonStatus); err != nil {
		return nil, err
	}

	pools := make([]*ZpoolStatus, 0, len(jsonStatus.Pools))
	for _, status := range jsonStatus.Pools {
		pools = append(pools, status)
	}

	return pools, nil
}

