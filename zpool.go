package zfs

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
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
	// Convenience fields for backward compatibility and easier access
	ReadErrs    uint64
	WriteErrs   uint64
	CksumErrs   uint64
	SlowIOCount uint64
	Children    []*ZpoolVdev
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
	// Convenience fields for backward compatibility
	Pool   string
	Status string
	Action string
	See    string
	Scrub  string
	Config *ZpoolVdev
	Errors string
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
func (z *Zpool) Status() (*ZpoolStatus, error) {
	return GetZpoolStatus(z.Name)
}

// GetZpoolStatus retrieves the status information of a ZFS pool by name using JSON format
func GetZpoolStatus(name string) (*ZpoolStatus, error) {
	cmd := exec.Command("zpool", "status", "--json", name)
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

	// Populate convenience fields for backward compatibility
	status.Pool = status.Name
	if status.ErrorCount == "0" {
		status.Errors = "No known data errors"
	} else {
		status.Errors = status.ErrorCount + " data errors"
	}

	// Set up root vdev as Config for backward compatibility
	if rootVdev, exists := status.Vdevs[name]; exists {
		status.Config = rootVdev
		populateVdevConvenienceFields(rootVdev)
	}

	return status, nil
}

// populateVdevConvenienceFields recursively populates convenience fields for backward compatibility
func populateVdevConvenienceFields(vdev *ZpoolVdev) {
	// Convert error counts from string to uint64 for backward compatibility
	vdev.ReadErrs, _ = parseErrorCount(vdev.ReadErrors)
	vdev.WriteErrs, _ = parseErrorCount(vdev.WriteErrors)
	vdev.CksumErrs, _ = parseErrorCount(vdev.ChecksumErrors)
	vdev.SlowIOCount, _ = parseErrorCount(vdev.SlowIOs)

	// Convert map to slice for Children field (backward compatibility)
	vdev.Children = make([]*ZpoolVdev, 0, len(vdev.Vdevs))
	for _, child := range vdev.Vdevs {
		populateVdevConvenienceFields(child)
		vdev.Children = append(vdev.Children, child)
	}
}

// parseErrorCount converts error count string to uint64
func parseErrorCount(errorStr string) (uint64, error) {
	if errorStr == "" || errorStr == "-" {
		return 0, nil
	}
	return strconv.ParseUint(errorStr, 10, 64)
}
