package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/docker/cli/cli/compose/loader"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"go.elastic.co/ecslogrus"
	"gopkg.in/yaml.v3"
)

// Build A version string that can be set with
//
//	-ldflags "-X main.Build=SOMEVERSION"
//
// at compile-time.
var Build string

const (
	appName string = "container-manager"
)

var (
	configFile string

	rootCmd = &cobra.Command{
		Use:     appName,
		Short:   "A service to start and monitor containers and attach to networks.\n\nService is started by running container-manager config.yaml",
		Run:     root,
		Version: Build,
	}
)

type Config struct {
	Interval   time.Duration `yaml:"interval"`
	Cleanup    bool          `yaml:"cleanup"`
	Containers []Container   `yaml:"containers"`
	LogLevel   string        `yaml:"logLevel"`
}

type Container struct {
	Name             string            `yaml:"name"`
	Image            string            `yaml:"image"`
	Volumes          []string          `yaml:"volumes"`
	AttachAllNetwork bool              `yaml:"attachAllNetwork"`
	IgnoredNetworks  []string          `yaml:"ignoredNetworks"`
	User             string            `yaml:"user"`
	NetworkMode      string            `yaml:"networkMode"`
	Labels           map[string]string `yaml:"labels"`
}

var ignoredNetworkNames = []string{"ingress", "host", "none"}

func init() {
	// Cobra parameters
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "i", "config.yaml", "Configuration YAML file")
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatalf("Error: %s", err.Error())
	}
}

func root(_ *cobra.Command, _ []string) {
	l := logrus.New()
	l.Out = os.Stdout
	l.Level = logrus.InfoLevel
	l.SetFormatter(&ecslogrus.Formatter{})

	_, err := os.Stat(configFile)
	if errors.Is(err, os.ErrNotExist) {
		// handle the case where the file doesn't exist
		l.Errorf("File does not exists: %s", configFile)
		os.Exit(1)
	}

	config := loadConfig(l, configFile)

	if config.LogLevel != "" {
		level, err := logrus.ParseLevel(config.LogLevel)
		if err != nil {
			l.WithError(err).Errorf("failed to parse log level: %s", config.LogLevel)
			os.Exit(1)
		}
		l.Level = level
	}

	mgmt, err := NewManager(l)
	if err != nil {
		l.Error(err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)
	signal.Notify(sigChan, syscall.SIGTERM)
	signal.Notify(sigChan, syscall.SIGINT)

	ticker := time.NewTicker(config.Interval)

	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				mgmt.run(config)
			case rawSig := <-sigChan:
				switch rawSig {
				case syscall.SIGHUP:
					l.Infof("Reloading")
					config = loadConfig(l, configFile)
					mgmt.run(config)
				case syscall.SIGINT, syscall.SIGTERM:
					sig := rawSig.String()
					l.WithField("signal", sig).Info("Caught signal, shutting down")
					if config.Cleanup {
						l.Infof("Cleaning up containers")
						mgmt.stopContainers(config)
					}
					done <- true
				}
			}
		}
	}()

	mgmt.run(config)

	<-done
}

func loadConfig(l *logrus.Logger, configFile string) *Config {
	yfile, err := os.ReadFile(configFile)
	if err != nil {
		l.Errorf("Failed to read config file: %s", configFile)
		os.Exit(1)
	}

	config := &Config{}
	err = yaml.Unmarshal(yfile, config)
	if err != nil {
		l.Errorf("Failed to parse config file: %s", configFile)
		os.Exit(1)
	}

	if config.Interval <= 0 {
		config.Interval = 5 * time.Minute
	}

	for _, c := range config.Containers {
		if c.NetworkMode == "" {
			c.NetworkMode = "default"
		}
	}

	return config
}

type manager struct {
	l   *logrus.Logger
	ctx context.Context
	cli *client.Client
}

func NewManager(l *logrus.Logger) (*manager, error) {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}

	return &manager{l, ctx, cli}, nil
}

func (m *manager) run(config *Config) {
	containerNames := lo.FilterMap(config.Containers, func(c Container, _ int) (string, bool) {
		return c.Name, c.AttachAllNetwork
	})

	// Build sorted index for deterministic IP assignment from the upper range.
	// nodeOffset ensures each swarm node picks a different IP block.
	nodeOffset := 0
	if info, err := m.cli.Info(m.ctx); err == nil && info.Swarm.NodeID != "" {
		h := fnv.New32a()
		h.Write([]byte(info.Swarm.NodeID))
		nodeOffset = int(h.Sum32()%128) * len(config.Containers)
	}
	sortedNames := lo.Map(config.Containers, func(c Container, _ int) string { return c.Name })
	slices.Sort(sortedNames)
	containerIndices := map[string]int{}
	for i, name := range sortedNames {
		containerIndices[name] = nodeOffset + i
	}

	// Returns a list of networks with more than 0 containers attached
	networkNames, err := m.getNetworkNames(containerNames)
	if err != nil {
		m.l.Error(err)
		return
	}
	m.l.WithField("networks", networkNames).Debugf("Networks: %s", networkNames)

	for _, c := range config.Containers {

		var networks []string = nil
		if c.AttachAllNetwork {
			m.l.WithField("name", c.Name).Debugf("Attaching all networks")
			networks = lo.Without(networkNames, c.IgnoredNetworks...)
		}
		networks = lo.Ternary(len(networks) > 0, networks, []string{"bridge"})

		err = m.ensureContainer(c, networks, containerIndices[c.Name])
		if err != nil {
			m.l.WithField("name", c.Name).WithError(err).Errorf("failed to enure container %s", c.Name)
		}
	}
}

func (m *manager) ensureContainer(config Container, networks []string, containerIndex int) error {
	containers, err := m.cli.ContainerList(m.ctx, container.ListOptions{All: true, Filters: filters.NewArgs(filters.Arg("name", fmt.Sprintf("^/%s$", config.Name)))})
	if err != nil {
		return err
	}

	if len(containers) > 1 {
		return fmt.Errorf("found more than one container matching %s", config.Name)
	}

	create := false
	reCreate := false
	state := ""

	if len(containers) == 0 {
		create = true
	} else {
		c := containers[0]
		networkNames := lo.Keys(c.NetworkSettings.Networks)

		if config.Image != c.Image {
			m.l.WithField("name", config.Name).Debugf("Image differ, recreate %s", config.Name)
			reCreate = true
		}

		if config.NetworkMode != c.HostConfig.NetworkMode {
			m.l.WithField("name", config.Name).Debugf("NetworkMode differ, recreate %s != %s", config.NetworkMode, c.HostConfig.NetworkMode)
			reCreate = true
		}

		m.l.WithField("name", config.Name).Debugf("Networks expected: %s", networks)
		m.l.WithField("name", config.Name).Debugf("Networks current: %s", networkNames)

		if !lo.ElementsMatch(networks, networkNames) {
			m.l.WithField("name", config.Name).Debugf("Network config differ, attach/detach %s", config.Name)

			// New networks attach
			for _, n := range networks {
				attached, err := m.cli.NetworkInspect(m.ctx, n, types.NetworkInspectOptions{})
				if err != nil {
					m.l.Error(err)
				}
				alreadyAttached := lo.SomeBy(lo.Values(attached.Containers), func(e types.EndpointResource) bool {
					return e.Name == config.Name
				})
				if !alreadyAttached {
					m.attachNetwork(n, config.Name, containerIndex)
				}
			}

			// Old networks detach
			detach, _ := lo.Difference(networkNames, networks)
			m.l.WithField("name", config.Name).Debugf("Network detach diff: %s\n", detach)
			for _, n := range detach {
				m.detachNetwork(n, config.Name)
			}
		}

		volumesExpected := make([]string, len(config.Volumes))
		for i, v := range config.Volumes {
			volume, err := loader.ParseVolume(v)
			if err != nil {
				return err
			}
			if volume.Source != "" {
				mode := lo.Ternary(volume.ReadOnly, ":ro", ":rw")
				volumesExpected[i] = fmt.Sprintf("%s:%s%s\n", volume.Source, volume.Target, mode)
			}
		}

		volumesCurrent := lo.Map(c.Mounts, func(m types.MountPoint, _ int) string {
			src := lo.Ternary(m.Name != "", m.Name, m.Source)
			mode := lo.Ternary(m.RW, ":rw", ":ro")
			return fmt.Sprintf("%s:%s%s\n", src, m.Destination, mode)
		})

		if !lo.ElementsMatch(volumesCurrent, volumesExpected) {
			m.l.WithField("name", config.Name).Debugf("Volume config differ, recreate %s", config.Name)
			m.l.WithField("name", config.Name).Debugf("Volumes expected: %s", volumesExpected)
			m.l.WithField("name", config.Name).Debugf("Volumes Current: %s", volumesCurrent)
			reCreate = true
		}

		labelsChanged := lo.SomeBy(lo.Entries(config.Labels), func(e lo.Entry[string, string]) bool {
			return c.Labels[e.Key] != e.Value
		})
		if labelsChanged {
			reCreate = true
		}

		state = c.State
	}

	if create || reCreate {
		images, err := m.cli.ImageList(m.ctx, types.ImageListOptions{
			Filters: filters.NewArgs(filters.Arg("reference", config.Image)),
		})
		if err != nil {
			return err
		}

		if len(images) == 0 {
			m.l.WithField("name", config.Name).Infof("Pulling image: %s\n", config.Image)
			out, err := m.cli.ImagePull(m.ctx, config.Image, types.ImagePullOptions{})
			if err != nil {
				return err
			}

			defer func() {
				err := out.Close()
				if err != nil {
					m.l.Errorf("Error defer out.close() response: %s", err.Error())
				}
			}()

			_, err = io.Copy(io.Discard, out)

			if err != nil {
				m.l.Errorf("Error discaring response: %s", err.Error())
			}

			m.l.WithField("name", config.Name).Infof("Pulled image: %s\n", config.Image)
		}
	}

	if state != "" && state != "running" {
		m.l.WithField("state", state).WithField("name", config.Name).Infof("Container state before creating new was: %s", state)
	}

	if reCreate {
		m.l.WithField("name", config.Name).Infof("Recreating container: %s\n", config.Name)
		m.l.WithField("name", config.Name).Infof("Stopping old container: %s\n", config.Name)
		containerID := fmt.Sprintf("/%s", config.Name)
		if state == "running" {
			timeout := int(30 * time.Second)
			err = m.cli.ContainerStop(m.ctx, containerID, container.StopOptions{Timeout: &timeout})
			if err != nil {
				return err
			}
		}
		m.l.WithField("name", config.Name).Infof("Removing old container: %s\n", config.Name)
		err = m.cli.ContainerRemove(m.ctx, containerID, container.RemoveOptions{RemoveVolumes: false, Force: true})
		if err != nil {
			return err
		}
	}

	if create || reCreate {
		m.l.WithField("name", config.Name).Infof("Creating container: %s\n", config.Name)
		c, err := m.cli.ContainerCreate(m.ctx, &container.Config{
			Image:   config.Image,
			Volumes: nil,
			User:    config.User,
			Labels:  config.Labels,
		}, &container.HostConfig{
			Binds:         config.Volumes,
			RestartPolicy: container.RestartPolicy{Name: "always"},
			NetworkMode:   container.NetworkMode(config.NetworkMode),
		}, nil, nil, config.Name)
		if err != nil {
			return err
		}

		m.l.WithField("name", config.Name).Debugf("Networks to connect: %s", networks)
		for _, n := range networks {
			m.l.WithField("name", config.Name).Debugf("Connecting %s to network %s", config.Name, n)
			endpointConfig := m.highIPEndpointConfig(n, containerIndex, config.Name)
			err = m.cli.NetworkConnect(m.ctx, n, c.ID, endpointConfig)
			if err != nil {
				m.l.WithField("name", config.Name).WithError(err).Errorf("failed to connect network %s", n)
			}
		}

	}

	c := m.getContainer(config.Name)
	if c != nil {
		if c.State != "running" {
			m.l.WithField("name", config.Name).Infof("Starting container %s with id %s\n", config.Name, c.ID)

			err = m.cli.ContainerStart(m.ctx, c.ID, container.StartOptions{})
			if err != nil {
				return err
			}

			m.l.WithField("name", config.Name).Infof("Started container %s with id %s\n", config.Name, c.ID)
		}
	}

	return nil
}

func (m *manager) getContainer(name string) *types.Container {
	containers, err := m.cli.ContainerList(m.ctx, container.ListOptions{All: true, Filters: filters.NewArgs(filters.Arg("name", fmt.Sprintf("^/%s$", name)))})
	if err != nil {
		return nil
	}

	if len(containers) == 1 {
		return &containers[0]
	}

	return nil
}

func (m *manager) stopContainers(config *Config) {
	for _, cc := range config.Containers {
		c := m.getContainer(cc.Name)

		if c != nil {
			if c.State == "running" {
				m.l.WithField("name", cc.Name).Infof("Stopping container: %s", cc.Name)
				timeout := int(30 * time.Second)
				err := m.cli.ContainerStop(m.ctx, c.ID, container.StopOptions{Timeout: &timeout})
				if err != nil {
					m.l.WithError(err).Errorf("error when stopping container")
				}
			}
			m.l.WithField("name", cc.Name).Infof("Removing container: %s", cc.Name)
			err := m.cli.ContainerRemove(m.ctx, c.ID, container.RemoveOptions{RemoveVolumes: false, Force: true})
			if err != nil {
				m.l.WithField("name", cc.Name).WithError(err).Errorf("error when removing container")
			}
		}
	}
}

func (m *manager) getNetworkNames(containerNames []string) ([]string, error) {
	allNetworks, err := m.cli.NetworkList(m.ctx, types.NetworkListOptions{})
	if err != nil {
		return nil, err
	}

	eligible := lo.Filter(allNetworks, func(n types.NetworkResource, _ int) bool {
		return (n.Attachable || n.Scope == "local") && !lo.Contains(ignoredNetworkNames, n.Name)
	})

	var names []string
	for _, n := range eligible {
		attached, err := m.cli.NetworkInspect(m.ctx, n.Name, types.NetworkInspectOptions{})
		if err != nil {
			return nil, err
		}
		hasExternalContainer := lo.SomeBy(lo.Values(attached.Containers), func(e types.EndpointResource) bool {
			return !lo.Contains(containerNames, e.Name)
		})
		if hasExternalContainer {
			names = append(names, n.Name)
		}
	}

	slices.Sort(names)
	return names, nil
}

// detach from network from container
func (m *manager) detachNetwork(network string, container string) {
	err := m.cli.NetworkDisconnect(m.ctx, network, container, true)

	m.l.Debugf("Detaching %s from network %s\n", container, network)

	if err != nil {
		m.l.Error(err)
	}
}

// attach from network from container
func (m *manager) attachNetwork(networkName string, containerName string, containerIndex int) {
	endpointConfig := m.highIPEndpointConfig(networkName, containerIndex, containerName)
	err := m.cli.NetworkConnect(m.ctx, networkName, containerName, endpointConfig)
	if err != nil {
		m.l.Error(err)
	}
	m.l.Debugf("Attaching %s to network %s\n", containerName, networkName)
}

// highIPEndpointConfig returns EndpointSettings with an IP from the upper end of
// the network's subnet. containerIndex determines the offset from the top, so
// multiple containers on the same node get distinct IPs without racing.
// Returns nil (Docker picks IP) if the network can't be inspected or has no IPAM config.
func (m *manager) highIPEndpointConfig(networkName string, containerIndex int, containerName string) *network.EndpointSettings {
	networkInfo, err := m.cli.NetworkInspect(m.ctx, networkName, types.NetworkInspectOptions{})
	if err != nil {
		m.l.WithField("name", containerName).WithError(err).Warnf("could not inspect network %s, letting Docker assign IP", networkName)
		return nil
	}

	if networkInfo.Options["com.docker.network.bridge.default_bridge"] == "true" {
		return nil
	}

	if len(networkInfo.IPAM.Config) == 0 {
		m.l.WithField("name", containerName).Warnf("no IPAM config for network %s, letting Docker assign IP", networkName)
		return nil
	}

	_, ipNet, err := net.ParseCIDR(networkInfo.IPAM.Config[0].Subnet)
	if err != nil {
		m.l.WithField("name", containerName).WithError(err).Warnf("could not parse subnet for network %s, letting Docker assign IP", networkName)
		return nil
	}

	usedIPs := lo.FilterMap(lo.Values(networkInfo.Containers), func(e types.EndpointResource, _ int) (string, bool) {
		ip, _, err := net.ParseCIDR(e.IPv4Address)
		if err != nil {
			return "", false
		}
		return ip.String(), true
	})
	if gw := networkInfo.IPAM.Config[0].Gateway; gw != "" {
		usedIPs = append(usedIPs, gw)
	}

	// Walk from broadcast-1 downward, picking the containerIndex-th free IP
	broadcast := broadcastAddr(ipNet)
	candidate := prevIP(broadcast)
	skipped := 0
	for ; ipNet.Contains(candidate) && !candidate.Equal(ipNet.IP); candidate = prevIP(candidate) {
		if lo.Contains(usedIPs, candidate.String()) {
			continue
		}
		if skipped == containerIndex {
			m.l.WithField("name", containerName).Infof("Assigning IP %s on network %s", candidate, networkName)
			return &network.EndpointSettings{
				IPAMConfig: &network.EndpointIPAMConfig{
					IPv4Address: candidate.String(),
				},
			}
		}
		skipped++
	}

	m.l.WithField("name", containerName).Warnf("no available IP in upper range of network %s, letting Docker assign", networkName)
	return nil
}

func broadcastAddr(n *net.IPNet) net.IP {
	ip := n.IP.To4()
	if ip == nil {
		ip = n.IP.To16()
	}
	broadcast := make(net.IP, len(ip))
	for i := range ip {
		broadcast[i] = ip[i] | ^n.Mask[i]
	}
	return broadcast
}

func prevIP(ip net.IP) net.IP {
	prev := make(net.IP, len(ip))
	copy(prev, ip)
	for i := len(prev) - 1; i >= 0; i-- {
		prev[i]--
		if prev[i] != 255 {
			break
		}
	}
	return prev
}
