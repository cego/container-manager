package main

import (
	"context"
	"fmt"
	"github.com/docker/cli/cli/compose/loader"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"syscall"
	"time"
)

type Config struct {
	Interval   time.Duration `yaml:"interval"`
	Containers []Container   `yaml:"containers"`
}

type Container struct {
	Name             string   `yaml:"name"`
	Image            string   `yaml:"image"`
	Volumes          []string `yaml:"volumes"`
	AttachAllNetwork bool     `yaml:"attachAllNetwork"`
	IgnoredNetworks  []string `yaml:"ignoredNetworks"`
}

var ignoredNetworkNames = []string{"ingress", "host", "none"}

func main() {
	l := logrus.New()
	l.Out = os.Stdout
	l.Level = logrus.DebugLevel

	configFile := "config.yaml"
	if len(os.Args) >= 2 {
		configFile = os.Args[1]
	}

	config := loadConfig(l, configFile)

	mgmt, err := NewManager(l)
	if err != nil {
		l.Error(err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal)
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
			case _ = <-ticker.C:
				mgmt.run(config)
			case rawSig := <-sigChan:
				if rawSig == syscall.SIGHUP {
					l.Infof("Reloading")
					config = loadConfig(l, configFile)
					mgmt.run(config)
				} else if rawSig == syscall.SIGINT || rawSig == syscall.SIGTERM {
					sig := rawSig.String()
					l.WithField("signal", sig).Info("Caught signal, shutting down")
					mgmt.stopContainers(config)
					done <- true
				}
			}
		}
	}()

	mgmt.run(config)

	<-done
}

func loadConfig(l *logrus.Logger, configFile string) *Config {
	yfile, err := ioutil.ReadFile(configFile)
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
	networkNames, err := m.getNetworkNames()
	if err != nil {
		m.l.Error(err)
		return
	}

	for _, c := range config.Containers {

		var networks []string = nil
		if c.AttachAllNetwork {
			copy(networks, networkNames)
			networks = remove(networks, c.IgnoredNetworks)
		}
		if len(networks) == 0 {
			networks = append(networks, "bridge")
		}

		err = m.ensureContainer(c.Name, c.Image, networks, c.Volumes)
		if err != nil {
			m.l.WithError(err).Errorf("failed to enure container %s", c.Name)
		}
	}
}

func (m *manager) ensureContainer(name, image string, networks, volumes []string) error {
	containers, err := m.cli.ContainerList(m.ctx, types.ContainerListOptions{All: true, Filters: filters.NewArgs(filters.Arg("name", fmt.Sprintf("^/%s$", name)))})
	if err != nil {
		return err
	}

	if len(containers) > 1 {
		return fmt.Errorf("found more than one container matching %s", name)
	}

	create := false
	reCreate := false
	state := ""

	if len(containers) == 0 {
		create = true
	} else {
		c := containers[0]
		networkNames := []string{}
		for n := range c.NetworkSettings.Networks {
			networkNames = append(networkNames, n)
		}
		sort.Strings(networkNames)

		if image != c.Image {
			m.l.Debugf("Image differ, recreate %s", name)
			reCreate = true
		}

		if !reflect.DeepEqual(networks, networkNames) {
			m.l.Debugf("Network config differ, recreate %s", name)
			m.l.Debugf("Networks expected: %s", networks)
			m.l.Debugf("Networks current: %s", networkNames)
			reCreate = true
		}

		volumesExpected := make([]string, len(volumes))
		for i, v := range volumes {
			volume, err := loader.ParseVolume(v)
			if err != nil {
				return err
			}
			if volume.Source != "" {
				mode := ""
				if !volume.ReadOnly {
					mode = ":rw"
				} else {
					mode = ":ro"
				}
				volumesExpected[i] = fmt.Sprintf("%s:%s%s\n", volume.Source, volume.Target, mode)
			}
		}

		volumesCurrent := make([]string, 0)
		for _, m := range c.Mounts {
			src := m.Name
			if src == "" {
				src = m.Source
			}
			mode := ""
			if m.RW {
				mode = ":rw"
			} else {
				mode = ":ro"
			}

			volumesCurrent = append(volumesCurrent, fmt.Sprintf("%s:%s%s\n", src, m.Destination, mode))
		}

		if !stringSlicesEqual(volumesCurrent, volumesExpected) {
			m.l.Debugf("Volume config differ, recreate %s", name)
			m.l.Debugf("Volumes expected: %s", volumesExpected)
			m.l.Debugf("Volumes Current: %s", volumesCurrent)
			reCreate = true
		}

		state = c.State
	}

	if create || reCreate {
		images, err := m.cli.ImageList(m.ctx, types.ImageListOptions{
			Filters: filters.NewArgs(filters.Arg("reference", fmt.Sprintf("%s", image))),
		})
		if err != nil {
			return err
		}

		if len(images) == 0 {
			m.l.Infof("Pulling image: %s\n", image)
			out, err := m.cli.ImagePull(m.ctx, image, types.ImagePullOptions{})
			if err != nil {
				return err
			}
			defer out.Close()

			io.Copy(ioutil.Discard, out)
			m.l.Infof("Pulled image: %s\n", image)
		}
	}

	if reCreate {
		m.l.Infof("Recreating container: %s\n", name)
		m.l.Infof("Stopping old container: %s\n", name)
		containerID := fmt.Sprintf("/%s", name)
		if state == "running" {
			timeout := 30 * time.Second
			err = m.cli.ContainerStop(m.ctx, containerID, &timeout)
			if err != nil {
				return err
			}
		}
		m.l.Infof("Removing old container: %s\n", name)
		err = m.cli.ContainerRemove(m.ctx, containerID, types.ContainerRemoveOptions{RemoveVolumes: false, Force: true})
		if err != nil {
			return err
		}
	}

	if create || reCreate {
		m.l.Infof("Creating container: %s\n", name)
		c, err := m.cli.ContainerCreate(m.ctx, &container.Config{
			Image:   image,
			Volumes: nil,
		}, &container.HostConfig{
			Binds:         volumes,
			RestartPolicy: container.RestartPolicy{Name: "always"},
		}, nil, nil, name)
		if err != nil {
			return err
		}

		for _, n := range networks {
			err = m.cli.NetworkConnect(m.ctx, n, c.ID, nil)
			if err != nil {
				return err
			}
		}

	}

	c := m.getContainer(name)
	if c != nil {
		if c.State != "running" {
			m.l.Infof("Starting container %s with id %s\n", name, c.ID)

			err = m.cli.ContainerStart(m.ctx, c.ID, types.ContainerStartOptions{})
			if err != nil {
				return err
			}

			m.l.Infof("Started container %s with id %s\n", name, c.ID)
		}
	}

	return nil
}

func (m *manager) getContainer(name string) *types.Container {
	containers, err := m.cli.ContainerList(m.ctx, types.ContainerListOptions{All: true, Filters: filters.NewArgs(filters.Arg("name", fmt.Sprintf("^/%s$", name)))})
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
				m.l.Infof("Stopping container: %s", cc.Name)
				timeout := 30 * time.Second
				err := m.cli.ContainerStop(m.ctx, c.ID, &timeout)
				if err != nil {
					m.l.WithError(err).Errorf("error when stopping container")
				}
			}
			m.l.Infof("Removing container: %s", cc.Name)
			err := m.cli.ContainerRemove(m.ctx, c.ID, types.ContainerRemoveOptions{RemoveVolumes: false, Force: true})
			if err != nil {
				m.l.WithError(err).Errorf("error when removing container")
			}
		}
	}
}

func (m *manager) getNetworkNames() ([]string, error) {
	names := []string{}

	networks, err := m.cli.NetworkList(m.ctx, types.NetworkListOptions{})
	if err != nil {
		return nil, err
	}

	for _, network := range networks {
		if (network.Attachable || network.Scope == "local") && !containsString(ignoredNetworkNames, network.Name) {
			names = append(names, network.Name)
		}
	}

	sort.Strings(names)

	return names, nil
}

func containsString(strings []string, s string) bool {
	for _, a := range strings {
		if s == a {
			return true
		}
	}
	return false
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func remove(slice []string, values []string) []string {
	var r []string
	for _, v := range slice {
		if !containsString(values, v) {
			r = append(r, v)
		}
	}
	return r
}
