// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package docker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"

	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/config"
	"github.com/matrix-org/complement/internal/instruction"
)

var (
	// HostnameRunningDocker is the hostname of the docker daemon from the perspective of Complement.
	HostnameRunningDocker = "localhost"
	// HostnameRunningComplement is the hostname of Complement from the perspective of a Homeserver.
	HostnameRunningComplement = "host.docker.internal"
)

const complementLabel = "complement_context"

type Builder struct {
	Config *config.Complement
	Docker *client.Client
}

func NewBuilder(cfg *config.Complement) (*Builder, error) {
	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, err
	}
	return &Builder{
		Docker: cli,
		Config: cfg,
	}, nil
}

func (d *Builder) log(str string, args ...interface{}) {
	if !d.Config.DebugLoggingEnabled {
		return
	}
	log.Printf(str, args...)
}

func (d *Builder) Cleanup() {
	err := d.removeContainers()
	if err != nil {
		d.log("Cleanup: Failed to remove containers: %s", err)
	}
	err = d.removeImages()
	if err != nil {
		d.log("Cleanup: Failed to remove images: %s", err)
	}
	err = d.removeNetworks()
	if err != nil {
		d.log("Cleanup: Failed to remove networks: %s", err)
	}
}

// removeImages removes all images with `complementLabel`.
func (d *Builder) removeNetworks() error {
	networks, err := d.Docker.NetworkList(context.Background(), network.ListOptions{
		Filters: label(
			complementLabel,
			"complement_pkg="+d.Config.PackageNamespace,
		),
	})
	if err != nil {
		return err
	}
	for _, nw := range networks {
		err = d.Docker.NetworkRemove(context.Background(), nw.ID)
		if err != nil {
			return err
		}
	}
	return nil
}

// removeImages removes all images with `complementLabel`.
func (d *Builder) removeImages() error {
	images, err := d.Docker.ImageList(context.Background(), image.ListOptions{
		Filters: label(
			complementLabel,
			"complement_pkg="+d.Config.PackageNamespace,
		),
	})
	if err != nil {
		return err
	}
	for _, img := range images {
		// we only clean up localhost/complement images else if someone docker pulls
		// an anonymous snapshot we might incorrectly nuke it :( any non-localhost
		// tag marks this image as safe (as images can have multiple tags)
		isLocalhost := true
		for _, rt := range img.RepoTags {
			if !strings.HasPrefix(rt, "localhost/complement") {
				isLocalhost = false
				break
			}
		}
		if !isLocalhost {
			d.log("Not cleaning up image with tags: %v", img.RepoTags)
			continue
		}
		bprintName := img.Labels["complement_blueprint"]
		keep := false
		for _, keepBprint := range d.Config.KeepBlueprints {
			if bprintName == keepBprint {
				keep = true
				break
			}
		}
		if keep {
			d.log("Keeping image created from blueprint %s", bprintName)
			continue
		}
		_, err = d.Docker.ImageRemove(context.Background(), img.ID, image.RemoveOptions{
			Force: true,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// removeContainers removes all containers with `complementLabel`.
func (d *Builder) removeContainers() error {
	containers, err := d.Docker.ContainerList(context.Background(), container.ListOptions{
		All: true,
		Filters: label(
			complementLabel,
			"complement_pkg="+d.Config.PackageNamespace,
		),
	})
	if err != nil {
		return err
	}
	for _, c := range containers {
		err = d.Docker.ContainerRemove(context.Background(), c.ID, container.RemoveOptions{
			Force: true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Builder) ConstructBlueprintIfNotExist(bprint b.Blueprint) error {
	images, err := d.Docker.ImageList(context.Background(), image.ListOptions{
		Filters: label(
			"complement_blueprint="+bprint.Name,
			"complement_pkg="+d.Config.PackageNamespace,
		),
	})
	if err != nil {
		return fmt.Errorf("ConstructBlueprintIfNotExist(%s): failed to ImageList: %w", bprint.Name, err)
	}
	if len(images) == 0 {
		err = d.ConstructBlueprint(bprint)
		if err != nil {
			return fmt.Errorf("ConstructBlueprintIfNotExist(%s): failed to ConstructBlueprint: %w", bprint.Name, err)
		}
	}
	return nil
}

func (d *Builder) ConstructBlueprint(bprint b.Blueprint) error {
	errs := d.construct(bprint)
	if len(errs) > 0 {
		for _, err := range errs {
			d.log("could not construct blueprint: %s", err)
		}
		return fmt.Errorf("errors whilst constructing blueprint %s: %v", bprint.Name, errs)
	}

	// wait a bit for images/containers to show up in 'image ls'
	foundImages := false
	var images []image.Summary
	var err error
	waitTime := 5 * time.Second
	startTime := time.Now()
	for time.Since(startTime) < waitTime {
		images, err = d.Docker.ImageList(context.Background(), image.ListOptions{
			Filters: label(
				complementLabel,
				"complement_blueprint="+bprint.Name,
				"complement_pkg="+d.Config.PackageNamespace,
			),
		})
		if err != nil {
			return err
		}
		if len(images) < len(bprint.Homeservers) {
			time.Sleep(100 * time.Millisecond)
		} else {
			foundImages = true
			break
		}
	}
	// do this after we have found images so we know that the containers have been detached so
	// we can actually remove the networks.
	d.removeNetworks()
	if !foundImages {
		return fmt.Errorf("failed to find built images via ImageList: did they all build ok?")
	}
	var imgDatas []string
	for _, img := range images {
		imgDatas = append(imgDatas, fmt.Sprintf("%s=>%v", img.ID, img.Labels))
	}
	d.log("Constructed blueprint '%s' : %v", bprint.Name, imgDatas)
	return nil
}

// construct all Homeservers sequentially then commits them
func (d *Builder) construct(bprint b.Blueprint) (errs []error) {
	d.log("Constructing blueprint '%s'", bprint.Name)

	networkName, err := createNetworkIfNotExists(d.Docker, d.Config.PackageNamespace, bprint.Name)
	if err != nil {
		return []error{err}
	}

	runner := instruction.NewRunner(bprint.Name, d.Config.BestEffort, d.Config.DebugLoggingEnabled)
	results := make([]result, len(bprint.Homeservers))
	for i, hs := range bprint.Homeservers {
		res := d.constructHomeserver(bprint.Name, runner, hs, networkName)
		if res.err != nil {
			errs = append(errs, res.err)
			if res.containerID != "" {
				// something went wrong, but we have a container which may have interesting logs
				printLogs(d.Docker, res.containerID, res.contextStr)
			}
			if delErr := d.Docker.ContainerRemove(context.Background(), res.containerID, container.RemoveOptions{
				Force: true,
			}); delErr != nil {
				d.log("%s: failed to remove container which failed to deploy: %s", res.contextStr, delErr)
			}
			// there is little point continuing to set up the remaining homeservers at this point
			return
		}
		// kill the container
		defer func(r result) {
			containerInfo, err := d.Docker.ContainerInspect(context.Background(), r.containerID)

			if err != nil {
				d.log("%s : Can't get status of %s", r.contextStr, r.containerID)
				return
			}

			if !containerInfo.State.Running {
				// The container isn't running anyway, so no need to kill it.
				return
			}

			killErr := d.Docker.ContainerKill(context.Background(), r.containerID, "KILL")
			if killErr != nil {
				d.log("%s : Failed to kill container %s: %s\n", r.contextStr, r.containerID, killErr)
			}

		}(res)
		results[i] = res
	}

	// commit containers
	for _, res := range results {
		if res.err != nil {
			continue
		}
		// collect and store access tokens as labels 'access_token_$userid: $token'
		labels := make(map[string]string)
		accessTokens := runner.AccessTokens(res.homeserver.Name)
		if len(bprint.KeepAccessTokensForUsers) > 0 {
			// only keep access tokens for specified users
			for _, userID := range bprint.KeepAccessTokensForUsers {
				tok, ok := accessTokens[userID]
				if ok {
					labels["access_token_"+userID] = tok
				}
			}
		} else {
			// keep all tokens
			for k, v := range accessTokens {
				labels["access_token_"+k] = v
			}
		}

		deviceIDs := runner.DeviceIDs(res.homeserver.Name)
		for userID, deviceID := range deviceIDs {
			labels["device_id"+userID] = deviceID
		}

		// Combine the labels for tokens and application services
		asLabels := labelsForApplicationServices(res.homeserver)
		for k, v := range asLabels {
			labels[k] = v
		}

		// Stop the container before we commit it.
		// This gives it chance to shut down gracefully.
		// If we don't do this, then e.g. Postgres databases can become corrupt, which
		// then incurs a slow recovery process when we use the blueprint later.
		d.log("%s: Stopping container: %s", res.contextStr, res.containerID)
		tenSeconds := 10
		d.Docker.ContainerStop(context.Background(), res.containerID, container.StopOptions{
			Timeout: &tenSeconds,
		})

		// Log again so we can see the timings.
		d.log("%s: Stopped container: %s", res.contextStr, res.containerID)

		// commit the container
		commit, err := d.Docker.ContainerCommit(context.Background(), res.containerID, container.CommitOptions{
			Author:    "Complement",
			Pause:     true,
			Reference: "localhost/complement:" + res.contextStr,
			Changes:   toChanges(labels),

			// Podman's compatibility API returns a 500 if the POST request has an empty body, so we give it an empty
			// Config to chew on.
			Config: &container.Config{},
		})
		if err != nil {
			d.log("%s : failed to ContainerCommit: %s\n", res.contextStr, err)
			errs = append(errs, fmt.Errorf("%s : failed to ContainerCommit: %w", res.contextStr, err))
			continue
		}
		imageID := strings.Replace(commit.ID, "sha256:", "", 1)
		d.log("%s: Created docker image %s\n", res.contextStr, imageID)
	}
	return errs
}

// Convert a map of labels to a list of changes directive in Dockerfile format.
// Labels keys and values can't be multiline (eg. can't contain `\n` character)
// neither can they contain unescaped `"` character.
func toChanges(labels map[string]string) []string {
	var changes []string
	for k, v := range labels {
		changes = append(changes, fmt.Sprintf("LABEL \"%s\"=\"%s\"", k, v))
	}
	return changes
}

// construct this homeserver and execute its instructions, keeping the container alive.
func (d *Builder) constructHomeserver(blueprintName string, runner *instruction.Runner, hs b.Homeserver, networkName string) result {
	contextStr := fmt.Sprintf("%s.%s.%s", d.Config.PackageNamespace, blueprintName, hs.Name)
	d.log("%s : constructing homeserver...\n", contextStr)
	dep, err := d.deployBaseImage(blueprintName, hs, contextStr, networkName)
	if err != nil {
		log.Printf("%s : failed to deployBaseImage: %s\n", contextStr, err)
		containerID := ""
		if dep != nil {
			containerID = dep.ContainerID
		}
		return result{
			err:         err,
			containerID: containerID,
			contextStr:  contextStr,
			homeserver:  hs,
		}
	}
	d.log("%s : deployed base image to %s (%s)\n", contextStr, dep.BaseURL, dep.ContainerID)
	err = runner.Run(hs, dep.BaseURL)
	if err != nil {
		d.log("%s : failed to run instructions: %s\n", contextStr, err)
	}
	return result{
		err:         err,
		containerID: dep.ContainerID,
		contextStr:  contextStr,
		homeserver:  hs,
	}
}

// deployBaseImage runs the base image and returns the baseURL, containerID or an error.
func (d *Builder) deployBaseImage(blueprintName string, hs b.Homeserver, contextStr, networkName string) (*HomeserverDeployment, error) {
	asIDToRegistrationMap := asIDToRegistrationFromLabels(labelsForApplicationServices(hs))
	var baseImageURI string
	if hs.BaseImageURI == nil {
		baseImageURI = d.Config.BaseImageURI
		// Use HS specific base image if defined
		if uri, ok := d.Config.BaseImageURIs[hs.Name]; ok {
			baseImageURI = uri
		}
	} else {
		baseImageURI = *hs.BaseImageURI
	}

	return deployImage(
		d.Docker, baseImageURI, fmt.Sprintf("complement_%s", contextStr),
		d.Config.PackageNamespace, blueprintName, hs.Name, asIDToRegistrationMap, contextStr,
		networkName, d.Config,
	)
}

// Multilines label using Dockerfile syntax is unsupported, let's inline \n instead
func generateASRegistrationYaml(as b.ApplicationService) string {
	return fmt.Sprintf("id: %s\\n", as.ID) +
		fmt.Sprintf("hs_token: %s\\n", as.HSToken) +
		fmt.Sprintf("as_token: %s\\n", as.ASToken) +
		fmt.Sprintf("url: '%s'\\n", as.URL) +
		fmt.Sprintf("sender_localpart: %s\\n", as.SenderLocalpart) +
		fmt.Sprintf("rate_limited: %v\\n", as.RateLimited) +
		fmt.Sprintf("de.sorunome.msc2409.push_ephemeral: %v\\n", as.SendEphemeral) +
		fmt.Sprintf("push_ephemeral: %v\\n", as.SendEphemeral) +
		fmt.Sprintf("org.matrix.msc3202: %v\\n", as.EnableEncryption) +
		"namespaces:\\n" +
		"  users:\\n" +
		"    - exclusive: false\\n" +
		"      regex: .*\\n" +
		"  rooms: []\\n" +
		"  aliases: []\\n"
}

// createNetworkIfNotExists creates a docker network and returns its name.
// Name is guaranteed not to be empty when err == nil
func createNetworkIfNotExists(docker *client.Client, pkgNamespace, blueprintName string) (networkName string, err error) {
	// check if a network already exists for this blueprint
	nws, err := docker.NetworkList(context.Background(), network.ListOptions{
		Filters: label(
			"complement_pkg="+pkgNamespace,
			"complement_blueprint="+blueprintName,
		),
	})
	if err != nil {
		return "", fmt.Errorf("%s: failed to list networks. %w", blueprintName, err)
	}
	// return the existing network
	if len(nws) > 0 {
		if len(nws) > 1 {
			log.Printf("WARNING: createNetworkIfNotExists got %d networks for pkg=%s blueprint=%s", len(nws), pkgNamespace, blueprintName)
		}
		return nws[0].Name, nil
	}
	networkName = "complement_" + pkgNamespace + "_" + blueprintName
	// make a user-defined network so we get DNS based on the container name
	nw, err := docker.NetworkCreate(context.Background(), networkName, network.CreateOptions{
		Labels: map[string]string{
			complementLabel:        blueprintName,
			"complement_blueprint": blueprintName,
			"complement_pkg":       pkgNamespace,
		},
	})
	if err != nil {
		return "", fmt.Errorf("%s: failed to create docker network. %w", blueprintName, err)
	}
	if nw.Warning != "" {
		if nw.ID == "" {
			return "", fmt.Errorf("%s: fatal warning while creating docker network. %s", blueprintName, nw.Warning)
		}
		log.Printf("WARNING: %s\n", nw.Warning)
	}
	if nw.ID == "" {
		return "", fmt.Errorf("%s: unexpected empty ID while creating networkID", blueprintName)
	}
	return networkName, nil
}

func printLogs(docker *client.Client, containerID, contextStr string) {
	reader, err := docker.ContainerLogs(context.Background(), containerID, container.LogsOptions{
		ShowStderr: true,
		ShowStdout: true,
		Follow:     false,
	})
	if err != nil {
		log.Printf("%s : Failed to extract container logs: %s\n", contextStr, err)
		return
	}
	log.Printf("============================================\n\n\n")
	log.Printf("%s : Server logs:\n", contextStr)
	stdcopy.StdCopy(log.Writer(), log.Writer(), reader)
	log.Printf("============== %s : END LOGS ==============\n\n\n", contextStr)
}

func printPortBindingsOfAllComplementContainers(docker *client.Client, contextStr string) {
	ctx := context.Background()

	containers, err := docker.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: label(
			complementLabel,
		),
	})
	if err != nil {
		log.Printf("%s : Failed to list containers while trying to `printPortBindingsOfAllComplementContainers`: %s\n", contextStr, err)
		return
	}

	log.Printf("============== %s : START ALL COMPLEMENT DOCKER PORT BINDINGS ==============\n", contextStr)

	for _, container := range containers {
		log.Printf("Container: %s: %s", container.ID, container.Names)

		inspectRes, err := docker.ContainerInspect(ctx, container.ID)
		if err != nil {
			log.Printf("%s : Failed to inspect container (%s) while trying to `printPortBindingsOfAllComplementContainers`: %s\n", contextStr, container.ID, err)
			return
		}

		// Print an example so it's easier to understand the output
		log.Printf("    (host) -> (container)\n")
		// Then print the actual port bindings
		for containerPort, portBindings := range inspectRes.NetworkSettings.Ports {
			hostPortBindingStrings := make([]string, len(portBindings))
			for portBindingIndex, portBinding := range portBindings {
				hostPortBindingStrings[portBindingIndex] = fmt.Sprintf("%s:%s", portBinding.HostIP, portBinding.HostPort)
			}

			log.Printf("    %s -> %s\n", strings.Join(hostPortBindingStrings, ", "), containerPort)
		}

	}

	log.Printf("=============== %s : END ALL COMPLEMENT DOCKER PORT BINDINGS ===============\n\n\n", contextStr)
}

// endpoints transforms the homeserver ports into the base URL and federation base URL.
func endpoints(p nat.PortMap, hsPortBindingIP string, csPort, ssPort int) (baseURL, fedBaseURL string, err error) {
	csapiPortBinding, err := findPortBinding(p, hsPortBindingIP, csPort)
	if err != nil {
		return "", "", fmt.Errorf("Problem finding CS API port: %s", err)
	}
	baseURL = fmt.Sprintf("http://"+csapiPortBinding.HostIP+":%s", csapiPortBinding.HostPort)

	ssapiPortBinding, err := findPortBinding(p, hsPortBindingIP, ssPort)
	if err != nil {
		return "", "", fmt.Errorf("Problem finding SS API port: %s", err)
	}
	fedBaseURL = fmt.Sprintf("https://"+ssapiPortBinding.HostIP+":%s", ssapiPortBinding.HostPort)
	return
}

// findPortBinding finds a matching port binding for the given host/port in the `nat.PortMap`.
//
// This function will return the first port binding that matches the given host IP. If a
// `0.0.0.0` binding is found, we will assume that it is listening on all interfaces,
// including the `hsPortBindingIP`, and return a binding with the `hsPortBindingIP` as
// the host IP.
func findPortBinding(p nat.PortMap, hsPortBindingIP string, port int) (portBinding nat.PortBinding, err error) {
	portString := fmt.Sprintf("%d/tcp", port)
	portBindings, ok := p[nat.Port(portString)]
	if !ok {
		return nat.PortBinding{}, fmt.Errorf("port %s not exposed - exposed ports: %v", portString, p)
	}
	if len(portBindings) == 0 {
		return nat.PortBinding{}, fmt.Errorf("port %s exposed with not mapped port: %+v", portString, p)
	}

	for _, pb := range portBindings {
		if pb.HostIP == hsPortBindingIP {
			return pb, nil
		} else if pb.HostIP == "0.0.0.0" {
			// `0.0.0.0` means "all interfaces", so we can assume that this will be listening
			// for connections from `hsPortBindingIP` as well.
			return nat.PortBinding{
				HostIP:   hsPortBindingIP,
				HostPort: pb.HostPort,
			}, nil
		} else if pb.HostIP == "" && hsPortBindingIP == "127.0.0.1" {
			// `HostIP` can be empty in certain environments (observed with podman v4.3.1). We
			// will assume this is only a binding for `127.0.0.1`.
			return nat.PortBinding{
				HostIP:   hsPortBindingIP,
				HostPort: pb.HostPort,
			}, nil
		}
	}

	return nat.PortBinding{}, fmt.Errorf("unable to find matching port binding for %s %s: %+v", hsPortBindingIP, portString, p)
}

type result struct {
	err         error
	containerID string
	contextStr  string
	homeserver  b.Homeserver
}
