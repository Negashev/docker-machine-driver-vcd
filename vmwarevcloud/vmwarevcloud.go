/*
* docker-machine-driver-vcloud-director
* Copyright (C) 2017  Juan Manuel Irigaray
* Copyright (C) 2022  Aleksandr Negashev (i@negash.ru)
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package vmwarevcloud

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	govcd "github.com/vmware/go-vcloud-director/v2/govcd"
	"github.com/vmware/go-vcloud-director/v2/types/v56"

	"github.com/docker/machine/libmachine/drivers"
	"github.com/docker/machine/libmachine/log"
	"github.com/docker/machine/libmachine/mcnflag"
	"github.com/docker/machine/libmachine/mcnutils"
	"github.com/docker/machine/libmachine/ssh"
	"github.com/docker/machine/libmachine/state"
)

type Driver struct {
	*drivers.BaseDriver
	UserName       string
	UserPassword   string
	VDC            string
	OrgVDCNet      string
	EdgeGateway    string
	VdcEdgeGateway string
	PublicIP       string
	PrivateIP      string
	Catalog        string
	CatalogItem    string
	StorProfile    string
	DockerPort     int
	CPUCount       int
	MemorySize     int
	VAppID         string
	Href           string
	Url            *url.URL
	Org            string
	Insecure       bool
}

const (
	defaultCatalog     = "Public Catalog"
	defaultCatalogItem = "Ubuntu Server 12.04 LTS (amd64 20150127)"
	defaultCpus        = 2
	defaultMemory      = 2048
	defaultSSHPort     = 22
	defaultDockerPort  = 2376
	defaultInsecure    = false
)

func takeIntAddress(x int) *int {
	return &x
}

func takeBoolPointer(value bool) *bool {
	return &value
}

// GetCreateFlags registers the flags this driver adds to
// "docker hosts create"
func (d *Driver) GetCreateFlags() []mcnflag.Flag {
	return []mcnflag.Flag{
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_USERNAME",
			Name:   "vmwarevclouddirector-username",
			Usage:  "vCloud Director username",
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_PASSWORD",
			Name:   "vmwarevclouddirector-password",
			Usage:  "vCloud Director password",
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_VDC",
			Name:   "vmwarevclouddirector-vdc",
			Usage:  "vCloud Director Virtual Data Center",
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_VDCEDGEGATEWAY",
			Name:   "vmwarevclouddirector-vdcedgegateway",
			Usage:  "vCloud Director Virtual Data Center Edge Gateway",
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_ORG",
			Name:   "vmwarevclouddirector-org",
			Usage:  "vCloud Director Organization",
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_ORGVDCNETWORK",
			Name:   "vmwarevclouddirector-orgvdcnetwork",
			Usage:  "vCloud Direcotr Org VDC Network",
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_EDGEGATEWAY",
			Name:   "vmwarevclouddirector-edgegateway",
			Usage:  "vCloud Director Edge Gateway (Default is <vdc>)",
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_PUBLICIP",
			Name:   "vmwarevclouddirector-publicip",
			Usage:  "vCloud Director Org Public IP to use",
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_CATALOG",
			Name:   "vmwarevclouddirector-catalog",
			Usage:  "vCloud Director Catalog (default is Public Catalog)",
			Value:  defaultCatalog,
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_CATALOGITEM",
			Name:   "vmwarevclouddirector-catalogitem",
			Usage:  "vCloud Director Catalog Item (default is Ubuntu Precise)",
			Value:  defaultCatalogItem,
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_STORPROFILE",
			Name:   "vmwarevclouddirector-storprofile",
			Usage:  "vCloud Storage Profile name",
		},
		mcnflag.StringFlag{
			EnvVar: "VCLOUDDIRECTOR_HREF",
			Name:   "vmwarevclouddirector-href",
			Usage:  "vCloud Director API Endpoint",
		},
		mcnflag.BoolFlag{
			EnvVar: "VCLOUDDIRECTOR_INSECURE",
			Name:   "vmwarevclouddirector-insecure",
			Usage:  "vCloud Director allow non secure connections",
		},
		mcnflag.IntFlag{
			EnvVar: "VCLOUDDIRECTOR_CPU_COUNT",
			Name:   "vmwarevclouddirector-cpu-count",
			Usage:  "vCloud Director VM Cpu Count (default 1)",
			Value:  defaultCpus,
		},
		mcnflag.IntFlag{
			EnvVar: "VCLOUDDIRECTOR_MEMORY_SIZE",
			Name:   "vmwarevclouddirector-memory-size",
			Usage:  "vCloud Director VM Memory Size in MB (default 2048)",
			Value:  defaultMemory,
		},
		mcnflag.IntFlag{
			EnvVar: "VCLOUDDIRECTOR_SSH_PORT",
			Name:   "vmwarevclouddirector-ssh-port",
			Usage:  "vCloud Director SSH port",
			Value:  defaultSSHPort,
		},
		mcnflag.IntFlag{
			EnvVar: "VCLOUDDIRECTOR_DOCKER_PORT",
			Name:   "vmwarevclouddirector-docker-port",
			Usage:  "vCloud Director Docker port",
			Value:  defaultDockerPort,
		},
	}
}

func NewDriver(hostName, storePath string) drivers.Driver {
	return &Driver{
		Catalog:     defaultCatalog,
		CatalogItem: defaultCatalogItem,
		CPUCount:    defaultCpus,
		MemorySize:  defaultMemory,
		DockerPort:  defaultDockerPort,
		Insecure:    defaultInsecure,
		BaseDriver: &drivers.BaseDriver{
			SSHPort:     defaultSSHPort,
			MachineName: hostName,
			StorePath:   storePath,
		},
	}
}

func (d *Driver) GetSSHHostname() (string, error) {
	return d.GetIP()
}

// DriverName returns the name of the driver
func (d *Driver) DriverName() string {
	return "vcloud-director"
}

func (d *Driver) SetConfigFromFlags(flags drivers.DriverOptions) error {

	d.UserName = flags.String("vmwarevclouddirector-username")
	d.UserPassword = flags.String("vmwarevclouddirector-password")
	d.VDC = flags.String("vmwarevclouddirector-vdc")
	d.Org = flags.String("vmwarevclouddirector-org")
	d.Href = flags.String("vmwarevclouddirector-href")
	d.Insecure = flags.Bool("vmwarevclouddirector-insecure")
	d.PublicIP = flags.String("vmwarevclouddirector-publicip")
	d.StorProfile = flags.String("vmwarevclouddirector-storprofile")
	d.SetSwarmConfigFromFlags(flags)

	// Check for required Params
	if d.UserName == "" || d.UserPassword == "" || d.Href == "" || d.VDC == "" || d.Org == "" || d.StorProfile == "" {
		return fmt.Errorf("Please specify vclouddirector mandatory params using options: -vmwarevclouddirector-username -vmwarevclouddirector-password -vmwarevclouddirector-vdc -vmwarevclouddirector-href -vmwarevclouddirector-org and -vmwarevclouddirector-storprofile")
	}

	u, err := url.ParseRequestURI(d.Href)
	if err != nil {
		return fmt.Errorf("Unable to pass url: %s", err)
	}
	d.Url = u

	// If the Org VDC Network is empty, set it to the default routed network.
	if flags.String("vmwarevclouddirector-orgvdcnetwork") == "" {
		d.OrgVDCNet = flags.String("vmwarevclouddirector-vdc") + "-default-routed"
	} else {
		d.OrgVDCNet = flags.String("vmwarevclouddirector-orgvdcnetwork")
	}

	// If the Edge Gateway is empty, just set it to the default edge gateway.
	// if flags.String("vmwarevclouddirector-edgegateway") == "" {
	// 	d.EdgeGateway = flags.String("vmwarevclouddirector-org")
	// } else {
	d.EdgeGateway = flags.String("vmwarevclouddirector-edgegateway")
	// }

	if flags.String("vmwarevclouddirector-vdcedgegateway") == "" {
		d.VdcEdgeGateway = flags.String("vmwarevclouddirector-vdc")
	} else {
		d.VdcEdgeGateway = flags.String("vmwarevclouddirector-vdcedgegateway")
	}

	d.Catalog = flags.String("vmwarevclouddirector-catalog")
	d.CatalogItem = flags.String("vmwarevclouddirector-catalogitem")

	d.DockerPort = flags.Int("vmwarevclouddirector-docker-port")
	d.SSHUser = "docker"
	d.SSHPort = flags.Int("vmwarevclouddirector-ssh-port")
	d.CPUCount = flags.Int("vmwarevclouddirector-cpu-count")
	d.MemorySize = flags.Int("vmwarevclouddirector-memory-size")
	d.PrivateIP = d.PublicIP

	return nil
}

func (d *Driver) GetURL() (string, error) {
	if err := drivers.MustBeRunning(d); err != nil {
		return "", err
	}

	return fmt.Sprintf("tcp://%s", net.JoinHostPort(d.PrivateIP, strconv.Itoa(d.DockerPort))), nil
}

func (d *Driver) GetIP() (string, error) {
	if d.PublicIP == "" {
		return d.PrivateIP, nil
	}
	return d.PublicIP, nil
}

func (d *Driver) GetState() (state.State, error) {
	p := govcd.NewVCDClient(*d.Url, d.Insecure)

	log.Debug("Connecting to vCloud Director to fetch vApp Status...")
	// Authenticate to vCloud Director
	err := p.Authenticate(d.UserName, d.UserPassword, d.Org)
	if err != nil {
		return state.Error, err
	}

	org, err := p.GetOrgByName(d.Org)
	if err != nil {
		return state.Error, err
	}

	vdc, err := org.GetVDCByName(d.VDC, true)
	if err != nil {
		return state.Error, err
	}

	vapp, err := vdc.GetVAppById(d.VAppID, true)
	if err != nil {
		return state.Error, err
	}

	status, err := vapp.GetStatus()
	if err != nil {
		return state.Error, err
	}

	// if err = p.Disconnect(); err != nil {
	// 	return state.Error, err
	// }

	switch status {
	case "POWERED_ON":
		return state.Running, nil
	case "POWERED_OFF":
		return state.Stopped, nil
	}
	return state.None, nil
}

func (d *Driver) Create() error {
	key, err := d.createSSHKey()
	if err != nil {
		return err
	}

	p := govcd.NewVCDClient(*d.Url, d.Insecure)

	log.Infof("Connecting to vCloud Director...")
	// Authenticate to vCloud Director
	err = p.Authenticate(d.UserName, d.UserPassword, d.Org)
	if err != nil {
		return err
	}

	org, err := p.GetOrgByName(d.Org)
	if err != nil {
		return err
	}

	vdc, err := org.GetVDCByName(d.VDC, true)
	if err != nil {
		return err
	}

	log.Infof("Finding VDC Network...")
	// Find VDC Network
	net, err := vdc.FindVDCNetwork(d.OrgVDCNet)
	if err != nil {
		return err
	}

	log.Infof("Finding Catalog...")
	// Find our Catalog
	cat, err := org.GetCatalogByName(d.Catalog, true)
	if err != nil {
		return err
	}

	log.Infof("Finding Catalog Item...")
	// Find our Catalog Item
	cati, err := cat.GetCatalogItemByName(d.CatalogItem, true)
	if err != nil {
		return err
	}

	// Fetch the vApp Template in the Catalog Item
	vapptemplate, err := cati.GetVAppTemplate()
	vapptemplate.VAppTemplate.Children.VM[0].Name = d.MachineName
	if err != nil {
		return err
	}

	// Create a new empty vApp
	vapp := govcd.NewVApp(&p.Client)

	var networks []*types.OrgVDCNetwork
	// Get StorageProfileReference
	storageProfileRef, err := vdc.FindStorageProfileReference(d.StorProfile)
	if err != nil {
		return fmt.Errorf("Error finding storage profile: %s", err)
	}
	networks = append(networks, net.OrgVDCNetwork)

	log.Infof("Creating a new vApp: %s...", d.MachineName)
	// Compose the vApp with ComposeVApp
	task, err := vdc.ComposeVApp(networks, vapptemplate, storageProfileRef, d.MachineName, "Container Host created with Docker Host", true)
	if err != nil {
		return err
	}

	// Wait for the creation to be completed
	if err = task.WaitTaskCompletion(); err != nil {
		return err
	}

	vapp, err = vdc.GetVAppByName(d.MachineName, true)
	if err != nil {
		return err
	}
	vm, err := vapp.GetVMByName(d.MachineName, true)
	if err != nil {
		return err
	}
	// Wait vm is created
	for {
		vapp, err = vdc.GetVAppByName(d.MachineName, true)
		if err != nil {
			return err
		}
		vm, err = vapp.GetVMByName(d.MachineName, true)
		if err != nil {
			return err
		}
		time.Sleep(2 * time.Second)
		if vm.VM.VmSpecSection != nil {
			break
		}
	}

	// Set VAppID with ID of the created VApp
	vmSpecSection := vm.VM.VmSpecSection
	description := vm.VM.Description

	vmSpecSection.NumCpus = takeIntAddress(d.CPUCount)
	// has to come together
	vmSpecSection.NumCoresPerSocket = takeIntAddress(d.CPUCount)

	vmSpecSection.MemoryResourceMb.Configured = int64(d.MemorySize)

	log.Infof("Change VM size...")
	_, err = vm.UpdateVmSpecSection(vmSpecSection, description)
	if err != nil {
		return fmt.Errorf("Error changing size: %s", err)
	}

	log.Infof("Running customization script (SSH)...")
	GuestCustomizationSection := vm.VM.GuestCustomizationSection

	GuestCustomizationSection.AdminPasswordEnabled = takeBoolPointer(false)

	GuestCustomizationSection.CustomizationScript = "useradd -m -d /home/docker -s /bin/bash docker\nmkdir -p /home/docker/.ssh\nchown -R docker:docker /home/docker/.ssh\nchmod 700 /home/docker/.ssh\nchmod 600 /home/docker/.ssh/authorized_keys\nusermod -a -G sudo docker\necho \"" + strings.TrimSpace(key) + "\" > /home/docker/.ssh/authorized_keys\npasswd -d docker\nsed -i_bak \"s/\\(nameserver\\) .*/\\1 1.1.1.1/\" /etc/resolv.conf"

	_, err = vm.SetGuestCustomizationSection(GuestCustomizationSection)
	if err != nil {
		return err
	}

	task, err = vapp.PowerOn()
	if err != nil {
		return err
	}

	log.Infof("Waiting for the VM to power on and run the customization script...")

	if err = task.WaitTaskCompletion(); err != nil {
		return err
	}

	for {
		vm, err = vapp.GetVMByName(d.MachineName, true)
		if err != nil {
			return err
		}
		time.Sleep(2 * time.Second)
		if vm.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress != "" {
			d.PrivateIP = vm.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress
			break
		}
	}

	if d.EdgeGateway != "" && d.PublicIP != "" {

		vdcGateway, err := org.GetVDCByName(d.VdcEdgeGateway, true)
		if err != nil {
			return err
		}
		edge, err := vdcGateway.GetEdgeGatewayByName(d.EdgeGateway, true)
		if err != nil {
			return err
		}

		log.Infof("Creating NAT and Firewall Rules on %s...", d.EdgeGateway)
		task, err = edge.Create1to1Mapping(vm.VM.NetworkConnectionSection.NetworkConnection[0].IPAddress, d.PublicIP, d.MachineName)
		if err != nil {
			return err
		}

		if err = task.WaitTaskCompletion(); err != nil {
			return err
		}
	}

	// log.Debugf("Disconnecting from vCloud Director...")

	// if err = p.Disconnect(); err != nil {
	// 	return err
	// }

	// Set VAppID with ID of the created VApp
	d.VAppID = vapp.VApp.ID

	d.IPAddress, err = d.GetIP()
	return err
}

func (d *Driver) Remove() error {
	p := govcd.NewVCDClient(*d.Url, d.Insecure)

	log.Infof("Connecting to vCloud Director...")
	// Authenticate to vCloud Director
	err := p.Authenticate(d.UserName, d.UserPassword, d.Org)
	if err != nil {
		return err
	}

	org, err := p.GetOrgByName(d.Org)
	if err != nil {
		return err
	}

	vdc, err := org.GetVDCByName(d.VDC, true)
	if err != nil {
		return err
	}

	vapp, err := vdc.FindVAppByID(d.VAppID)
	if err != nil {
		log.Infof("Can't find the vApp, assuming it was deleted already...")
		return nil
	}

	status, err := vapp.GetStatus()
	if err != nil {
		return err
	}

	if d.EdgeGateway != "" && d.PublicIP != "" {

		vdcGateway, err := org.GetVDCByName(d.VdcEdgeGateway, true)
		if err != nil {
			return err
		}
		edge, err := vdcGateway.GetEdgeGatewayByName(d.EdgeGateway, true)
		if err != nil {
			return err
		}

		log.Infof("Removing NAT and Firewall Rules on %s...", d.EdgeGateway)
		task, err := edge.Remove1to1Mapping(vapp.VApp.Children.VM[0].NetworkConnectionSection.NetworkConnection[0].IPAddress, d.PublicIP)
		if err != nil {
			return err
		}
		if err = task.WaitTaskCompletion(); err != nil {
			return err
		}
	}

	if status == "POWERED_ON" {
		// If it's powered on, power it off before deleting
		log.Infof("Powering Off %s...", d.MachineName)
		task, err := vapp.PowerOff()
		if err != nil {
			return err
		}
		if err = task.WaitTaskCompletion(); err != nil {
			return err
		}

	}

	log.Debugf("Undeploying %s...", d.MachineName)
	task, err := vapp.Undeploy()
	if err != nil {
		return err
	}
	if err = task.WaitTaskCompletion(); err != nil {
		return err
	}

	log.Infof("Deleting %s...", d.MachineName)
	task, err = vapp.Delete()
	if err != nil {
		return err
	}
	if err = task.WaitTaskCompletion(); err != nil {
		return err
	}

	// if err = p.Disconnect(); err != nil {
	// 	return err
	// }

	return nil
}

func (d *Driver) Start() error {
	p := govcd.NewVCDClient(*d.Url, d.Insecure)

	log.Infof("Connecting to vCloud Director...")
	// Authenticate to vCloud Director
	err := p.Authenticate(d.UserName, d.UserPassword, d.Org)
	if err != nil {
		return err
	}

	org, err := p.GetOrgByName(d.Org)
	if err != nil {
		return err
	}

	vdc, err := org.GetVDCByName(d.VDC, true)
	if err != nil {
		return err
	}

	log.Infof("Finding vApp %s", d.VAppID)
	vapp, err := vdc.FindVAppByID(d.VAppID)
	if err != nil {
		return err
	}

	status, err := vapp.GetStatus()
	if err != nil {
		return err
	}

	if status == "POWERED_OFF" {
		log.Infof("Starting %s...", d.MachineName)
		task, err := vapp.PowerOn()
		if err != nil {
			return err
		}
		if err = task.WaitTaskCompletion(); err != nil {
			return err
		}

	}

	// if err = p.Disconnect(); err != nil {
	// 	return err
	// }

	d.IPAddress, err = d.GetIP()
	return err
}

func (d *Driver) Stop() error {
	p := govcd.NewVCDClient(*d.Url, d.Insecure)

	log.Infof("Connecting to vCloud Director...")
	// Authenticate to vCloud Director
	err := p.Authenticate(d.UserName, d.UserPassword, d.Org)
	if err != nil {
		return err
	}

	org, err := p.GetOrgByName(d.Org)
	if err != nil {
		return err
	}

	vdc, err := org.GetVDCByName(d.VDC, true)
	if err != nil {
		return err
	}

	vapp, err := vdc.FindVAppByID(d.VAppID)
	if err != nil {
		return err
	}

	task, err := vapp.Shutdown()
	if err != nil {
		return err
	}
	if err = task.WaitTaskCompletion(); err != nil {
		return err
	}

	// if err = p.Disconnect(); err != nil {
	// 	return err
	// }

	d.IPAddress = ""

	return nil
}

func (d *Driver) Restart() error {
	p := govcd.NewVCDClient(*d.Url, d.Insecure)

	log.Infof("Connecting to vCloud Director...")
	// Authenticate to vCloud Director
	err := p.Authenticate(d.UserName, d.UserPassword, d.Org)
	if err != nil {
		return err
	}

	org, err := p.GetOrgByName(d.Org)
	if err != nil {
		return err
	}

	vdc, err := org.GetVDCByName(d.VDC, true)
	if err != nil {
		return err
	}

	vapp, err := vdc.FindVAppByID(d.VAppID)
	if err != nil {
		return err
	}

	task, err := vapp.Reset()
	if err != nil {
		return err
	}
	if err = task.WaitTaskCompletion(); err != nil {
		return err
	}

	// if err = p.Disconnect(); err != nil {
	// 	return err
	// }

	d.IPAddress, err = d.GetIP()
	return err
}

func (d *Driver) Kill() error {
	p := govcd.NewVCDClient(*d.Url, d.Insecure)

	log.Infof("Connecting to vCloud Director...")
	// Authenticate to vCloud Director
	err := p.Authenticate(d.UserName, d.UserPassword, d.Org)
	if err != nil {
		return err
	}

	org, err := p.GetOrgByName(d.Org)
	if err != nil {
		return err
	}

	vdc, err := org.GetVDCByName(d.VDC, true)
	if err != nil {
		return err
	}

	vapp, err := vdc.FindVAppByID(d.VAppID)
	if err != nil {
		return err
	}

	task, err := vapp.PowerOff()
	if err != nil {
		return err
	}
	if err = task.WaitTaskCompletion(); err != nil {
		return err
	}

	// if err = p.Disconnect(); err != nil {
	// 	return err
	// }

	d.IPAddress = ""

	return nil
}

// Helpers

func generateVMName() string {
	randomID := mcnutils.TruncateID(mcnutils.GenerateRandomID())
	return fmt.Sprintf("docker-host-%s", randomID)
}

func (d *Driver) createSSHKey() (string, error) {
	if err := ssh.GenerateSSHKey(d.GetSSHKeyPath()); err != nil {
		return "", err
	}

	publicKey, err := ioutil.ReadFile(d.publicSSHKeyPath())
	if err != nil {
		return "", err
	}

	return string(publicKey), nil
}

func (d *Driver) publicSSHKeyPath() string {
	return d.GetSSHKeyPath() + ".pub"
}
