package main

import (
	"fmt"
	"net"
	"slices"
	"testing"
)

func TestBroadcastAddr(t *testing.T) {
	tests := []struct {
		name      string
		cidr      string
		broadcast string
	}{
		{"slash 24", "10.0.1.0/24", "10.0.1.255"},
		{"slash 21", "10.0.64.0/21", "10.0.71.255"},
		{"slash 16", "172.16.0.0/16", "172.16.255.255"},
		{"slash 28", "192.168.1.0/28", "192.168.1.15"},
		{"slash 30", "10.0.0.0/30", "10.0.0.3"},
		{"slash 8", "10.0.0.0/8", "10.255.255.255"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ipNet, err := net.ParseCIDR(tt.cidr)
			if err != nil {
				t.Fatalf("bad CIDR: %v", err)
			}
			got := broadcastAddr(ipNet).String()
			if got != tt.broadcast {
				t.Errorf("broadcastAddr(%s) = %s, want %s", tt.cidr, got, tt.broadcast)
			}
		})
	}
}

func TestAddToIP(t *testing.T) {
	tests := []struct {
		name  string
		ip    string
		delta int
		want  string
	}{
		{"minus 1", "10.0.71.255", -1, "10.0.71.254"},
		{"minus 2", "10.0.71.255", -2, "10.0.71.253"},
		{"minus 255", "10.0.71.255", -255, "10.0.71.0"},
		{"byte boundary", "10.0.71.255", -256, "10.0.70.255"},
		{"byte boundary minus 1", "10.0.71.255", -257, "10.0.70.254"},
		{"two byte boundary", "10.0.0.0", -1, "9.255.255.255"},
		{"plus 1", "10.0.0.0", 1, "10.0.0.1"},
		{"zero delta", "10.0.0.1", 0, "10.0.0.1"},
		{"large negative", "10.0.71.255", -512, "10.0.69.255"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip).To4()
			got := addToIP(ip, tt.delta).String()
			if got != tt.want {
				t.Errorf("addToIP(%s, %d) = %s, want %s", tt.ip, tt.delta, got, tt.want)
			}
		})
	}

	t.Run("does not mutate input", func(t *testing.T) {
		ip := net.ParseIP("10.0.1.0").To4()
		original := ip.String()
		addToIP(ip, -1)
		if ip.String() != original {
			t.Errorf("input mutated: was %s, now %s", original, ip.String())
		}
	})
}

func mustParseCIDR(t *testing.T, cidr string) *net.IPNet {
	t.Helper()
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatalf("bad CIDR: %v", err)
	}
	return ipNet
}

func ipsToStrings(ips []net.IP) []string {
	result := make([]string, len(ips))
	for i, ip := range ips {
		result[i] = ip.String()
	}
	return result
}

func assertIPs(t *testing.T, label string, got []net.IP, want []string) {
	t.Helper()
	gotStr := ipsToStrings(got)
	if len(gotStr) != len(want) {
		t.Fatalf("%s: got %d IPs %v, want %d %v", label, len(gotStr), gotStr, len(want), want)
	}
	for i := range gotStr {
		if gotStr[i] != want[i] {
			t.Errorf("%s IP[%d]: got %s, want %s", label, i, gotStr[i], want[i])
		}
	}
}

func TestComputeIPCandidates(t *testing.T) {
	t.Run("plan example: /21, 4 peers, 2 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")
		peers := []string{"10.0.0.4", "10.0.0.1", "10.0.0.3", "10.0.0.2"}

		// After sorting: [10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.4]
		assertIPs(t, "node1 (idx 0)", computeIPCandidates(ipNet, peers, "10.0.0.1", 2),
			[]string{"10.0.71.254", "10.0.71.253"})
		assertIPs(t, "node2 (idx 1)", computeIPCandidates(ipNet, peers, "10.0.0.2", 2),
			[]string{"10.0.71.252", "10.0.71.251"})
		assertIPs(t, "node3 (idx 2)", computeIPCandidates(ipNet, peers, "10.0.0.3", 2),
			[]string{"10.0.71.250", "10.0.71.249"})
		assertIPs(t, "node4 (idx 3)", computeIPCandidates(ipNet, peers, "10.0.0.4", 2),
			[]string{"10.0.71.248", "10.0.71.247"})
	})

	t.Run("plan example: /24, 48 peers, 2 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		peers := make([]string, 48)
		for i := range peers {
			peers[i] = fmt.Sprintf("10.0.1.%d", i+1)
		}

		// Numeric sort: 10.0.1.1 is first (idx 0), 10.0.1.48 is last (idx 47)
		assertIPs(t, "first peer", computeIPCandidates(ipNet, peers, "10.0.1.1", 2),
			[]string{"10.0.1.254", "10.0.1.253"})

		// Last peer (idx 47): startOffset = 1+47*2 = 95
		assertIPs(t, "last peer", computeIPCandidates(ipNet, peers, "10.0.1.48", 2),
			[]string{"10.0.1.160", "10.0.1.159"})

		// Band = 96 IPs (.254 down to .159), leaves 158 for swarm
	})

	t.Run("single node, single container", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		assertIPs(t, "single",
			computeIPCandidates(ipNet, []string{"10.0.0.1"}, "10.0.0.1", 1),
			[]string{"10.0.1.254"})
	})

	t.Run("single node, 5 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		assertIPs(t, "5 containers",
			computeIPCandidates(ipNet, []string{"10.0.0.1"}, "10.0.0.1", 5),
			[]string{"10.0.1.254", "10.0.1.253", "10.0.1.252", "10.0.1.251", "10.0.1.250"})
	})

	t.Run("large cluster: /16, 100 peers, 3 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "172.16.0.0/16")
		peers := make([]string, 100)
		for i := range peers {
			peers[i] = fmt.Sprintf("10.0.0.%d", i+1)
		}

		// Numeric sort: 10.0.0.1 is first (idx 0), 10.0.0.100 is last (idx 99)
		assertIPs(t, "first peer",
			computeIPCandidates(ipNet, peers, "10.0.0.1", 3),
			[]string{"172.16.255.254", "172.16.255.253", "172.16.255.252"})

		// Last peer (idx 99): startOffset = 1 + 99*3 = 298
		// broadcast - 298 = 172.16.255.255 - 298 = 172.16.254.213
		assertIPs(t, "last peer",
			computeIPCandidates(ipNet, peers, "10.0.0.100", 3),
			[]string{"172.16.254.213", "172.16.254.212", "172.16.254.211"})

		// Total band = 300 IPs out of 65534 usable
	})

	t.Run("large cluster: /21, 50 peers, 4 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")
		peers := make([]string, 50)
		for i := range peers {
			peers[i] = fmt.Sprintf("10.1.0.%d", i+1)
		}

		// Numeric sort: 10.1.0.1 is first, 10.1.0.50 is last
		assertIPs(t, "first peer",
			computeIPCandidates(ipNet, peers, "10.1.0.1", 4),
			[]string{"10.0.71.254", "10.0.71.253", "10.0.71.252", "10.0.71.251"})

		// Last peer (idx 49): startOffset = 1 + 49*4 = 197
		last := computeIPCandidates(ipNet, peers, "10.1.0.50", 4)
		if len(last) != 4 {
			t.Fatalf("last peer: got %d IPs, want 4", len(last))
		}
		// Band = 200 IPs. Subnet has 2046 usable. Leaves 1846 for swarm.
	})

	t.Run("node not in peers list gets next slot after last peer", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		peers := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}

		// Unknown node gets index = len(peers) = 3
		// startOffset = 1 + 3*2 = 7 → .248, .247
		assertIPs(t, "unknown node",
			computeIPCandidates(ipNet, peers, "10.0.0.99", 2),
			[]string{"10.0.1.248", "10.0.1.247"})
	})

	t.Run("does not mutate input slice", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		peers := []string{"10.0.0.3", "10.0.0.1", "10.0.0.2"}
		original := slices.Clone(peers)
		computeIPCandidates(ipNet, peers, "10.0.0.1", 2)
		for i := range peers {
			if peers[i] != original[i] {
				t.Errorf("input slice mutated at [%d]: was %s, now %s", i, original[i], peers[i])
			}
		}
	})

	t.Run("peer order does not affect result", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")

		order1 := computeIPCandidates(ipNet, []string{"10.0.0.3", "10.0.0.1", "10.0.0.2"}, "10.0.0.2", 2)
		order2 := computeIPCandidates(ipNet, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, "10.0.0.2", 2)
		order3 := computeIPCandidates(ipNet, []string{"10.0.0.2", "10.0.0.3", "10.0.0.1"}, "10.0.0.2", 2)

		got1 := ipsToStrings(order1)
		got2 := ipsToStrings(order2)
		got3 := ipsToStrings(order3)

		for i := range got1 {
			if got1[i] != got2[i] || got2[i] != got3[i] {
				t.Errorf("order matters: %v vs %v vs %v", got1, got2, got3)
				break
			}
		}
	})

	t.Run("no overlap between any peers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")
		peers := make([]string, 20)
		for i := range peers {
			peers[i] = fmt.Sprintf("10.0.0.%d", i+1)
		}

		seen := map[string]string{}
		for _, peer := range peers {
			candidates := computeIPCandidates(ipNet, peers, peer, 3)
			for _, ip := range candidates {
				if prev, exists := seen[ip.String()]; exists {
					t.Errorf("IP %s assigned to both %s and %s", ip, prev, peer)
				}
				seen[ip.String()] = peer
			}
		}

		// Also check that unknown-node band doesn't overlap
		unknown := computeIPCandidates(ipNet, peers, "10.0.0.99", 3)
		for _, ip := range unknown {
			if prev, exists := seen[ip.String()]; exists {
				t.Errorf("unknown node IP %s overlaps with %s", ip, prev)
			}
		}
	})

	t.Run("all candidates within subnet and above network address", func(t *testing.T) {
		subnets := []string{"10.0.1.0/24", "10.0.64.0/21", "172.16.0.0/16", "192.168.1.0/28"}
		for _, cidr := range subnets {
			ipNet := mustParseCIDR(t, cidr)
			peers := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"}
			for _, peer := range peers {
				candidates := computeIPCandidates(ipNet, peers, peer, 3)
				for _, ip := range candidates {
					if !ipNet.Contains(ip) {
						t.Errorf("%s: candidate %s outside subnet", cidr, ip)
					}
					if ip.Equal(ipNet.IP) {
						t.Errorf("%s: candidate %s equals network address", cidr, ip)
					}
					broadcast := broadcastAddr(ipNet)
					if ip.Equal(broadcast) {
						t.Errorf("%s: candidate %s equals broadcast", cidr, ip)
					}
				}
			}
		}
	})

	t.Run("candidates are contiguous descending IPs", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")
		peers := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}

		for _, peer := range peers {
			candidates := computeIPCandidates(ipNet, peers, peer, 4)
			for i := 1; i < len(candidates); i++ {
				expected := addToIP(candidates[i-1], -1)
				if !candidates[i].Equal(expected) {
					t.Errorf("peer %s: gap between candidates[%d]=%s and candidates[%d]=%s",
						peer, i-1, candidates[i-1], i, candidates[i])
				}
			}
		}
	})

	t.Run("first candidate is always broadcast minus 1 for first peer", func(t *testing.T) {
		subnets := []string{"10.0.1.0/24", "10.0.64.0/21", "172.16.0.0/16"}
		for _, cidr := range subnets {
			ipNet := mustParseCIDR(t, cidr)
			// Use a peer that sorts first
			peers := []string{"10.0.0.1", "10.0.0.2"}
			candidates := computeIPCandidates(ipNet, peers, "10.0.0.1", 2)
			broadcast := broadcastAddr(ipNet)
			expected := addToIP(broadcast, -1)
			if !candidates[0].Equal(expected) {
				t.Errorf("%s: first candidate %s, want %s (broadcast-1)", cidr, candidates[0], expected)
			}
		}
	})

	t.Run("gateway at .1 not in candidates for /24", func(t *testing.T) {
		// Docker typically puts the gateway at .1 — our band at the top should never reach it
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		peers := make([]string, 10)
		for i := range peers {
			peers[i] = fmt.Sprintf("10.0.0.%d", i+1)
		}

		gateway := "10.0.1.1"
		for _, peer := range peers {
			candidates := computeIPCandidates(ipNet, peers, peer, 3)
			for _, ip := range candidates {
				if ip.String() == gateway {
					t.Errorf("peer %s got gateway IP %s as candidate", peer, gateway)
				}
			}
		}
	})

	t.Run("numeric sort: 10.0.0.10 sorts after 10.0.0.9 not after 10.0.0.1", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		peers := []string{"10.0.0.1", "10.0.0.9", "10.0.0.10"}

		// Numeric order: .1 (idx 0), .9 (idx 1), .10 (idx 2)
		// If lexicographic, .10 would be at idx 1 and .9 at idx 2
		assertIPs(t, ".9 at idx 1",
			computeIPCandidates(ipNet, peers, "10.0.0.9", 1),
			[]string{"10.0.1.253"})
		assertIPs(t, ".10 at idx 2",
			computeIPCandidates(ipNet, peers, "10.0.0.10", 1),
			[]string{"10.0.1.252"})
	})

	t.Run("adding node 10 to 9-node cluster does not shift existing bands", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")

		peers9 := make([]string, 9)
		for i := range peers9 {
			peers9[i] = fmt.Sprintf("10.0.0.%d", i+1)
		}
		// Record all 9 nodes' bands
		before := make(map[string][]string)
		for _, peer := range peers9 {
			before[peer] = ipsToStrings(computeIPCandidates(ipNet, peers9, peer, 2))
		}

		// Add 10.0.0.10 — numeric sort puts it last
		peers10 := append(slices.Clone(peers9), "10.0.0.10")
		for _, peer := range peers9 {
			after := ipsToStrings(computeIPCandidates(ipNet, peers10, peer, 2))
			for i := range before[peer] {
				if before[peer][i] != after[i] {
					t.Errorf("peer %s band changed after adding .10: %v → %v", peer, before[peer], after)
					break
				}
			}
		}
	})

	t.Run("adding a peer preserves earlier peers bands", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")

		// 3-node cluster
		peers3 := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
		before1 := ipsToStrings(computeIPCandidates(ipNet, peers3, "10.0.0.1", 2))
		before2 := ipsToStrings(computeIPCandidates(ipNet, peers3, "10.0.0.2", 2))

		// Add a 4th node that sorts AFTER existing ones
		peers4 := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"}
		after1 := ipsToStrings(computeIPCandidates(ipNet, peers4, "10.0.0.1", 2))
		after2 := ipsToStrings(computeIPCandidates(ipNet, peers4, "10.0.0.2", 2))

		// Peers before the new one keep their bands
		for i := range before1 {
			if before1[i] != after1[i] {
				t.Errorf("node1 band changed: %v → %v", before1, after1)
				break
			}
		}
		for i := range before2 {
			if before2[i] != after2[i] {
				t.Errorf("node2 band changed: %v → %v", before2, after2)
				break
			}
		}
	})

	t.Run("adding a peer that sorts in the middle shifts later peers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")

		peers2 := []string{"10.0.0.1", "10.0.0.3"}
		before3 := ipsToStrings(computeIPCandidates(ipNet, peers2, "10.0.0.3", 2))

		// Insert 10.0.0.2 between 1 and 3 — node3 shifts from index 1 to index 2
		peers3 := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
		after3 := ipsToStrings(computeIPCandidates(ipNet, peers3, "10.0.0.3", 2))

		if before3[0] == after3[0] {
			t.Errorf("node3 band should have shifted but didn't: %v → %v", before3, after3)
		}
	})

	t.Run("real world: spilnu-shared /21 with 6 nodes, 2 containers", func(t *testing.T) {
		// The actual network from the bug report
		ipNet := mustParseCIDR(t, "10.0.64.0/21")
		// 6 swarm nodes with realistic IPs
		peers := []string{
			"10.116.0.2", // node1
			"10.116.0.3", // node2
			"10.116.0.4", // node3
			"10.116.0.5", // node4
			"10.116.0.6", // node5
			"10.116.0.7", // node6
		}

		// Verify all bands are in the top 12 IPs (.254 down to .243)
		// and the collision IP .188 is nowhere near any band
		for _, peer := range peers {
			candidates := computeIPCandidates(ipNet, peers, peer, 2)
			for _, ip := range candidates {
				lastOctet := ip[3]
				if lastOctet < 243 {
					t.Errorf("peer %s: candidate %s too low (octet %d < 243), risk of Swarm collision",
						peer, ip, lastOctet)
				}
				if ip.String() == "10.0.71.188" {
					t.Errorf("peer %s: got the known collision IP 10.0.71.188", peer)
				}
			}
		}

		// Band uses 12 IPs out of 2046 usable
		// Swarm would need to fill 2034 IPs to reach it
	})

	t.Run("tiny subnet /28 with too many peers truncates", func(t *testing.T) {
		// /28 has 14 usable IPs (16 - network - broadcast)
		ipNet := mustParseCIDR(t, "192.168.1.0/28")
		peers := make([]string, 10)
		for i := range peers {
			peers[i] = fmt.Sprintf("10.0.0.%d", i+1)
		}

		// Numeric sort: 10.0.0.1 is first (idx 0), first peer: .14, .13 — fits
		assertIPs(t, "first peer",
			computeIPCandidates(ipNet, peers, "10.0.0.1", 2),
			[]string{"192.168.1.14", "192.168.1.13"})

		// Peer at index 7 (10.0.0.8): startOffset = 1+7*2 = 15, broadcast(.15) - 15 = .0 = network addr → no candidates
		got := computeIPCandidates(ipNet, peers, "10.0.0.8", 2)
		if len(got) != 0 {
			t.Errorf("peer at index 7 on /28: got %v, want empty (exceeds subnet)", ipsToStrings(got))
		}
	})

	t.Run("byte boundary crossing in band", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.0.0/21")
		// Broadcast is 10.0.7.255, peer index 3 with 4 containers:
		// startOffset = 1 + 3*4 = 13 → 10.0.7.242, .241, .240, .239
		peers := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"}
		assertIPs(t, "peer at idx 3",
			computeIPCandidates(ipNet, peers, "10.0.0.4", 4),
			[]string{"10.0.7.242", "10.0.7.241", "10.0.7.240", "10.0.7.239"})
	})

	t.Run("/30 minimal subnet, 1 peer, 1 container", func(t *testing.T) {
		// /30 has 2 usable IPs: .1 and .2
		ipNet := mustParseCIDR(t, "10.0.0.0/30")
		assertIPs(t, "/30 single",
			computeIPCandidates(ipNet, []string{"10.0.0.1"}, "10.0.0.1", 1),
			[]string{"10.0.0.2"})
	})

	t.Run("/30 minimal subnet, 1 peer, 2 containers — both fit", func(t *testing.T) {
		// network = .0, broadcast = .3, usable = .1 and .2
		// startOffset = 1 → .2, then .1 (not equal to network .0)
		ipNet := mustParseCIDR(t, "10.0.0.0/30")
		assertIPs(t, "/30 two containers",
			computeIPCandidates(ipNet, []string{"10.0.0.1"}, "10.0.0.1", 2),
			[]string{"10.0.0.2", "10.0.0.1"})
	})

	t.Run("/30 minimal subnet, 2 peers each get 1 IP", func(t *testing.T) {
		// /30: network=.0, broadcast=.3, usable=.1 and .2
		// peer 0: startOffset=1 → .2; peer 1: startOffset=2 → .1
		ipNet := mustParseCIDR(t, "10.0.0.0/30")
		assertIPs(t, "peer 0",
			computeIPCandidates(ipNet, []string{"10.0.0.1", "10.0.0.2"}, "10.0.0.1", 1),
			[]string{"10.0.0.2"})
		assertIPs(t, "peer 1",
			computeIPCandidates(ipNet, []string{"10.0.0.1", "10.0.0.2"}, "10.0.0.2", 1),
			[]string{"10.0.0.1"})
	})

	t.Run("/30 minimal subnet, 3 peers — third has no room", func(t *testing.T) {
		// peer 2: startOffset=1+2*1=3, broadcast(.3)-3=.0=network addr → no candidates
		ipNet := mustParseCIDR(t, "10.0.0.0/30")
		got := computeIPCandidates(ipNet, []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, "10.0.0.3", 1)
		if len(got) != 0 {
			t.Errorf("peer 2 on /30: got %v, want empty", ipsToStrings(got))
		}
	})

	t.Run("zero overlay containers returns nil", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		got := computeIPCandidates(ipNet, []string{"10.0.0.1"}, "10.0.0.1", 0)
		if got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})
}
