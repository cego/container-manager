package main

import (
	"net"
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

func TestComputeIPCandidates(t *testing.T) {
	t.Run("plan example: /21, 4 peers, 2 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")
		peers := []string{"10.0.0.4", "10.0.0.1", "10.0.0.3", "10.0.0.2"}

		// After sorting: [10.0.0.1, 10.0.0.2, 10.0.0.3, 10.0.0.4]
		expectations := []struct {
			nodeAddr string
			want     []string
		}{
			{"10.0.0.1", []string{"10.0.71.254", "10.0.71.253"}},
			{"10.0.0.2", []string{"10.0.71.252", "10.0.71.251"}},
			{"10.0.0.3", []string{"10.0.71.250", "10.0.71.249"}},
			{"10.0.0.4", []string{"10.0.71.248", "10.0.71.247"}},
		}

		for _, e := range expectations {
			got := ipsToStrings(computeIPCandidates(ipNet, peers, e.nodeAddr, 2))
			if len(got) != len(e.want) {
				t.Errorf("node %s: got %d IPs, want %d", e.nodeAddr, len(got), len(e.want))
				continue
			}
			for i := range got {
				if got[i] != e.want[i] {
					t.Errorf("node %s IP[%d]: got %s, want %s", e.nodeAddr, i, got[i], e.want[i])
				}
			}
		}
	})

	t.Run("plan example: /24, 48 peers, 2 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		peers := make([]string, 48)
		for i := range peers {
			peers[i] = net.IPv4(192, 168, 0, byte(i+1)).String()
		}

		// Lexicographic sort: 192.168.0.1 is first (index 0), 192.168.0.9 is last (index 47)
		first := ipsToStrings(computeIPCandidates(ipNet, peers, "192.168.0.1", 2))
		if first[0] != "10.0.1.254" || first[1] != "10.0.1.253" {
			t.Errorf("first peer: got %v, want [10.0.1.254 10.0.1.253]", first)
		}

		// 192.168.0.9 is at index 47 after lexicographic sort, startOffset = 1 + 47*2 = 95
		last := ipsToStrings(computeIPCandidates(ipNet, peers, "192.168.0.9", 2))
		if last[0] != "10.0.1.160" || last[1] != "10.0.1.159" {
			t.Errorf("last peer: got %v, want [10.0.1.160 10.0.1.159]", last)
		}
	})

	t.Run("single node, single container", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		got := ipsToStrings(computeIPCandidates(ipNet, []string{"10.0.0.1"}, "10.0.0.1", 1))
		if len(got) != 1 || got[0] != "10.0.1.254" {
			t.Errorf("got %v, want [10.0.1.254]", got)
		}
	})

	t.Run("single node, 5 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		got := ipsToStrings(computeIPCandidates(ipNet, []string{"10.0.0.1"}, "10.0.0.1", 5))
		want := []string{"10.0.1.254", "10.0.1.253", "10.0.1.252", "10.0.1.251", "10.0.1.250"}
		if len(got) != len(want) {
			t.Fatalf("got %d IPs, want %d", len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("IP[%d]: got %s, want %s", i, got[i], want[i])
			}
		}
	})

	t.Run("large cluster: /16, 100 peers, 3 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "172.16.0.0/16")
		peers := make([]string, 100)
		for i := range peers {
			peers[i] = net.IPv4(10, 0, byte(i/256), byte(i%256+1)).String()
		}

		// First peer: broadcast is 172.16.255.255, gets .254, .253, .252
		first := ipsToStrings(computeIPCandidates(ipNet, peers, peers[0], 3))
		if first[0] != "172.16.255.254" {
			t.Errorf("first peer IP[0]: got %s, want 172.16.255.254", first[0])
		}
		if len(first) != 3 {
			t.Errorf("first peer: got %d IPs, want 3", len(first))
		}

		// Last peer (index 99): startOffset = 1 + 99*3 = 298
		// broadcast - 298 = 172.16.255.255 - 298 = 172.16.254.213
		last := ipsToStrings(computeIPCandidates(ipNet, peers, peers[99], 3))
		if len(last) != 3 {
			t.Fatalf("last peer: got %d IPs, want 3", len(last))
		}
		if last[0] != "172.16.254.213" {
			t.Errorf("last peer IP[0]: got %s, want 172.16.254.213", last[0])
		}

		// Total band = 300 IPs out of 65534 usable — plenty of room
	})

	t.Run("large cluster: /21, 50 peers, 4 containers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")
		peers := make([]string, 50)
		for i := range peers {
			peers[i] = net.IPv4(10, 1, 0, byte(i+1)).String()
		}

		// Band = 200 IPs. Subnet has 2046 usable. Leaves 1846 for swarm.
		first := ipsToStrings(computeIPCandidates(ipNet, peers, peers[0], 4))
		if len(first) != 4 || first[0] != "10.0.71.254" {
			t.Errorf("first peer: got %v", first)
		}

		// Last peer (index 49): startOffset = 1 + 49*4 = 197
		last := ipsToStrings(computeIPCandidates(ipNet, peers, peers[49], 4))
		if len(last) != 4 {
			t.Fatalf("last peer: got %d IPs, want 4", len(last))
		}
		if last[0] != "10.0.71.58" {
			t.Errorf("last peer IP[0]: got %s, want 10.0.71.58", last[0])
		}
	})

	t.Run("node not in peers list", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")
		peers := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}

		// Unknown node gets index = len(peers) = 3
		got := ipsToStrings(computeIPCandidates(ipNet, peers, "10.0.0.99", 2))
		// startOffset = 1 + 3*2 = 7 → .248, .247
		if len(got) != 2 || got[0] != "10.0.1.248" || got[1] != "10.0.1.247" {
			t.Errorf("unknown node: got %v, want [10.0.1.248 10.0.1.247]", got)
		}
	})

	t.Run("peers sorted regardless of input order", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.1.0/24")

		// Same peers, different order — same node should get same IPs
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

	t.Run("no overlap between peers", func(t *testing.T) {
		ipNet := mustParseCIDR(t, "10.0.64.0/21")
		peers := make([]string, 20)
		for i := range peers {
			peers[i] = net.IPv4(10, 0, 0, byte(i+1)).String()
		}

		seen := map[string]int{}
		for peerIdx, peer := range peers {
			candidates := computeIPCandidates(ipNet, peers, peer, 3)
			for _, ip := range candidates {
				if prev, exists := seen[ip.String()]; exists {
					t.Errorf("IP %s assigned to both peer %d and peer %d", ip, prev, peerIdx)
				}
				seen[ip.String()] = peerIdx
			}
		}
	})

	t.Run("tiny subnet /28 with too many peers truncates", func(t *testing.T) {
		// /28 has 14 usable IPs (16 - network - broadcast)
		ipNet := mustParseCIDR(t, "192.168.1.0/28")
		peers := make([]string, 10)
		for i := range peers {
			peers[i] = net.IPv4(10, 0, 0, byte(i+1)).String()
		}

		// First peer: .14, .13 — fits
		first := ipsToStrings(computeIPCandidates(ipNet, peers, peers[0], 2))
		if len(first) != 2 || first[0] != "192.168.1.14" || first[1] != "192.168.1.13" {
			t.Errorf("first peer: got %v, want [192.168.1.14 192.168.1.13]", first)
		}

		// Peer at index 7: startOffset = 1 + 7*2 = 15, broadcast - 15 = .0 = network addr → no candidates
		late := computeIPCandidates(ipNet, peers, peers[7], 2)
		if len(late) != 0 {
			t.Errorf("peer 7 on /28: got %v, want empty (exceeds subnet)", ipsToStrings(late))
		}
	})

	t.Run("byte boundary crossing in band", func(t *testing.T) {
		// Subnet where the band crosses a .0 boundary
		ipNet := mustParseCIDR(t, "10.0.0.0/21")
		// Broadcast is 10.0.7.255, peer index 3 with 4 containers:
		// startOffset = 1 + 3*4 = 13 → 10.0.7.242, .241, .240, .239
		peers := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"}
		got := ipsToStrings(computeIPCandidates(ipNet, peers, "10.0.0.4", 4))
		want := []string{"10.0.7.242", "10.0.7.241", "10.0.7.240", "10.0.7.239"}
		if len(got) != len(want) {
			t.Fatalf("got %d IPs, want %d", len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("IP[%d]: got %s, want %s", i, got[i], want[i])
			}
		}
	})

	t.Run("/30 minimal subnet, 1 peer, 1 container", func(t *testing.T) {
		// /30 has 2 usable IPs: .1 and .2
		ipNet := mustParseCIDR(t, "10.0.0.0/30")
		got := ipsToStrings(computeIPCandidates(ipNet, []string{"10.0.0.1"}, "10.0.0.1", 1))
		if len(got) != 1 || got[0] != "10.0.0.2" {
			t.Errorf("got %v, want [10.0.0.2]", got)
		}
	})

	t.Run("/30 minimal subnet, 1 peer, 2 containers — only 1 fits", func(t *testing.T) {
		// /30 has 2 usable IPs, but broadcast-1 = .2, broadcast-2 = .1 which
		// equals the network+1... actually .1 is still in the subnet. Let's check:
		// network = .0, broadcast = .3, usable = .1 and .2
		// startOffset = 1 → candidate .2 (ok), then .1 — .1 != network .0 so it's valid
		ipNet := mustParseCIDR(t, "10.0.0.0/30")
		got := ipsToStrings(computeIPCandidates(ipNet, []string{"10.0.0.1"}, "10.0.0.1", 2))
		if len(got) != 2 || got[0] != "10.0.0.2" || got[1] != "10.0.0.1" {
			t.Errorf("got %v, want [10.0.0.2 10.0.0.1]", got)
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
