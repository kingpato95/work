package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pbConsensus "aeroDist/proto/consensus"
)

const (
	defaultNodeID      = "atc1"
	atcNodePort        = ":50053"
	electionTimeoutMin = 500 * time.Millisecond
	electionTimeoutMax = 1000 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)


type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type Server struct {
	pbConsensus.UnimplementedConsensusServiceServer

	NodeID      string
	PeerClients map[string]pbConsensus.ConsensusServiceClient

	mu       sync.Mutex
	Role     Role
	Term     int64
	VotedFor string
	
	Log      []*pbConsensus.LogEntry 

	CommitIndex int64
	LastApplied int64

	NextIndex  map[string]int64
	MatchIndex map[string]int64

	electionTimeout *time.Timer
	heartbeatC      chan struct{}
}

func main() {
	log.Println("Iniciando Nodo de Control de Tráfico Aéreo (Consenso)...")

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = defaultNodeID
	}

	s := NewConsensusServer(nodeID)

	peerAddrs := os.Getenv("PEER_ADDRS")
	if peerAddrs == "" {
		peerAddrs = "atc2:50053,atc3:50053"
	}
	s.initializePeerClients(peerAddrs)

	go s.run()

	lis, err := net.Listen("tcp", atcNodePort)
	if err != nil {
		log.Fatalf("Error al escuchar en el puerto %s: %v", atcNodePort, err)
	}

	grpcServer := grpc.NewServer()
	pbConsensus.RegisterConsensusServiceServer(grpcServer, s)

	log.Printf("👂 Nodo ATC %s escuchando en %v", nodeID, lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Fallo al servir: %v", err)
	}
}

func NewConsensusServer(nodeID string) *Server {
	return &Server{
		NodeID:          nodeID,
		Role:            Follower,
		Term:            0,
		Log:             make([]*pbConsensus.LogEntry, 0), // Inicializar slice de punteros
		PeerClients:     make(map[string]pbConsensus.ConsensusServiceClient),
		electionTimeout: time.NewTimer(getElectionTimeout()),
		heartbeatC:      make(chan struct{}),
	}
}

func (s *Server) run() {
	for {
		s.mu.Lock()
		role := s.Role
		s.mu.Unlock()

		switch role {
		case Follower:
			s.runFollower()
		case Candidate:
			s.runCandidate()
		case Leader:
			s.runLeader()
		}
	}
}

func (s *Server) runFollower() {
	log.Printf("⚙️ %s: Soy Follower (Term %d). Esperando Heartbeat.", s.NodeID, s.Term)
	s.electionTimeout.Reset(getElectionTimeout())

	for s.Role == Follower {
		select {
		case <-s.electionTimeout.C:
			s.mu.Lock()
			s.Role = Candidate
			s.mu.Unlock()
			log.Printf("⌛ %s: Election Timeout. Transicionando a Candidate.", s.NodeID)
			return
		case <-s.heartbeatC:
			s.electionTimeout.Reset(getElectionTimeout())
		}
	}
}

func (s *Server) runCandidate() {
	s.startElection()

	votesReceived := 1
	votesNeeded := (len(s.PeerClients)+1)/2 + 1

	electionTimer := time.NewTimer(getElectionTimeout())
	defer electionTimer.Stop()

	for s.Role == Candidate {
		select {
		case <-electionTimer.C:
			log.Printf("🔄 %s: Election fallida (Timeout). Reintentando.", s.NodeID)
			return
		case <-s.heartbeatC:
			s.mu.Lock()
			s.Role = Follower
			s.VotedFor = ""
			s.mu.Unlock()
			log.Printf("👑 %s: Nuevo Líder encontrado. Transicionando a Follower.", s.NodeID)
			return
		case <-time.After(1 * time.Millisecond):
			s.mu.Lock()
			if votesReceived >= votesNeeded {
				s.Role = Leader
				log.Printf("🏆 %s: Ganó la elección con %d votos. Transicionando a Leader (Term %d).", s.NodeID, votesReceived, s.Term)
			}
			s.mu.Unlock()
			if s.Role == Leader {
				return
			}
		}
	}
}

func (s *Server) runLeader() {
	log.Printf("🌟 %s: Soy el Líder (Term %d). Enviando Heartbeats...", s.NodeID, s.Term)
	s.initializeLeaderState()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	s.sendHeartbeats()

	for s.Role == Leader {
		select {
		case <-ticker.C:
			s.sendHeartbeats()
		case <-s.heartbeatC:
			return
		}
	}
}

func (s *Server) startElection() {
	s.mu.Lock()
	s.Term++
	s.Role = Candidate
	s.VotedFor = s.NodeID
	currentTerm := s.Term
	lastLogIndex, lastLogTerm := s.getLastLogInfo()
	s.mu.Unlock()

	log.Printf("🗳️ %s: Iniciando elección para Term %d.", s.NodeID, currentTerm)

	for id, client := range s.PeerClients {
		go func(id string, client pbConsensus.ConsensusServiceClient) {
			req := &pbConsensus.RequestVoteRequest{
				CandidateId:  s.NodeID,
				CurrentTerm:  currentTerm,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			ctx, cancel := context.WithTimeout(context.Background(), heartbeatInterval)
			defer cancel()

			resp, err := client.RequestVote(ctx, req)
			if err != nil {
				return
			}

			s.mu.Lock()
			
			if resp.GetCurrentTerm() > currentTerm {
				s.Term = resp.GetCurrentTerm()
				s.Role = Follower
				s.VotedFor = ""
				s.heartbeatC <- struct{}{}
			}
			s.mu.Unlock()
		}(id, client)
	}
}

func (s *Server) sendHeartbeats() {
	s.mu.Lock()
	if s.Role != Leader {
		s.mu.Unlock()
		return
	}
	currentTerm := s.Term
	leaderID := s.NodeID
	commitIndex := s.CommitIndex
	s.mu.Unlock()

	for id, client := range s.PeerClients {
		go func(id string, client pbConsensus.ConsensusServiceClient) {
			req := &pbConsensus.AppendEntriesRequest{
				LeaderId:     leaderID,
				CurrentTerm:  currentTerm,
				PrevLogIndex: s.MatchIndex[id],
				PrevLogTerm:  currentTerm, // Simplificado
				Entries:      []*pbConsensus.LogEntry{},
				LeaderCommit: commitIndex,
			}

			ctx, cancel := context.WithTimeout(context.Background(), heartbeatInterval)
			defer cancel()

			resp, err := client.AppendEntries(ctx, req)
			if err != nil {
				return
			}

			s.handleAppendEntriesResponse(resp)
		}(id, client)
	}
}


func (s *Server) ProposeAction(ctx context.Context, req *pbConsensus.ProposeRequest) (*pbConsensus.ProposeResponse, error) {
	s.mu.Lock()
	if s.Role != Leader {
		s.mu.Unlock()
		return &pbConsensus.ProposeResponse{Success: false, Message: "Redireccionar: No soy líder"}, nil
	}

	newIndex := s.getLastLogIndex() + 1
	// Creamos la entrada usando el tipo gRPC
	newEntry := &pbConsensus.LogEntry{
		Term:  s.Term,
		Index: newIndex,
		Data:  req.ProposalData,
	}

	// Añadimos al log (que ahora es []*pbConsensus.LogEntry)
	s.Log = append(s.Log, newEntry)
	s.mu.Unlock()

	// --- INICIO LÓGICA DE REPLICACIÓN REAL ---
	successCount := 1
	peersCount := len(s.PeerClients)
	quorum := (peersCount / 2) + 1
	resultCh := make(chan bool, peersCount)

	for id, client := range s.PeerClients {
		go func(peerID string, c pbConsensus.ConsensusServiceClient) {
			s.mu.Lock()
			prevIndex := s.getLastLogIndex() - 1
			currentTerm := s.Term
			commitIndex := s.CommitIndex
			s.mu.Unlock()
			
			appendReq := &pbConsensus.AppendEntriesRequest{
				LeaderId:     s.NodeID,
				CurrentTerm:  currentTerm,
				LeaderCommit: commitIndex,
				Entries:      []*pbConsensus.LogEntry{newEntry},
				PrevLogIndex: prevIndex,
				PrevLogTerm:  currentTerm,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			resp, err := c.AppendEntries(ctx, appendReq)
			if err == nil && resp.Success {
				resultCh <- true
			} else {
				resultCh <- false
			}
		}(id, client)
	}

	timeout := time.After(500 * time.Millisecond)
	
LoopEsperarVotos:
	for i := 0; i < peersCount; i++ {
		select {
		case success := <-resultCh:
			if success {
				successCount++
			}
			if successCount >= quorum {
				break LoopEsperarVotos 
			}
		case <-timeout:
			break LoopEsperarVotos
		}
	}

	if successCount < quorum {
		return &pbConsensus.ProposeResponse{Success: false, Message: "Fallo: No se alcanzó quórum"}, nil
	}

	s.mu.Lock()
	if newIndex > s.CommitIndex {
		s.CommitIndex = newIndex
	}
	s.mu.Unlock()

	stateMsg := s.applyLogEntry(newEntry)

	return &pbConsensus.ProposeResponse{
		Success: true,
		Message: "Decisión comprometida por quórum.",
		NewState: &pbConsensus.CriticalResourceState{
			ResourceId:       "Pista 01",
			AssignedToFlight: stateMsg,
			LastCommitIndex:  s.CommitIndex,
		},
	}, nil
}

func (s *Server) RequestVote(ctx context.Context, req *pbConsensus.RequestVoteRequest) (*pbConsensus.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.CurrentTerm > s.Term {
		s.Term = req.CurrentTerm
		s.Role = Follower
		s.VotedFor = ""
	}

	voteGranted := false
	if req.CurrentTerm == s.Term && (s.VotedFor == "" || s.VotedFor == req.CandidateId) {
		lastLogIndex, lastLogTerm := s.getLastLogInfo()
		if req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			s.VotedFor = req.CandidateId
			voteGranted = true
			s.electionTimeout.Reset(getElectionTimeout())
			s.heartbeatC <- struct{}{}
		}
	}

	return &pbConsensus.RequestVoteResponse{
		NodeId:      s.NodeID,
		CurrentTerm: s.Term,
		VoteGranted: voteGranted,
	}, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *pbConsensus.AppendEntriesRequest) (*pbConsensus.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.CurrentTerm < s.Term {
		return &pbConsensus.AppendEntriesResponse{Success: false, CurrentTerm: s.Term}, nil
	}

	s.Term = req.CurrentTerm
	s.Role = Follower
	s.VotedFor = ""
	s.heartbeatC <- struct{}{}

	if len(req.Entries) > 0 {
		// Lógica simplificada: Agregar todo lo que llegue (asumimos consistencia para el lab)
		for _, entry := range req.Entries {
			s.Log = append(s.Log, entry)
		}
	}

	if req.LeaderCommit > s.CommitIndex {
		s.CommitIndex = min(req.LeaderCommit, s.getLastLogIndex())
	}

	return &pbConsensus.AppendEntriesResponse{Success: true, CurrentTerm: s.Term, LastLogIndex: s.getLastLogIndex()}, nil
}

// =================================================================
// FUNCIONES AUXILIARES
// =================================================================

func (s *Server) initializePeerClients(addrs string) {
	addrList := strings.Split(addrs, ",")
	for _, addr := range addrList {
		parts := strings.Split(addr, ":")
		if len(parts) == 2 {
			peerID := parts[0]
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("❌ Error conectando con Peer %s: %v", peerID, err)
				continue
			}
			s.PeerClients[peerID] = pbConsensus.NewConsensusServiceClient(conn)
		}
	}
}

func (s *Server) getLastLogInfo() (int64, int64) {
	if len(s.Log) == 0 {
		return 0, 0
	}
	lastEntry := s.Log[len(s.Log)-1]
	return lastEntry.Index, lastEntry.Term
}

func (s *Server) getLastLogIndex() int64 {
	if len(s.Log) == 0 {
		return 0
	}
	return s.Log[len(s.Log)-1].Index
}

func getElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

func (s *Server) initializeLeaderState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.NextIndex = make(map[string]int64)
	s.MatchIndex = make(map[string]int64)
	nextIndex := s.getLastLogIndex() + 1
	for id := range s.PeerClients {
		s.NextIndex[id] = nextIndex
		s.MatchIndex[id] = 0
	}
}

func (s *Server) handleAppendEntriesResponse(resp *pbConsensus.AppendEntriesResponse) {
	// Placeholder
}

func (s *Server) applyLogEntry(entry *pbConsensus.LogEntry) string {
	return "Vuelo asignado: " + entry.Data
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}