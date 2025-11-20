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
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond // Para el Líder
)

// Role define el estado de un nodo ATC.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// LogEntry representa una decisión atómica en el log.
type LogEntry struct {
	Term  int64
	Index int64
	Data  string // La decisión: "ASSIGN Pista 01 TO IB-6833"
}

// Server implementa el servicio ConsensusService y contiene el estado del nodo.
type Server struct {
	pbConsensus.UnimplementedConsensusServiceServer

	NodeID      string
	PeerClients map[string]pbConsensus.ConsensusServiceClient

	mu       sync.Mutex
	Role     Role
	Term     int64      // Término actual
	VotedFor string     // Votado en el término actual (vacío o ID de candidato)
	Log      []LogEntry // Log replicado inmutable

	CommitIndex int64 // Último índice conocido como committed
	LastApplied int64 // Último índice aplicado a la máquina de estado

	// Lógica de Liderazgo (solo para el Líder)
	NextIndex  map[string]int64 // Índice del siguiente LogEntry a enviar a cada seguidor
	MatchIndex map[string]int64 // Índice más alto conocido como replicado en cada seguidor

	// Temporizadores y señales
	electionTimeout *time.Timer
	heartbeatC      chan struct{} // Canal para resetear el timeout
}

func main() {
	log.Println("🚀 Iniciando Nodo de Control de Tráfico Aéreo (Consenso)...")

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = defaultNodeID
	}

	s := NewConsensusServer(nodeID)

	peerAddrs := os.Getenv("PEER_ADDRS")
	if peerAddrs == "" {
		peerAddrs = "atc2:50053,atc3:50053" // Ejemplo
	}
	s.initializePeerClients(peerAddrs)

	// Iniciar la goroutine de Consenso (maneja roles, timeouts y elecciones)
	go s.run()

	lis, err := net.Listen("tcp", atcNodePort)
	if err != nil {
		log.Fatalf("❌ Error al escuchar en el puerto %s: %v", atcNodePort, err)
	}

	grpcServer := grpc.NewServer()
	pbConsensus.RegisterConsensusServiceServer(grpcServer, s)

	log.Printf("👂 Nodo ATC %s escuchando en %v", nodeID, lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("❌ Fallo al servir: %v", err)
	}
}

// NewConsensusServer constructor.
func NewConsensusServer(nodeID string) *Server {
	return &Server{
		NodeID:          nodeID,
		Role:            Follower,
		Term:            0,
		Log:             make([]LogEntry, 0),
		PeerClients:     make(map[string]pbConsensus.ConsensusServiceClient),
		electionTimeout: time.NewTimer(getElectionTimeout()),
		heartbeatC:      make(chan struct{}),
	}
}

// =================================================================
// PROTOCOLO DE CONCENSO CENTRAL
// =================================================================

// run es el loop principal que gestiona el rol del nodo y los timeouts.
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

// runFollower espera por heartbeats o inicia la elección si hay timeout.
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
			return // Sale del loop de Follower para ejecutar runCandidate
		case <-s.heartbeatC:
			// Recibió Heartbeat o RequestVote válido: reiniciar timeout
			s.electionTimeout.Reset(getElectionTimeout())
		}
	}
}

// runCandidate inicia una nueva elección.
func (s *Server) runCandidate() {
	s.startElection()

	votesReceived := 1                          // Vota por sí mismo
	votesNeeded := (len(s.PeerClients)+1)/2 + 1 // Regla de Quórum (N/2 + 1)

	// Temporizador para la nueva elección si no se gana.
	electionTimer := time.NewTimer(getElectionTimeout())
	defer electionTimer.Stop()

	for s.Role == Candidate {
		select {
		case <-electionTimer.C:
			// Timeout: iniciar nueva elección (el loop exterior llamará startElection de nuevo)
			log.Printf("🔄 %s: Election fallida (Timeout). Reintentando.", s.NodeID)
			return
		case <-s.heartbeatC:
			// Recibió un AppendEntries (Heartbeat) de un Líder legítimo
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
				return // Sale del loop de Candidate para ejecutar runLeader
			}
		}
	}
}

// runLeader envía heartbeats periódicamente.
func (s *Server) runLeader() {
	log.Printf("🌟 %s: Soy el Líder (Term %d). Enviando Heartbeats...", s.NodeID, s.Term)
	s.initializeLeaderState()

	// Loop de Heartbeat
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	s.sendHeartbeats() // Enviar primer heartbeat inmediatamente

	for s.Role == Leader {
		select {
		case <-ticker.C:
			s.sendHeartbeats()
		case <-s.heartbeatC:
			// Si recibe un mensaje RequestVote con un término mayor, se convierte en Follower.
			return
		}
	}
}

// startElection incrementa el término e inicia la votación.
func (s *Server) startElection() {
	s.mu.Lock()
	s.Term++
	s.Role = Candidate
	s.VotedFor = s.NodeID // Votar por sí mismo
	currentTerm := s.Term
	lastLogIndex, lastLogTerm := s.getLastLogInfo()
	s.mu.Unlock()

	log.Printf("🗳️ %s: Iniciando elección para Term %d.", s.NodeID, currentTerm)

	// Enviar RequestVote a todos los pares
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
				log.Printf("❌ Error votación con %s: %v", id, err)
				return
			}

			s.mu.Lock()
			if resp.GetVoteGranted() && s.Role == Candidate && resp.GetCurrentTerm() == currentTerm {
				// Incrementar votos recibidos (Lógica de votesReceived no implementada en este bloque, debe ser externa)
			} else if resp.GetCurrentTerm() > currentTerm {
				s.Term = resp.GetCurrentTerm()
				s.Role = Follower
				s.VotedFor = ""
				s.heartbeatC <- struct{}{} // Señal para salir del loop Candidate
			}
			s.mu.Unlock()
		}(id, client)
	}
}

// sendHeartbeats envía AppendEntries sin entradas de log.
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
			// Preparamos el heartbeat
			req := &pbConsensus.AppendEntriesRequest{
				LeaderId:     leaderID,
				CurrentTerm:  currentTerm,
				PrevLogIndex: s.MatchIndex[id],          // Usa MatchIndex para saber dónde continuar
				PrevLogTerm:  currentTerm,               // Simplificado
				Entries:      []*pbConsensus.LogEntry{}, // Vacío para Heartbeat
				LeaderCommit: commitIndex,
			}

			ctx, cancel := context.WithTimeout(context.Background(), heartbeatInterval)
			defer cancel()

			resp, err := client.AppendEntries(ctx, req)
			if err != nil {
				// Fallo de red: Ignorar o registrar fallo de nodo
				return
			}

			s.handleAppendEntriesResponse(resp)
		}(id, client)
	}
}

// =================================================================
// IMPLEMENTACIÓN DE RPCS (Interfaz gRPC)
// =================================================================

// ProposeAction maneja la solicitud de escritura crítica del Broker.
func (s *Server) ProposeAction(ctx context.Context, req *pbConsensus.ProposeRequest) (*pbConsensus.ProposeResponse, error) {
	s.mu.Lock()
	if s.Role != Leader {
		leaderAddr := "" // Lógica para encontrar dirección del líder actual (no implementada aquí)
		s.mu.Unlock()
		return &pbConsensus.ProposeResponse{Success: false, Message: "Redireccionar: No soy líder", LeaderAddress: leaderAddr}, nil
	}

	// 1. Crear nueva entrada de log
	newIndex := s.getLastLogIndex() + 1
	newEntry := pbConsensus.LogEntry{
		Term:  s.Term,
		Index: newIndex,
		Data:  req.ProposalData,
	}

	s.Log = append(s.Log, LogEntry(newEntry))
	s.mu.Unlock()

	// 2. Replicar la entrada (Lógica de replicación en AppendEntries no implementada en este bloque)
	// 3. Esperar el Commit por quórum

	// Simulamos el commit por simplicidad
	time.Sleep(100 * time.Millisecond)
	s.CommitIndex = newIndex

	// 4. Aplicar al estado final
	state := s.applyLogEntry(newEntry)

	return &pbConsensus.ProposeResponse{
		Success: true,
		Message: "Decisión comprometida por quórum.",
		NewState: &pbConsensus.CriticalResourceState{
			ResourceId:       "Pista 01",
			AssignedToFlight: state,
			LastCommitIndex:  s.CommitIndex,
		},
	}, nil
}

// RequestVote maneja las peticiones de voto de otros candidatos.
func (s *Server) RequestVote(ctx context.Context, req *pbConsensus.RequestVoteRequest) (*pbConsensus.RequestVoteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Regla de Término
	if req.CurrentTerm > s.Term {
		s.Term = req.CurrentTerm
		s.Role = Follower
		s.VotedFor = ""
	}

	// 2. Regla de Voto y Log
	voteGranted := false
	if req.CurrentTerm == s.Term && (s.VotedFor == "" || s.VotedFor == req.CandidateId) {
		lastLogIndex, lastLogTerm := s.getLastLogInfo()
		if req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			s.VotedFor = req.CandidateId
			voteGranted = true
			s.electionTimeout.Reset(getElectionTimeout()) // Reiniciar el temporizador
			s.heartbeatC <- struct{}{}                    // Señal para salir del loop de espera si es Follower
		}
	}

	return &pbConsensus.RequestVoteResponse{
		NodeId:      s.NodeID,
		CurrentTerm: s.Term,
		VoteGranted: voteGranted,
	}, nil
}

// AppendEntries maneja Heartbeats y Replicación de Log.
func (s *Server) AppendEntries(ctx context.Context, req *pbConsensus.AppendEntriesRequest) (*pbConsensus.AppendEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Regla de Término
	if req.CurrentTerm < s.Term {
		return &pbConsensus.AppendEntriesResponse{Success: false, CurrentTerm: s.Term}, nil
	}

	// Si el término del Líder es >= al nuestro, siempre nos convertimos en Follower (o mantenemos).
	s.Term = req.CurrentTerm
	s.Role = Follower
	s.VotedFor = ""
	s.heartbeatC <- struct{}{} // Reiniciar temporizador de elección

	// 2. Verificación de Consistencia del Log (simplificado)
	// Debe verificar que el log en prevLogIndex tenga el término prevLogTerm.

	// 3. Apendizar nuevas entradas (simplificado)
	if len(req.Entries) > 0 {
		// Lógica para borrar entradas conflictivas y apendizar nuevas
	}

	// 4. Actualizar CommitIndex
	if req.LeaderCommit > s.CommitIndex {
		s.CommitIndex = min(req.LeaderCommit, s.getLastLogIndex())
		// Aplicar entradas pendientes a la máquina de estado (applyLogEntry)
	}

	return &pbConsensus.AppendEntriesResponse{Success: true, CurrentTerm: s.Term, LastLogIndex: s.getLastLogIndex()}, nil
}

// =================================================================
// FUNCIONES AUXILIARES
// =================================================================

// initializePeerClients establece las conexiones con otros Nodos ATC.
func (s *Server) initializePeerClients(addrs string) {
	addrList := strings.Split(addrs, ",")
	for _, addr := range addrList {
		parts := strings.Split(addr, ":")
		if len(parts) == 2 {
			peerID := parts[0]
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("❌ Error fatal al conectar con Peer ATC %s: %v", peerID, err)
			}
			s.PeerClients[peerID] = pbConsensus.NewConsensusServiceClient(conn)
		}
	}
}

// getLastLogInfo devuelve el índice y término del último log.
func (s *Server) getLastLogInfo() (int64, int64) {
	if len(s.Log) == 0 {
		return 0, 0
	}
	lastEntry := s.Log[len(s.Log)-1]
	return lastEntry.Index, lastEntry.Term
}

// getLastLogIndex devuelve el índice del último log.
func (s *Server) getLastLogIndex() int64 {
	if len(s.Log) == 0 {
		return 0
	}
	return s.Log[len(s.Log)-1].Index
}

// getElectionTimeout devuelve un timeout aleatorio entre min y max.
func getElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

// initializeLeaderState inicializa el estado de seguimiento después de ganar la elección.
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

// handleAppendEntriesResponse maneja la respuesta del seguidor (para el líder).
func (s *Server) handleAppendEntriesResponse(resp *pbConsensus.AppendEntriesResponse) {
	// Lógica para decrementar NextIndex si falló (resp.Success == false) o incrementar MatchIndex si fue exitoso
}

// applyLogEntry aplica una entrada del log a la máquina de estado (asignación de pista).
func (s *Server) applyLogEntry(entry pbConsensus.LogEntry) string {
	// Esta función simularía la aplicación real del recurso.
	// Ej: Extraer "Pista X" y "Vuelo Y" de entry.Data y actualizar el estado interno.
	return "Vuelo asignado según log: " + entry.Data
}

// min devuelve el mínimo entre dos int64.
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
