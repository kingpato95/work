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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pbDatanode "aeroDist/proto/datanode"
)

const (
	// Se asume que el ID del nodo se pasa como argumento de línea de comandos o variable de entorno
	// Por defecto, usamos un valor de ejemplo.
	defaultNodeID  = "dn1"
	datanodePort   = ":50052"
	gossipInterval = 5 * time.Second
	// Lista de direcciones de otros Datanodes para Gossip (excluyendo a sí mismo)
	peerAddrs = "dn2:50052,dn3:50052"
)

// FlightData almacena el estado del vuelo y su metadato causal.
type FlightData struct {
	State pbDatanode.FlightState
	mu    sync.RWMutex
}

// Server implementa el servicio DatanodeService.
type Server struct {
	pbDatanode.UnimplementedDatanodeServiceServer

	NodeID string
	// Almacenamiento de vuelos: FlightID -> FlightData
	storage map[string]*FlightData
	mu      sync.RWMutex

	peerClients map[string]pbDatanode.DatanodeServiceClient
}

func main() {
	log.Println("🚀 Iniciando Datanode...")

	// 1. Obtener NodeID (ej. de argumentos o env var)
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = defaultNodeID
	}
	log.Printf("ID del Datanode: %s", nodeID)

	// 2. Inicializar el servidor y los clientes peer
	s := NewDatanodeServer(nodeID)

	peerAddrs := os.Getenv("PEER_ADDRS")
	if peerAddrs == "" {
		peerAddrs = "dn2:50052,dn3:50052" // Asumiendo que dn1 tiene dn2 y dn3 como peers.
	}
	s.initializePeerClients(peerAddrs)

	// 3. Iniciar el protocolo Gossip asíncrono
	go s.startGossip()

	// 4. Configurar y lanzar el servidor gRPC
	lis, err := net.Listen("tcp", datanodePort)
	if err != nil {
		log.Fatalf("❌ Error al escuchar en el puerto %s: %v", datanodePort, err)
	}

	grpcServer := grpc.NewServer()
	pbDatanode.RegisterDatanodeServiceServer(grpcServer, s)

	log.Printf("👂 Datanode %s escuchando en %v", nodeID, lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("❌ Fallo al servir: %v", err)
	}
}

// NewDatanodeServer constructor.
func NewDatanodeServer(nodeID string) *Server {
	return &Server{
		NodeID:      nodeID,
		storage:     make(map[string]*FlightData),
		peerClients: make(map[string]pbDatanode.DatanodeServiceClient),
	}
}

// =================================================================
// LÓGICA DE CONSISTENCIA EVENTUAL (ESCRITURA)
// =================================================================

// ProcessUpdate maneja las escrituras (actualizaciones de vuelos) del Broker.
func (s *Server) ProcessUpdate(ctx context.Context, req *pbDatanode.UpdateRequest) (*pbDatanode.UpdateResponse, error) {
	update := req.GetUpdate()
	flightID := update.GetFlightId()

	s.mu.Lock()
	fd, exists := s.storage[flightID]
	if !exists {
		fd = &FlightData{State: *update}
		s.storage[flightID] = fd
		s.mu.Unlock()
		log.Printf("✅ %s: Nuevo vuelo %s creado.", s.NodeID, flightID)
		return &pbDatanode.UpdateResponse{Success: true, Message: "Vuelo creado"}, nil
	}
	s.mu.Unlock()

	fd.mu.Lock()
	defer fd.mu.Unlock()

	oldVC := fd.State.GetClock().GetVc()
	newVC := update.GetClock().GetVc()

	// Detección de Conflicto
	isConcurrent, isCausal := compareVectorClocks(oldVC, newVC, s.NodeID)

	if isCausal == 1 {
		// 1. Causalmente posterior (caso normal, aplicar y fusionar)
		s.applyUpdate(fd, update, newVC)
		log.Printf("⬆️ %s: Vuelo %s actualizado causalmente.", s.NodeID, flightID)
	} else if isCausal == -1 {
		// 2. Causalmente anterior (evento viejo, descartar, pero fusionar VC)
		s.mergeVectorClocks(oldVC, newVC)
		log.Printf("⬇️ %s: Vuelo %s ignorado (VC anterior).", s.NodeID, flightID)
		return &pbDatanode.UpdateResponse{Success: true, Message: "Actualización ignorada (VC anterior)"}, nil
	} else if isConcurrent {
		// 3. Concurrente: Detectado Conflicto. Aplicar Política Determinista.
		resolvedMsg := s.resolveConflict(fd, update)
		log.Printf("💥 %s: Conflicto en %s. Resuelto con política: %s", s.NodeID, flightID, resolvedMsg)
		// Fusionar el VC resultante
		s.applyUpdate(fd, update, newVC)
		return &pbDatanode.UpdateResponse{Success: true, Message: "Conflicto resuelto: " + resolvedMsg}, nil
	} else {
		// Si es igual o alguna otra condición de borde, fusionar y actualizar.
		s.applyUpdate(fd, update, newVC)
		log.Printf("🔄 %s: Vuelo %s actualizado/fusionado.", s.NodeID, flightID)
	}

	return &pbDatanode.UpdateResponse{Success: true, Message: "Actualización procesada"}, nil
}

// compareVectorClocks compara dos relojes vectoriales.
// Devuelve: (isConcurrent, isCausal).
// isCausal: 1 si VC1 > VC2, -1 si VC1 < VC2, 0 si no hay relación (iguales o concurrentes).
func compareVectorClocks(vc1 map[string]int64, vc2 map[string]int64, nodeID string) (bool, int) {
    // Unificar claves de ambos mapas
    allKeys := make(map[string]bool)
    for k := range vc1 { allKeys[k] = true }
    for k := range vc2 { allKeys[k] = true }

    greater := false
    less := false

    for k := range allKeys {
        v1 := vc1[k]
        v2 := vc2[k]
        if v1 > v2 { greater = true }
        if v1 < v2 { less = true }
    }

    if greater && less { return true, 0 } // Concurrentes (Conflicto)
    if greater { return false, 1 }        // VC1 es posterior
    if less { return false, -1 }          // VC1 es anterior
    return false, 0                       // Iguales
}

// applyUpdate aplica los cambios de estado (si no están vacíos) y fusiona/actualiza el VC.
func (s *Server) applyUpdate(fd *FlightData, update *pbDatanode.FlightState, newVC map[string]int64) {
	if update.GetStatus() != "" {
		fd.State.Status = update.GetStatus()
	}
	if update.GetGate() != "" {
		fd.State.Gate = update.GetGate()
	}
	// Incrementar el componente propio del VC y fusionar el VC recibido.
	s.mergeVectorClocks(fd.State.Clock.GetVc(), newVC)
	fd.State.Clock.Vc[s.NodeID]++ // Siempre incrementamos nuestro propio componente al escribir.
}

// resolveConflict aplica la política determinista.
func (s *Server) resolveConflict(fd *FlightData, update *pbDatanode.FlightState) string {
	// Política de Resolución: El estado (Cancelado > Retrasado > A Tiempo) tiene prioridad sobre la puerta.
	// Si ambos son cambios de estado, elegimos el estado de mayor prioridad.

	currentStatus := fd.State.GetStatus()
	newStatus := update.GetStatus()

	if newStatus != "" && (currentStatus == "" || newStatus == "Cancelado") {
		fd.State.Status = newStatus
		return "Prioridad: Estado (Cancelado)"
	}

	// Si el estado no cambia o es menos prioritario, aplicamos la puerta si está presente.
	if update.GetGate() != "" {
		fd.State.Gate = update.GetGate()
		return "Prioridad: Puerta (si el estado no prevaleció)"
	}

	return "No se aplicó un cambio concurrente"
}

// mergeVectorClocks toma el máximo componente por posición.
func (s *Server) mergeVectorClocks(vcTarget map[string]int64, vcSource map[string]int64) {
	for id, count := range vcSource {
		if vcTarget[id] < count {
			vcTarget[id] = count
		}
	}
}

// =================================================================
// LÓGICA DE MONOTONIC READS (LECTURA)
// =================================================================

// ReadFlightState maneja las consultas de lectura del Broker/Coordinador.
func (s *Server) ReadFlightState(ctx context.Context, req *pbDatanode.ReadRequest) (*pbDatanode.ReadResponse, error) {
	flightID := req.GetFlightId()
	clientVC := req.GetClientVersion().GetVc()

	s.mu.RLock()
	fd, exists := s.storage[flightID]
	s.mu.RUnlock()

	if !exists {
		return &pbDatanode.ReadResponse{CurrentState: nil, Consistent: true}, status.Errorf(codes.NotFound, "Vuelo no encontrado")
	}

	fd.mu.RLock()
	currentVC := fd.State.GetClock().GetVc()
	fd.mu.RUnlock()

	// Verificar Monotonic Reads: El VC local debe ser >= al VC del cliente.
	// isCausal = 1 si currentVC > clientVC, 0 si son iguales o concurrentes, -1 si clientVC > currentVC
	_, isCausal := compareVectorClocks(currentVC, clientVC, s.NodeID)

	if isCausal == -1 {
		// Caso de Monotonic Read fallida: El Datanode está atrasado respecto al cliente.
		log.Printf("⚠️ %s: Lectura Monotonic fallida para %s. DN atrasado.", s.NodeID, flightID)

		// Estrategia: Podríamos esperar un breve momento o devolver un error para que el cliente reintente. [cite: 213]
		// Aquí, devolvemos un error de "intente de nuevo".
		return &pbDatanode.ReadResponse{Consistent: false}, status.Errorf(codes.Unavailable, "Datanode atrasado. Reintente.")
	}

	// Lectura exitosa (es posterior o igual)
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	return &pbDatanode.ReadResponse{CurrentState: &fd.State, Consistent: true}, nil
}

// =================================================================
// LÓGICA DE GOSSIP (SINCRONIZACIÓN)
// =================================================================

// initializePeerClients establece las conexiones con otros Datanodes.
func (s *Server) initializePeerClients(addrs string) {
	addrList := strings.Split(addrs, ",")
	for _, addr := range addrList {
		parts := strings.Split(addr, ":")
		if len(parts) == 2 {
			peerID := parts[0]
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("⚠️ No se pudo conectar a Peer %s: %v", peerID, err)
				continue
			}
			s.peerClients[peerID] = pbDatanode.NewDatanodeServiceClient(conn)
		}
	}
}

// startGossip inicia el proceso periódico de intercambio de actualizaciones.
func (s *Server) startGossip() {
	ticker := time.NewTicker(gossipInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.gossipToRandomPeer()
	}
}

// gossipToRandomPeer selecciona un peer al azar y le envía las actualizaciones.
func (s *Server) gossipToRandomPeer() {
	if len(s.peerClients) == 0 {
		return
	}

	// 1. Seleccionar un peer al azar
	peers := make([]string, 0, len(s.peerClients))
	for id := range s.peerClients {
		peers = append(peers, id)
	}
	targetID := peers[rand.Intn(len(peers))]
	client := s.peerClients[targetID]

	// 2. Preparar las actualizaciones a enviar (en un sistema real, solo enviaríamos cambios recientes)
	s.mu.RLock()
	updates := make(map[string]*pbDatanode.FlightState)
	for id, fd := range s.storage {
		fd.mu.RLock()
		updates[id] = &fd.State
		fd.mu.RUnlock()
	}
	s.mu.RUnlock()

	req := &pbDatanode.GossipRequest{
		SenderId: s.NodeID,
		Updates:  updates,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// 3. Enviar Gossip
	resp, err := client.ExchangeGossip(ctx, req)
	if err != nil {
		log.Printf("❌ Gossip falló con %s: %v", targetID, err)
		return
	}

	// 4. Procesar counter_updates (Anti-Entropy)
	s.processGossipUpdates(resp.GetCounterUpdates(), targetID)
}

// ExchangeGossip maneja la recepción de actualizaciones de otros Datanodes.
func (s *Server) ExchangeGossip(ctx context.Context, req *pbDatanode.GossipRequest) (*pbDatanode.GossipResponse, error) {
	senderID := req.GetSenderId()
	log.Printf("🔄 Recibiendo Gossip de %s. Procesando %d vuelos.", senderID, len(req.GetUpdates()))

	// El Datanode aplica los updates recibidos a su almacenamiento local (lo que desencadenará
	// la lógica de detección/resolución de conflictos y fusión de VC).
	s.processGossipUpdates(req.GetUpdates(), senderID)

	// En un sistema real, devolveríamos un subconjunto de nuestros datos que creemos que el emisor no tiene.
	return &pbDatanode.GossipResponse{Success: true, CounterUpdates: nil}, nil
}

// processGossipUpdates itera sobre las actualizaciones recibidas y las aplica localmente.
func (s *Server) processGossipUpdates(updates map[string]*pbDatanode.FlightState, senderID string) {
	for _, update := range updates {
		// Llamamos a ProcessUpdate internamente, usando un contexto vacío, para reutilizar
		// la lógica de detección de conflictos y fusión de VC.
		s.ProcessUpdate(context.Background(), &pbDatanode.UpdateRequest{Update: update})
	}
}
