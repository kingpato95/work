package main

import (
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pbCoordinator "aeroDist/proto/coordinator"
	pbDatanode "aeroDist/proto/datanode"
)

const (
	coordinatorPort = ":50054"
	brokerAddr      = "broker:50051"
	// Mapa de direcciones de Datanodes para la redirección de lectura
	datanodeAddrs = "dn1:50052,dn2:50052,dn3:50052"
	sessionTTL    = 5 * time.Minute // TTL para afinidad de sesión [cite: 68]
)

type AffinitySession struct {
	DatanodeID string
	Expiry     time.Time
}

type Server struct {
	pbCoordinator.UnimplementedCoordinatorServiceServer

	brokerClient pbDatanode.DatanodeServiceClient // Cliente para comunicarse con el Broker Central [cite: 77]

	// Mapa de Clientes gRPC a Datanodes (para la lectura afín)
	dnClientMap map[string]pbDatanode.DatanodeServiceClient

	// Tabla de afinidad de sesión: ClientID -> AffinitySession
	affinityTable map[string]AffinitySession
	mu            sync.RWMutex
}

func main() {
	log.Println("🚀 Iniciando Coordinador (Gateway de Check-in)...")

	brokerClient, err := initializeBrokerClient(brokerAddr)
	if err != nil {
		log.Fatalf("❌ Error fatal al conectar con Broker: %v", err)
	}
	log.Println("✅ Conexión con Broker Central establecida.")

	dnClientMap := initializeDatanodeClientMap(datanodeAddrs)
	if len(dnClientMap) == 0 {
		log.Fatal("❌ Error: No se pudo conectar a ningún Datanode.")
	}
	log.Printf("✅ Conexiones a %d Datanodes inicializadas.", len(dnClientMap))

	s := &Server{
		brokerClient:  brokerClient,
		dnClientMap:   dnClientMap,
		affinityTable: make(map[string]AffinitySession),
	}

	// Inicia el recolector de basura de sesiones TTL
	go s.cleanupSessions()

	lis, err := net.Listen("tcp", coordinatorPort)
	if err != nil {
		log.Fatalf("❌ Error al escuchar en el puerto %s: %v", coordinatorPort, err)
	}

	grpcServer := grpc.NewServer()
	pbCoordinator.RegisterCoordinatorServiceServer(grpcServer, s)

	log.Printf("👂 Coordinador escuchando en %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("❌ Fallo al servir: %v", err)
	}
}

// CheckIn maneja la operación de escritura y establece la afinidad de sesión.
// CheckIn maneja la operación de escritura y establece la afinidad de sesión.
func (s *Server) CheckIn(ctx context.Context, req *pbCoordinator.CheckInRequest) (*pbCoordinator.CheckInResponse, error) {
	log.Printf("📝 Recibiendo Check-in de Cliente %s para Vuelo %s", req.ClientId, req.FlightId)

	// 1. Seleccionar un Datanode ESPECÍFICO
	selectedDatanodeID := selectDatanodeID(s.dnClientMap)
	if selectedDatanodeID == "" {
		return nil, status.Errorf(codes.Unavailable, "No Datanodes disponibles.")
	}

	// 2. Obtener el cliente directo de ese Datanode (¡NO EL DEL BROKER!)
	dnClient, exists := s.dnClientMap[selectedDatanodeID]
	if !exists {
		return nil, status.Errorf(codes.Internal, "Cliente Datanode %s no encontrado", selectedDatanodeID)
	}

	// 3. Construir la actualización
	updateReq := &pbDatanode.UpdateRequest{
		Update: &pbDatanode.FlightState{
			FlightId: req.FlightId,
			Gate:     "CHECKED-IN",
			Status:   "Seat: " + req.SelectedSeat,
            // Inicializar VC vacío para escritura nueva
            Clock:    &pbDatanode.VectorClock{Vc: make(map[string]int64)}, 
		},
	}

	// 4. Enviar escritura DIRECTA al Datanode seleccionado
    // (Esto garantiza que el dato esté ahí cuando vayamos a leerlo después)
	log.Printf("🚀 Enviando Check-in DIRECTO a %s...", selectedDatanodeID)
	respDN, err := dnClient.ProcessUpdate(ctx, updateReq)
	
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Error al escribir en Datanode %s: %v", selectedDatanodeID, err)
	}

	if !respDN.Success {
		return &pbCoordinator.CheckInResponse{Success: false, Message: respDN.Message}, nil
	}

	// 5. Registrar la afinidad (Ahora sí es seguro)
	s.registerAffinity(req.ClientId, selectedDatanodeID)
	log.Printf("🔗 Afinidad registrada: %s -> %s", req.ClientId, selectedDatanodeID)

	return &pbCoordinator.CheckInResponse{
		Success: true, 
		Message: "Check-in exitoso. Asiento: " + req.SelectedSeat, 
		UpdatedState: updateReq.Update,
	}, nil
}

// GetBoardingPass maneja la lectura de confirmación (RYW).
func (s *Server) GetBoardingPass(ctx context.Context, req *pbCoordinator.GetBoardingPassRequest) (*pbCoordinator.GetBoardingPassResponse, error) {
	log.Printf("🗂️ Recibiendo solicitud de Boarding Pass de Cliente %s", req.ClientId)

	// 1. Verificar afinidad de sesión
	session := s.getAffinity(req.ClientId)

	var readClient pbDatanode.DatanodeServiceClient
	var targetNodeID string

	if session.DatanodeID != "" && time.Now().Before(session.Expiry) {
		// Afinidad activa: Redirigir al Datanode de sesión
		readClient = s.dnClientMap[session.DatanodeID]
		targetNodeID = session.DatanodeID
		log.Printf("➡️ Redirigiendo lectura RYW de %s a Datanode afín %s", req.ClientId, targetNodeID)
	} else {
		// Sin afinidad o expirada: Redirigir al Broker para Round Robin
		readClient = s.brokerClient // Usamos el cliente del Broker
		targetNodeID = "Broker Central"
		log.Printf("➡️ Redirigiendo lectura de %s a Broker (sin afinidad activa)", req.ClientId)
		// Borrar si estaba expirada
		if session.DatanodeID != "" {
			s.deleteAffinity(req.ClientId)
		}
	}

	// 2. Ejecutar la lectura (simulando una ReadRequest sin VC inicial para RYW)
	readReq := &pbDatanode.ReadRequest{
		FlightId: req.FlightId,
		ClientVersion: &pbDatanode.VectorClock{
			Vc: make(map[string]int64),
		},
	}

	respDN, err := readClient.ReadFlightState(ctx, readReq)
	if err != nil {
		log.Printf("❌ Error en la lectura desde %s: %v", targetNodeID, err)
		return nil, status.Errorf(codes.Internal, "Fallo en la lectura de Boarding Pass.")
	}

	log.Printf("✅ Lectura de Boarding Pass exitosa desde %s. Validando RYW...", targetNodeID)
	return &pbCoordinator.GetBoardingPassResponse{BoardingPassState: respDN.CurrentState}, nil
}

// =================================================================
// GESTIÓN INTERNA DE AFINIDAD
// =================================================================

func (s *Server) registerAffinity(clientID string, datanodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.affinityTable[clientID] = AffinitySession{
		DatanodeID: datanodeID,
		Expiry:     time.Now().Add(sessionTTL),
	}
}

func (s *Server) getAffinity(clientID string) AffinitySession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.affinityTable[clientID]
	if !ok {
		return AffinitySession{}
	}
	return session
}

func (s *Server) deleteAffinity(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.affinityTable, clientID)
}

// cleanupSessions elimina las sesiones expiradas.
func (s *Server) cleanupSessions() {
	ticker := time.NewTicker(sessionTTL / 5) // Revisar cada quinto del TTL
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		deletedCount := 0
		for clientID, session := range s.affinityTable {
			if now.After(session.Expiry) {
				delete(s.affinityTable, clientID)
				deletedCount++
			}
		}
		s.mu.Unlock()
		if deletedCount > 0 {
			log.Printf("🧹 Limpieza de sesiones: %d sesiones RYW expiradas eliminadas.", deletedCount)
		}
	}
}

// =================================================================
// INICIALIZACIÓN DE CLIENTES
// =================================================================

func initializeBrokerClient(addr string) (pbDatanode.DatanodeServiceClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	// El Broker implementa el ProcessUpdate (similar al Datanode)
	return pbDatanode.NewDatanodeServiceClient(conn), nil
}

func initializeDatanodeClientMap(addrs string) map[string]pbDatanode.DatanodeServiceClient {
	clientMap := make(map[string]pbDatanode.DatanodeServiceClient)
	addrList := strings.Split(addrs, ",")

	// Asumimos que las direcciones son "id:port" (ej: "dn1:50052")
	for _, addr := range addrList {
		parts := strings.Split(addr, ":")
		if len(parts) != 2 {
			log.Printf("⚠️ Formato de dirección de Datanode incorrecto: %s", addr)
			continue
		}
		dnID := parts[0]

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("⚠️ No se pudo conectar a Datanode %s en %s: %v", dnID, addr, err)
			continue
		}
		clientMap[dnID] = pbDatanode.NewDatanodeServiceClient(conn)
	}
	return clientMap
}

// selectDatanodeID selecciona una clave de Datanode aleatoriamente para registrar la afinidad inicial.
func selectDatanodeID(clientMap map[string]pbDatanode.DatanodeServiceClient) string {
	for id := range clientMap {
		return id // Simplemente devuelve el primer ID (Round Robin más complejo no es estrictamente necesario aquí)
	}
	return ""
}
