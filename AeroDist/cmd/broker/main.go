package main

import (
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"encoding/csv" // Nuevo
	"io"           // Nuevo
	"os"           // Nuevo
	"strconv"      // Nuevo

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	// Importar los paquetes gRPC generados.
	// Asumimos que los servicios del Broker, Datanode y Consensus se usan aquí.
	pbConsensus "aeroDist/proto/consensus"
	pbDatanode "aeroDist/proto/datanode"
	// Nota: Si el Broker expone un servicio propio (ej. para Monotonic Reads),
	// se debería definir un 'broker.proto'. Aquí usaremos los métodos ReadFlightState
	// y ProposeAction. El gRPC Server se registra con UnimplementedDatanodeServiceServer
	// ya que su método ReadFlightState es el que consume el Cliente MR.
)

const (
	brokerPort = ":50051"
	// NOTA: Estas direcciones deben ser reemplazadas por las direcciones reales de Docker en las VMs.
	datanodeAddrs = "dn1:50052,dn2:50052,dn3:50052"
	atcNodeAddrs  = "atc1:50053,atc2:50053,atc3:50053"
)

// =================================================================
// ESTRUCTURA DEL SERVIDOR BROKER
// El Broker implementa los métodos que los clientes (MR, Coordinador) necesitan.
// Por simplicidad, el Broker implementará los RPCs ReadFlightState y ProposeAction
// definidos en los otros .proto.
// =================================================================

// Server encapsula la lógica y los clientes internos del Broker.
type Server struct {
	// Implementa los servicios que el Broker maneja directamente.
	// Asumimos que el Cliente MR llama a ReadFlightState, el cual está definido en datanode.proto.
	// Podríamos implementar pbDatanode.UnimplementedDatanodeServiceServer o definir un pbBroker.
	// Usaremos la estructura para simplificar la interfaz.
	pbDatanode.UnimplementedDatanodeServiceServer

	datanodeClients []pbDatanode.DatanodeServiceClient
	atcClients      []pbConsensus.ConsensusServiceClient

	rrCounter int
	mu        sync.Mutex

	logicalClock int64


	// Para el Reporte.txt
	criticalOps []string
}

func main() {
	log.Println("Iniciando Broker Central (Sistema Central de Operaciones)...")

	// 1. Inicializar Clientes a Subsistemas
	datanodeClients := initializeDatanodeClients(datanodeAddrs)
	if len(datanodeClients) == 0 {
		log.Printf("Advertencia: No se pudo conectar a Datanodes. Solo Consenso/Broadcast funcionarán.")
	} else {
		log.Printf("Clientes a %d Datanodes inicializados.", len(datanodeClients))
	}

	atcClients := initializeConsensusClients(atcNodeAddrs)
	if len(atcClients) == 0 {
		log.Fatal("Error: No se pudo conectar a ningún Nodo ATC para operaciones críticas.")
	}
	log.Printf("Clientes a %d Nodos ATC inicializados.", len(atcClients))

	// 2. Crear la instancia del servidor Broker
	s := &Server{
		datanodeClients: datanodeClients,
		atcClients:      atcClients,
		rrCounter:       0,
		criticalOps:     make([]string, 0),
	}

	// 3. Iniciar la simulación de actualizaciones de Aerolíneas (Broadcast Asíncrono)
	go s.startAirlineUpdateSimulator()

	// 4. Configurar y lanzar el servidor gRPC
	lis, err := net.Listen("tcp", brokerPort)
	if err != nil {
		log.Fatalf("Error al escuchar en el puerto %s: %v", brokerPort, err)
	}

	grpcServer := grpc.NewServer()

	// El Broker implementa la interfaz del servicio Datanode para manejar las lecturas del Cliente MR
	// y la interfaz para las propuestas de Consenso.
	pbDatanode.RegisterDatanodeServiceServer(grpcServer, s) // Para ReadFlightState
	// Tendríamos que registrar el servicio de consenso, pero como el Broker es el cliente del consenso,
	// solo implementamos el RPC ProposeAction aquí.

	log.Printf("👂 Broker escuchando en %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Fallo al servir: %v", err)
	}
}

// =================================================================
// LÓGICA DE GESTIÓN DEL BROKER
// =================================================================

// GetNextDatanodeClient selecciona un Datanode usando Round Robin para balancear lecturas.
func (s *Server) GetNextDatanodeClient() pbDatanode.DatanodeServiceClient {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.datanodeClients) == 0 {
		return nil
	}

	client := s.datanodeClients[s.rrCounter]
	s.rrCounter = (s.rrCounter + 1) % len(s.datanodeClients)
	return client
}

// startAirlineUpdateSimulator simula la recepción de actualizaciones y realiza el Broadcast Asíncrono.
// startAirlineUpdateSimulator lee eventos desde un CSV y los transmite.
func (s *Server) startAirlineUpdateSimulator() {
	// Esperar un poco a que los Datanodes estén listos
	time.Sleep(5 * time.Second)

	log.Println("📂 Abriendo archivo de simulación: flight_updates.csv")
	file, err := os.Open("/app/flight_updates.csv")
	if err != nil {
		log.Printf("❌ Error abriendo CSV: %v. Usando modo manual/silencioso.", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	
	// Leer encabezado (si existe) para saltarlo
	if _, err := reader.Read(); err != nil {
		log.Printf("⚠️ Error leyendo encabezado CSV: %v", err)
		return
	}

	var lastTime int = 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			log.Println("🏁 Fin del archivo de simulación CSV.")
			break
		}
		if err != nil {
			log.Printf("❌ Error leyendo línea CSV: %v", err)
			continue
		}

		// Formato esperado: sim_time_sec, flight_id, update_type, update_value
		// Ejemplo: 2, AF-021, estado, En vuelo
		if len(record) < 4 {
			continue
		}

		simTime, _ := strconv.Atoi(record[0])
		flightID := record[1]
		updateType := record[2]
		updateValue := record[3]

		// Calcular espera (delta de tiempo)
		delay := time.Duration(simTime - lastTime) * time.Second
		if delay > 0 {
			time.Sleep(delay)
		}
		lastTime = simTime

		// Construir actualización
		update := &pbDatanode.FlightState{
			FlightId: flightID,
		}

		if updateType == "estado" {
			update.Status = updateValue
		} else if updateType == "puerta" {
			update.Gate = updateValue
		}

		// === LÓGICA DE RELOJ VECTORIAL ===
		s.mu.Lock()
		s.logicalClock++
		currentTick := s.logicalClock
		s.mu.Unlock()

		update.Clock = &pbDatanode.VectorClock{
			Vc: map[string]int64{"broker": currentTick},
		}
		// ================================

		log.Printf("csv_event -> Vuelo: %s | %s: %s | Reloj: %d", flightID, updateType, updateValue, currentTick)
		
		// Enviar a todos
		s.broadcastUpdate(update)
	}
}

// broadcastUpdate distribuye la actualización a todos los Datanodes de forma asíncrona.
func (s *Server) broadcastUpdate(update *pbDatanode.FlightState) {
	log.Printf("Broker Broadcast: Enviando actualización de vuelo %s...", update.FlightId)

	var wg sync.WaitGroup
	for _, client := range s.datanodeClients {
		wg.Add(1)
		go func(c pbDatanode.DatanodeServiceClient) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond) // Tiempo corto para asíncrono
			defer cancel()

			_, err := c.ProcessUpdate(ctx, &pbDatanode.UpdateRequest{Update: update})
			if err != nil {
				log.Printf("⚠️ Error al enviar a un Datanode: %v", err)
			}
		}(client)
	}
}

// ReadFlightState implementa la lógica de Lectura para Clientes MR y el Coordinador (lecturas no afines).
// Note que el Broker actúa como proxy, enrutando al Datanode seleccionado por RR.
func (s *Server) ReadFlightState(ctx context.Context, req *pbDatanode.ReadRequest) (*pbDatanode.ReadResponse, error) {
	log.Printf("🔍 Broker: Recibiendo consulta MR para %s. VKnown: %v", req.FlightId, req.ClientVersion.GetVc())

	client := s.GetNextDatanodeClient()
	if client == nil {
		return nil, grpc.Errorf(codes.Unavailable, "No Datanodes available")
	}

	// Reenviar la solicitud de lectura versionada (Monotonic Reads)
	resp, err := client.ReadFlightState(ctx, req)
	if err != nil {
		log.Printf("❌ Error al leer del Datanode: %v", err)
		return nil, err
	}

	return resp, nil
}

// ProposeAction implementa el inicio del Protocolo de Consenso.
// Aunque este método está definido en consensus.proto, el Broker lo llama
// para enviar la propuesta al líder del consenso. Aquí lo implementamos de forma proxy.
func (s *Server) ProposeAction(ctx context.Context, req *pbConsensus.ProposeRequest) (*pbConsensus.ProposeResponse, error) {
	s.mu.Lock()
	log.Printf("🚦 Broker: Solicitud crítica de Consenso: %s", req.ProposalData)
	s.mu.Unlock()

	if len(s.atcClients) == 0 {
		return &pbConsensus.ProposeResponse{Success: false, Message: "No ATC Nodes available"}, nil
	}

	// Estrategia: Enviar al primer nodo disponible (asumiendo que redireccionará si no es el líder).
	client := s.atcClients[0]

	resp, err := client.ProposeAction(ctx, req)
	if err != nil {
		s.mu.Lock()
		s.criticalOps = append(s.criticalOps, time.Now().Format("2006-01-02 15:04:05")+" | FAIL: "+req.ProposalData+" | ERROR: "+err.Error())
		s.mu.Unlock()
		return nil, err
	}

	s.mu.Lock()
	if resp.Success {
		s.criticalOps = append(s.criticalOps, time.Now().Format("2006-01-02 15:04:05")+" | SUCCESS: "+req.ProposalData+" | NEW_STATE: "+resp.NewState.String())
	} else {
		s.criticalOps = append(s.criticalOps, time.Now().Format("2006-01-02 15:04:05")+" | REDIRECT/FAIL: "+req.ProposalData+" | MESSAGE: "+resp.Message)
	}
	s.mu.Unlock()

	return resp, nil
}

// GenerateReport genera el archivo Reporte.txt al finalizar la ejecución.
func (s *Server) GenerateReport() {
	// Implementación para escribir s.criticalOps en un archivo Reporte.txt
	// (Se invocaría con una señal de interrupción en un sistema real)
}

// =================================================================
// FUNCIONES AUXILIARES DE INICIALIZACIÓN DE CLIENTES
// =================================================================

// initializeDatanodeClients establece las conexiones gRPC con los Datanodes.
func initializeDatanodeClients(addrs string) []pbDatanode.DatanodeServiceClient {
	clients := make([]pbDatanode.DatanodeServiceClient, 0)
	addrList := strings.Split(addrs, ",")

	for _, addr := range addrList {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("No se pudo conectar a Datanode %s: %v", addr, err)
			continue
		}
		// NOTE: La conexión debería cerrarse al finalizar la ejecución del Broker.
		clients = append(clients, pbDatanode.NewDatanodeServiceClient(conn))
	}
	return clients
}

// initializeConsensusClients establece las conexiones gRPC con los Nodos ATC.
func initializeConsensusClients(addrs string) []pbConsensus.ConsensusServiceClient {
	clients := make([]pbConsensus.ConsensusServiceClient, 0)
	addrList := strings.Split(addrs, ",")

	for _, addr := range addrList {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Error fatal al conectar con ATC Node %s: %v", addr, err)
		}
		// NOTE: La conexión debería cerrarse al finalizar la ejecución del Broker.
		clients = append(clients, pbConsensus.NewConsensusServiceClient(conn))
	}
	return clients
}

// ProcessUpdate maneja las escrituras (Check-in) redirigidas por el Coordinador.
func (s *Server) ProcessUpdate(ctx context.Context, req *pbDatanode.UpdateRequest) (*pbDatanode.UpdateResponse, error) {
	log.Printf("📝 Broker: Recibiendo escritura (Check-in) para vuelo %s", req.Update.FlightId)

	// 1. Seleccionar un Datanode destino (Balanceo de Carga)
	client := s.GetNextDatanodeClient()
	if client == nil {
		return nil, grpc.Errorf(codes.Unavailable, "No Datanodes available for write")
	}

	// 2. Reenviar la solicitud al Datanode seleccionado
	resp, err := client.ProcessUpdate(ctx, req)
	if err != nil {
		log.Printf("❌ Error al escribir en Datanode: %v", err)
		return nil, err
	}

	log.Printf("✅ Escritura exitosa en Datanode.")
	return resp, nil
} 