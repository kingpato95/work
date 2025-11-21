package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pbDatanode "aeroDist/proto/datanode"
)

const (
	brokerAddr      = "broker:50051" // El cliente MR se comunica directamente con el Broker Central [cite: 109]
	defaultClientID = "Observador1"
	targetFlightID  = "LA-500" // Vuelo de ejemplo
)

// Client simula un pasajero observador.
type Client struct {
	ClientID     string
	BrokerClient pbDatanode.DatanodeServiceClient

	// Estado local: Guarda la última VectorClock vista para cada FlightID
	localVersions map[string]map[string]int64 // map[FlightID] -> map[NodeID] -> Count
	mu            sync.Mutex
}

func main() {
	log.Println("Iniciando Cliente MR (Pasajero Observador)...")

	// 1. Obtener ClientID
	clientID := os.Getenv("CLIENT_ID")
	if clientID == "" {
		clientID = defaultClientID
	}

	// 2. Conexión con el Broker
	conn, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("No se pudo conectar al Broker %s: %v", brokerAddr, err)
	}
	defer conn.Close()

	client := &Client{
		ClientID:      clientID,
		BrokerClient:  pbDatanode.NewDatanodeServiceClient(conn),
		localVersions: make(map[string]map[string]int64),
	}

	log.Printf("ID del Observador: %s. Iniciando consultas.", client.ClientID)

	// Iniciar el loop de consultas continuas
	client.StartContinuousReads()
}

// StartContinuousReads inicia el ciclo de consultas versionadas.
func (c *Client) StartContinuousReads() {
	for i := 1; ; i++ {
		time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second) // Consultar cada 1-3 segundos

		log.Printf("\n--- [%s] Consulta de Estado #%d ---", c.ClientID, i)
		c.PerformMonotonicRead(targetFlightID)
	}
}

// PerformMonotonicRead ejecuta una consulta, enviando la versión conocida.
func (c *Client) PerformMonotonicRead(flightID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. Obtener la última versión conocida
	currentVC := c.getKnownVersion(flightID)

	// 2. Construir la solicitud versionada
	readReq := &pbDatanode.ReadRequest{
		FlightId: flightID,
		ClientVersion: &pbDatanode.VectorClock{
			Vc: currentVC, // Enviar el VC conocido
		},
	}

	log.Printf("Solicitando Vuelo %s. Versión conocida (VC): %v", flightID, currentVC)

	// 3. Enviar la consulta al Broker
	resp, err := c.BrokerClient.ReadFlightState(ctx, readReq)

	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Unavailable {
			log.Printf("⚠️ %s: Datanode atrasado. Reintentando en breve.", c.ClientID)
			return // El servidor (Datanode) indicó que no pudo satisfacer la monotonicidad.
		}
		log.Printf("[%s] ERROR de Lectura: %v", c.ClientID, err)
		return
	}

	currentState := resp.GetCurrentState()
	if currentState == nil {
		log.Println("Vuelo no encontrado o sin datos aún.")
		return
	}

	// 4. Validar y Actualizar la Versión Local
	newVC := currentState.GetClock().GetVc()

	// La validación se hace implícitamente por el servidor, pero el cliente verifica si hubo retroceso.
	if c.isVersionCausallyBefore(newVC, currentVC) {
		log.Printf("[%s] FALLO DE CONSISTENCIA MONOTONIC READS!", c.ClientID)
		log.Printf("   Versión Anterior (VC): %v", currentVC)
		log.Printf("   Versión Recibida (VC): %v", newVC)
		log.Printf("   Estado: %s, Puerta: %s", currentState.GetStatus(), currentState.GetGate())
	} else {
		// Éxito: Actualizar el estado local con la nueva versión
		c.updateKnownVersion(flightID, newVC)
		log.Printf("✨ [%s] OK. Estado: %s, Puerta: %s. Nuevo VC Guardado.", c.ClientID, currentState.GetStatus(), currentState.GetGate())
	}
}

// =================================================================
// GESTIÓN DEL ESTADO LOCAL (VERSIONES CONOCIDAS)
// =================================================================

// getKnownVersion obtiene el VC conocido, o un VC vacío si es la primera vez.
func (c *Client) getKnownVersion(flightID string) map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	if versions, ok := c.localVersions[flightID]; ok {
		return versions
	}
	// Inicialmente, la versión es vacía (o cero) [cite: 101]
	return make(map[string]int64)
}

// updateKnownVersion guarda la nueva versión.
func (c *Client) updateKnownVersion(flightID string, newVC map[string]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Crear una copia para evitar modificación concurrente
	copiedVC := make(map[string]int64)
	for k, v := range newVC {
		copiedVC[k] = v
	}

	c.localVersions[flightID] = copiedVC
}

// isVersionCausallyBefore verifica si el nuevo VC es causalmente anterior al conocido.
// Esta función es redundante si el servidor aplica correctamente la lógica MR, pero sirve para la validación del cliente.
func (c *Client) isVersionCausallyBefore(newVC map[string]int64, knownVC map[string]int64) bool {
	// Se considera anterior si *todos* los componentes del nuevo VC son <= a los del conocido,
	// Y *al menos uno* es estrictamente menor.

	isBefore := true
	hasSmaller := false

	// Iterar sobre todas las claves presentes en ambos mapas
	allKeys := make(map[string]struct{})
	for k := range newVC {
		allKeys[k] = struct{}{}
	}
	for k := range knownVC {
		allKeys[k] = struct{}{}
	}

	for k := range allKeys {
		vNew := newVC[k]
		vKnown := knownVC[k]

		if vNew > vKnown {
			// Si el nuevo es mayor en algún componente, no puede ser causalmente anterior.
			return false
		}
		if vNew < vKnown {
			hasSmaller = true
		}
	}

	return isBefore && hasSmaller
}
