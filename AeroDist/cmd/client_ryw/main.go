package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pbCoordinator "aeroDist/proto/coordinator"
)

const (
	coordinatorAddr = "coordinator:50054" // Dirección del Coordinador
	defaultClientID = "Pasajero1"
	targetFlightID  = "IB-6833" // Vuelo de ejemplo
)

// Client simula un pasajero en Check-in.
type Client struct {
	ClientID         string
	CoordClient      pbCoordinator.CoordinatorServiceClient
	LastSeatSelected string
	LastRequestID    string
}

func main() {
	log.Println("🚀 Iniciando Cliente RYW (Pasajero en Check-in)...")

	// 1. Obtener ClientID
	clientID := os.Getenv("CLIENT_ID")
	if clientID == "" {
		clientID = defaultClientID
	}

	// 2. Conexión con el Coordinador
	conn, err := grpc.Dial(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("❌ No se pudo conectar al Coordinador %s: %v", coordinatorAddr, err)
	}
	defer conn.Close()

	client := &Client{
		ClientID:    clientID,
		CoordClient: pbCoordinator.NewCoordinatorServiceClient(conn),
	}

	log.Printf("ID del Cliente: %s. Listo para Check-in.", client.ClientID)

	// Simular el ciclo de check-in
	// El bucle for permite reintentos en caso de fallo o simular múltiples operaciones.
	for i := 0; i < 3; i++ {
		time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second) // Espera entre operaciones
		client.PerformCheckInCycle(i)
	}
}

// PerformCheckInCycle ejecuta la secuencia Lectura -> Escritura -> Lectura de Confirmación.
func (c *Client) PerformCheckInCycle(attempt int) {
	log.Printf("\n--- [%s] Inicio del Ciclo de Check-in (Intento %d) ---", c.ClientID, attempt+1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Solicitud de Estado Inicial (Lectura)
	// Aunque el lab menciona una lectura inicial, la enfocaremos en la escritura y RYW.
	// Por simplicidad, omitimos la RPC de "GetMapaSientos" y simulamos la selección de asiento.
	c.LastSeatSelected = fmt.Sprintf("%d%s", rand.Intn(20)+1, string('A'+rand.Intn(6)))
	c.LastRequestID = uuid.New().String()
	log.Printf("✍️ Asiento seleccionado: %s. ID Solicitud: %s.", c.LastSeatSelected, c.LastRequestID)

	// 2. Envío de Operación de Escritura (Check-in Idempotente)
	writeReq := &pbCoordinator.CheckInRequest{
		ClientId:     c.ClientID,
		FlightId:     targetFlightID,
		SelectedSeat: c.LastSeatSelected,
		RequestId:    c.LastRequestID, // Clave para idempotencia
	}

	log.Println("🛫 Enviando Check-in al Coordinador...")

	writeResp, err := c.CoordClient.CheckIn(ctx, writeReq)
	if err != nil {
		log.Printf("❌ [%s] ERROR en Check-in: %v", c.ClientID, err)
		return
	}

	if !writeResp.Success {
		log.Printf("⚠️ [%s] Check-in fallido (Condición de carrera o error): %s", c.ClientID, writeResp.Message)
		return
	}

	log.Printf("✅ [%s] Check-in confirmado por Coordinador: %s", c.ClientID, writeResp.Message)

	// 3. Lectura de Confirmación (Garantía Read Your Writes)
	// Esta lectura debe realizarse inmediatamente después y debe reflejar la escritura.
	readReq := &pbCoordinator.GetBoardingPassRequest{
		ClientId: c.ClientID,
		FlightId: targetFlightID,
	}

	log.Println("🔍 Solicitando Boarding Pass (Lectura RYW)...")
	readResp, err := c.CoordClient.GetBoardingPass(ctx, readReq)

	if err != nil {
		log.Printf("❌ [%s] ERROR en Lectura de Confirmación: %v", c.ClientID, err)
		return
	}

	// 4. Validación de Consistencia RYW
	currentState := readResp.GetBoardingPassState()

	// La validación se basa en si el estado devuelto contiene la información escrita.
	if currentState == nil || !strings.Contains(currentState.GetStatus(), c.LastSeatSelected) {
		log.Printf("🛑 [%s] FALLO DE CONSISTENCIA RYW!", c.ClientID)
		log.Printf("   Esperaba asiento: %s. Recibí estado: %v", c.LastSeatSelected, currentState)
	} else {
		log.Printf("✨ [%s] VALIDACIÓN RYW EXITOSA. Mi asiento %s se refleja inmediatamente.", c.ClientID, c.LastSeatSelected)
	}
}

// Implementar una función string.Contains para evitar el import del paquete strings en la función main
func stringsContains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// Importar el paquete strings necesario para las funciones auxiliares
