# Generar todo y levantar el sistema
run:
	docker-compose up --build

# Detener y borrar todo
stop:
	docker-compose down

# Limpiar archivos temporales o contenedores huérfanos
clean:
	docker system prune -f

# (Opcional) Comandos individuales para las VMs si te los piden
#docker-VM1:
#	docker run -d --net=host --name broker aero-dist-broker