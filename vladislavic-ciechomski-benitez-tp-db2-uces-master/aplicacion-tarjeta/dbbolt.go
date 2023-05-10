package main

import (
	"encoding/json"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"log"
	"strconv"
)

type Cliente struct {
	Nrocliente int
	Nombre     string
	Apellido   string
	Domicilio  string
	Telefono   string
}
type Comercio struct {
	Nrocomercio  int
	Nombre       string
	Domicilio    string
	Codigopostal string
	Telefono     string
}
type Tarjeta struct {
	Nrotarjeta   string
	Nrocliente   int
	Validadesde  string
	Validahasta  string
	Codseguridad string
	Limitecompra float64
	Estado       string
}
type Consumo struct {
	Nrotarjeta   string
	Codseguridad string
	Nrocomercio  int
	Monto        float64
}

func CreateUpdate(db *bolt.DB, bucketName string, key []byte, val []byte) error {
	// abre transacción de escritura
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, _ := tx.CreateBucketIfNotExists([]byte(bucketName))

	err = b.Put(key, val)
	if err != nil {
		return err
	}

	// cierra transacción
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func ReadUnique(db *bolt.DB, bucketName string, key []byte) ([]byte, error) {
	var buf []byte

	// abre una transacción de lectura
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		buf = b.Get(key)
		return nil
	})

	return buf, err
}

func clientes() {
	db, err := bolt.Open("aplicacion-tarjeta.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sofi := Cliente{1, "Sofia", "Ciechomski", "Rawson 1869", "47903732"}
	data, err := json.Marshal(sofi)
	nico := Cliente{2, "Nicolas", "Di Cesare", "Entre Rios 2721", "541147906564"}
	date, err := json.Marshal(nico)
	car := Cliente{3, "Carlos", "Vladislavic", "Warnes 564", "541147904546"}
	dato, err := json.Marshal(car)

	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "cliente", []byte(strconv.Itoa(sofi.Nrocliente)), data)
	resultado, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(sofi.Nrocliente)))
	CreateUpdate(db, "cliente", []byte(strconv.Itoa(nico.Nrocliente)), date)
	res, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(nico.Nrocliente)))
	CreateUpdate(db, "cliente", []byte(strconv.Itoa(car.Nrocliente)), dato)
	respu, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(car.Nrocliente)))

	fmt.Printf("%s\n", resultado)
	fmt.Printf("%s\n", res)
	fmt.Printf("%s\n", respu)

}

func comercio() {

	db, err := bolt.Open("aplicacion-tarjeta.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	wil := Comercio{102, "Williamburg", "Arieta 3545", "B1754APC", "541147901213"}
	data, err := json.Marshal(wil)
	tm := Comercio{143, "Todo Moda", "Av. Rivadavia 14450", "B1704ERA", "541147921244"}
	date, err := json.Marshal(tm)
	yp := Comercio{188, "YPF", "Luis Guemes 369", "B1706EYH", "541147919988"}
	dato, err := json.Marshal(yp)

	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "cliente", []byte(strconv.Itoa(wil.Nrocomercio)), data)
	resultado, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(wil.Nrocomercio)))
	CreateUpdate(db, "cliente", []byte(strconv.Itoa(tm.Nrocomercio)), date)
	res, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(tm.Nrocomercio)))
	CreateUpdate(db, "cliente", []byte(strconv.Itoa(yp.Nrocomercio)), dato)
	respu, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(yp.Nrocomercio)))

	fmt.Printf("%s\n", resultado)
	fmt.Printf("%s\n", res)
	fmt.Printf("%s\n", respu)

}

func tarjeta() {

	db, err := bolt.Open("aplicacion-tarjeta.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	uno := Tarjeta{"4662451537132351", 1, "202001", "202210", "5123", 5000, "Vigente"}
	data, err := json.Marshal(uno)
	dos := Tarjeta{"4861740374483652", 2, "201911", "202312", "2346", 8000, "Vigente"}
	date, err := json.Marshal(dos)
	tres := Tarjeta{"4405938055039775", 3, "202201", "202702", "3452", 10000, "Vigente"}
	dato, err := json.Marshal(tres)

	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "cliente", []byte(strconv.Itoa(uno.Nrocliente)), data)
	resultado, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(uno.Nrocliente)))
	CreateUpdate(db, "cliente", []byte(strconv.Itoa(dos.Nrocliente)), date)
	res, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(dos.Nrocliente)))
	CreateUpdate(db, "cliente", []byte(strconv.Itoa(tres.Nrocliente)), dato)
	respu, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(tres.Nrocliente)))

	fmt.Printf("%s\n", resultado)
	fmt.Printf("%s\n", res)
	fmt.Printf("%s\n", respu)

}

func consumo() {

	db, err := bolt.Open("aplicacion-tarjeta.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	uno := Consumo{"4861740374483652", "2346", 337, 10600}
	data, err := json.Marshal(uno)
	dos := Consumo{"4101436112271841", "1896", 597, 5750}
	date, err := json.Marshal(dos)
	tres := Consumo{"4115513260891341", "1124", 266, 12000}
	dato, err := json.Marshal(tres)

	if err != nil {
		log.Fatal(err)
	}

	CreateUpdate(db, "cliente", []byte(strconv.Itoa(uno.Nrocomercio)), data)
	resultado, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(uno.Nrocomercio)))
	CreateUpdate(db, "cliente", []byte(strconv.Itoa(dos.Nrocomercio)), date)
	res, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(dos.Nrocomercio)))
	CreateUpdate(db, "cliente", []byte(strconv.Itoa(tres.Nrocomercio)), dato)
	respu, err := ReadUnique(db, "cliente", []byte(strconv.Itoa(tres.Nrocomercio)))

	fmt.Printf("%s\n", resultado)
	fmt.Printf("%s\n", res)
	fmt.Printf("%s\n", respu)

}

func main() {

	var opc int

	for {

		fmt.Printf("MENU DE DATA BASE\n")
		fmt.Printf("1...Mostrar Clientes\n")
		fmt.Printf("2...Mostrar Comercios\n")
		fmt.Printf("3...Mostrar Tarjetas\n")
		fmt.Printf("4...Mostrar Consumos\n")
		fmt.Printf("5...Salir\n")
		fmt.Scanf("%d", &opc)
		switch opc {
		case 1:
			clientes()
			break
		case 2:
			comercio()
			break
		case 3:
			tarjeta()
			break
		case 4:
			consumo()
			break
		case 5:
		break	
		}
		if opc < 1 && opc > 5 {
			break

		}
	}
}
