package main


import (

	//"encoding/json"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	//bolt "go.etcd.io/bbolt"
	"log"
	//"strconv"
)

//función para la creación de la base de datos.
func createDatabase() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=postgres sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec(`drop database if exists tp`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create database tp`)

	if err != nil {
		log.Fatal(err)
	}
}

//función para la creación de las tablas.
func crearTablas() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec(`create table cliente(
	nrocliente int,
	nombre text,
	apellido text,
	domicilio text,
	telefono char(12)
)`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table tarjeta(
    nrotarjeta char(16),
    nrocliente int,
    validadesde char(6),
    validahasta char(6),
    codseguridad char(4),
    limitecompra decimal(8,2),
    estado char(10)
)`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table comercio(
    nrocomercio int,
    nombre text,
    domicilio text,
    codigopostal char(8),
    telefono char(12)
)`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create sequence compra_nrooperacion_seq`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table compra(
    nrooperacion int not null default nextval('compra_nrooperacion_seq'),
    nrotarjeta char(16),
    nrocomercio int,
    fecha timestamp,
    monto decimal(7,2),
    pagado boolean
)`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create sequence rechazo_nrorechazo_seq`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table rechazo(
    nrorechazo int not null default nextval('rechazo_nrorechazo_seq'),
    nrotarjeta char(16),
    nrocomercio int,
    fecha timestamp,
    monto decimal(7,2),
    motivo text
)`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table cierre(
    anio int,
    mes int,
    terminacion int,
    fechainicio date,
    fechacierre date,
    fechavto date
)`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create sequence cabecera_nroresumen_seq`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table cabecera(
    nroresumen int not null default nextval('cabecera_nroresumen_seq'),
    nombre text,
    apellido text,
    domicilio text,
    nrotarjeta char(16),
    desde date,
    hasta date,
    vence date,
    total decimal(8,2)
)`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create sequence detalle_nrolinea_seq`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table detalle(
    nroresumen int,
    nrolinea int not null default nextval('detalle_nrolinea_seq'),
    fecha date,
    nombrecomercio text,
    monto decimal(7,2)
 )`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create sequence alerta_nroalerta_seq`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table alerta(
    nroalerta int not null default nextval('alerta_nroalerta_seq'),
    nrotarjeta char(16),
    fecha timestamp,
    nrorechazo int,
    codalerta int,
    descripcion text
)`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create table consumo(
	nrotarjeta char (16),
	codseguridad char(4),
	nrocomercio int,
	monto decimal(7,2) 
)`)

	if err != nil {
		log.Fatal(err)
	}
}

//función para crear primary keys y foreign keys.
func crearKeys() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec(`alter table cliente  add constraint cliente_pk  primary key (nrocliente);
alter table tarjeta  add constraint tarjeta_pk  primary key (nrotarjeta);
alter table comercio add constraint comercio_pk primary key (nrocomercio);
alter table compra   add constraint compra_pk   primary key (nrooperacion);
alter table rechazo  add constraint rechazo_pk  primary key (nrorechazo);
alter table cierre   add constraint cierre_pk   primary key (anio,mes,terminacion);
alter table cabecera add constraint cabecera_pk primary key (nroresumen);
alter table detalle  add constraint detalle_pk  primary key (nroresumen,nrolinea);
alter table alerta   add constraint alerta_pk   primary key (nroalerta);
alter table tarjeta  add constraint tarjeta_nrocliente_fk  foreign key (nrocliente)  references cliente(nrocliente);
alter table compra   add constraint compra_nrotarjeta_fk   foreign key (nrotarjeta)  references tarjeta(nrotarjeta);
alter table compra   add constraint compra_nrocomercio_fk  foreign key (nrocomercio) references comercio(nrocomercio);
alter table cabecera add constraint cabecera_nrotarjeta_fk foreign key (nrotarjeta)  references tarjeta(nrotarjeta);
alter table detalle  add constraint detalle_nroresumen_fk  foreign key (nroresumen)  references cabecera(nroresumen);
alter table alerta   add constraint alerta_nrotarjeta_fk   foreign key (nrotarjeta)  references tarjeta(nrotarjeta);
;`)

	if err != nil {
		log.Fatal(err)
	}
}

//función para borrar primary keys y foreign keys
func borrarKeys() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec(`alter table tarjeta  drop constraint tarjeta_nrocliente_fk;
	alter table compra   drop constraint compra_nrotarjeta_fk;
	alter table compra   drop constraint compra_nrocomercio_fk ;
	alter table cabecera drop constraint cabecera_nrotarjeta_fk;
	alter table detalle  drop constraint detalle_nroresumen_fk;
	alter table alerta   drop constraint alerta_nrotarjeta_fk; 
	--alter table alerta   drop constraint alerta_nrorechazo_fk;
	alter table tarjeta  drop constraint tarjeta_pk;
	alter table comercio drop constraint comercio_pk;
	alter table compra   drop constraint compra_pk;
	alter table rechazo  drop constraint rechazo_pk;
	alter table cierre   drop constraint cierre_pk;
	alter table cabecera drop constraint cabecera_pk;
	alter table detalle  drop constraint detalle_pk;
	alter table alerta   drop constraint alerta_pk;`)

	if err != nil {
		log.Fatal(err)
	}
}

//función para cargar datos a las tablas
func cargarDatos() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec(`insert into cliente values(1,'Sofia','Ciechomski','Rawson 1869','47903732'),
(2,'Nicolas','Di Cesare','Entre Rios 2721','541147906564'),
(3,'Carlos','Vladislavic','Warnes 546','541147904546'),
(4,'Patricia','Chaca','Juan de Garay 4213','541147903232'),
(5,'Florencia','Rodriguez','Libertador 14500','541147983432'),
(6,'Julieta','Fimiani','Sarmiento 4091','541146531243'),
(7,'Roberto','Clerici','Cabildo 533','541147924378'),
(8,'Santiago','Desimone','Cramer 2343','541147885456'),
(9,'Lucia','Tapia','Warnes 2198','541147963344'),
(10,'Cristina','Mac cormack','Marmol 2541','541147995577'),
(11,'Miriam','Ucañan','Quintana 1650','541147952211'),
(12,'Leandro','Benitez','Parana 1122','541147901122'),
(13,'Lidia','Lau','San Martin 2830','541147992233'),
(14,'Marcos','Zavaleta','Jose M. Bosch 4530','541147885687'),
(15,'Romina','Velasquez','Psje el Cano 2831','541147924532'),
(16,'Roberto','Gutierrez','Aldo de la Rosa 2381','541147945657'),
(17,'Antonio','Moreno','Laprida 1443','541147994351'),
(18,'Daniel','Acosta','Pedernera 3322','541147934546'),
(19,'Ivan','Soria','Juarez 4630','541147965477'),
(20,'Marilin','Rodriguez','Lavalle 124','541147953104');`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`insert into comercio values
(102,'Williamburg','Arieta 3545','B1754APC','541147901213'),
(143,'Todo Moda','Av. Rivadavia 14450','B1704ERA','541147921244'),
(188,'YPF','Luis Guemes 369','B1706EYH','541147919988'),
(212,'Bucare','Ramon Falcon 7145','C1406GNA','541147327711'),
(231,'Chinin','Pueyrredon 4316','B1650KUA','541147984322'),
(266,'Dash','Alvear 2693','B1653FVA','541147924455'),
(295,'Repuestos Ĺeandro SRL','Juarez 4917','B1650KUA','541146672324'),
(298,'HP','Independencia 4602','B1653FVA','541149873435'),
(307,'Santa Marta','Belgrano 3329','B1650KUA','541147552329'),
(319,'Cuesta Blanca','Av. Cabildo 2128','C1428AAQ','541147934566'),
(337,'Samsung','Vedia 3600','C1430DAF','541147988822'),
(358,'Morita','Calle 9 de julio 1415','B1820KJG','541147038871'),
(374,'Carrefour','Laprida 342','B1832HOH','541147047790'),
(396,'Fiestisima','Av. Cordoba 2331','C1120AAF','541147804433'),
(416,'Ver','Gallo 1330','C1425EFD','541147924435'),
(455,'Elefante Bar','Eduardo Costa 2024','B1640BAP','541149982341'),
(597,'Coppel','Av Maipu 2841','B1636AAH','541147925005'),
(642,'Biblos','Av. Maipu 3001','B1636AAK','541147992002'),
(657,'Pizza Cero','Av Libertador 1800','C1112ABP','541147963300'),
(794,'Howard Johnson','Aristobulo del Valle 1259','B1640EQS','541147754332');`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`insert into tarjeta values
('4662451537132351',1,'202001','202210','5123',5000,'Vigente'),
('4861740374483652',2,'201911','202312','2346',8000,'Vigente'),
('4405938055039775',3,'202201','202702','3452',10000,'Vigente'),
('4652319479526440',4,'202112','202503','4569',25000,'Vigente'),
('4405203219837445',5,'201811','202603','5676',30000,'Vigente'),
('4144516916019635',6,'202004','202709','6783',7000,'Vigente'),
('4524910120323240',7,'201803','202503','7891',12500,'Vigente'),
('4667844291721309',8,'202201','202807','8909',9000,'Vigente'),
('4127661197753240',9,'202112','202609','9103',34000,'Vigente'),
('4964853426625332',10,'201909','202710','1019',17900,'Vigente'),
('4115513260891341',11,'202205','202804','1124',16000,'Vigente'),
('4662793715981189',12,'202105','202703','1936',20000,'Vigente'),
('4101305928066852',13,'202004','202703','1349',13000,'Vigente'),
('4127299546411227',14,'201802','202801','1456',19000,'Vigente'),
('4775163311895101',15,'202008','202403','1563',20000,'Vigente'),
('4511737412472598',16,'202204','202803','1679',15000,'Suspendida'),
('4839930728243240',17,'202207','202708','1789',9000,'Vigente'),
('4101436112271841',18,'202004','202604','1896',12500,'Anulada'),
('4534744527996438',19,'201906','202504','1921',14000,'Vigente'),
('4188623129745321',20,'201803','202304','2013',12500,'Vigente'),
('4320906496542568',1,'201801','202209','2126',12500,'Vigente'),
('4661737412272589',20,'202206','202801','2293',15000,'Vigente');`)

	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`insert into consumo values
('4861740374483652','2346',337,10600),
('4101436112271841','1896',597,5750),
('4115513260891341','1124',266,12000),
('4115513260891341','1124',374,9000),
('4662451537132351','5123',143,800),
('4405938055039775','3452',307,1500),
('4405938055039775','3452',231,1500),
('4405938055039775','3452',597,4000),
('4511737412472598','1679',298,6000),
('4101305928066852','1349',794,6300),
('4524910120323240','7891',794,26300),
('4524910120323240','7891',657,16300);`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`insert into cierre values
(2022,01,0,'2021-12-03','2022-01-02','2022-01-10'),
(2022,01,1,'2021-12-04','2022-01-03','2022-01-11'),
(2022,01,2,'2021-12-05','2022-01-04','2022-01-12'),
(2022,01,3,'2021-12-06','2022-01-05','2022-01-13'),
(2022,01,4,'2021-12-07','2022-01-06','2022-01-14'),
(2022,01,5,'2021-12-08','2022-01-07','2022-01-15'),
(2022,01,6,'2021-12-09','2022-01-08','2022-01-16'),
(2022,01,7,'2021-12-10','2022-01-09','2022-01-17'),
(2022,01,8,'2021-12-11','2022-01-10','2022-01-18'),
(2022,01,9,'2021-12-12','2022-01-11','2022-01-19'),
(2022,02,0,'2022-01-03','2022-02-02','2022-02-10'),
(2022,02,1,'2022-01-04','2022-02-03','2022-02-11'),
(2022,02,2,'2022-01-05','2022-02-04','2022-02-12'),
(2022,02,3,'2022-01-06','2022-02-05','2022-02-13'),
(2022,02,4,'2022-01-07','2022-02-06','2022-02-14'),
(2022,02,5,'2022-01-08','2022-02-07','2022-02-15'),
(2022,02,6,'2022-01-09','2022-02-08','2022-02-16'),
(2022,02,7,'2022-01-10','2022-02-09','2022-02-17'),
(2022,02,8,'2022-01-11','2022-02-10','2022-02-18'),
(2022,02,9,'2022-01-12','2022-02-11','2022-02-19'),
(2022,03,0,'2022-02-03','2022-03-02','2022-03-10'),
(2022,03,1,'2022-02-04','2022-03-03','2022-03-11'),
(2022,03,2,'2022-02-05','2022-03-04','2022-03-12'),
(2022,03,3,'2022-02-06','2022-03-05','2022-03-13'),
(2022,03,4,'2022-02-07','2022-03-06','2022-03-14'),
(2022,03,5,'2022-02-08','2022-03-07','2022-03-15'),
(2022,03,6,'2022-02-09','2022-03-08','2022-03-16'),
(2022,03,7,'2022-02-10','2022-03-09','2022-03-17'),
(2022,03,8,'2022-02-11','2022-03-10','2022-03-18'),
(2022,03,9,'2022-02-12','2022-03-11','2022-03-19'),
(2022,04,0,'2022-03-03','2022-04-02','2022-04-10'),
(2022,04,1,'2022-03-04','2022-04-03','2022-04-11'),
(2022,04,2,'2022-03-05','2022-04-04','2022-04-12'),
(2022,04,3,'2022-03-06','2022-04-05','2022-04-13'),
(2022,04,4,'2022-03-07','2022-04-06','2022-04-14'),
(2022,04,5,'2022-03-08','2022-04-07','2022-04-15'),
(2022,04,6,'2022-03-09','2022-04-08','2022-04-16'),
(2022,04,7,'2022-03-10','2022-04-09','2022-04-17'),
(2022,04,8,'2022-03-11','2022-04-10','2022-04-18'),
(2022,04,9,'2022-03-12','2022-04-11','2022-04-19'),
(2022,05,0,'2022-04-03','2022-05-02','2022-05-10'),
(2022,05,1,'2022-04-04','2022-05-03','2022-05-11'),
(2022,05,2,'2022-04-05','2022-05-04','2022-05-12'),
(2022,05,3,'2022-04-06','2022-05-05','2022-05-13'),
(2022,05,4,'2022-04-07','2022-05-06','2022-05-14'),
(2022,05,5,'2022-04-08','2022-05-07','2022-05-15'),
(2022,05,6,'2022-04-09','2022-05-08','2022-05-16'),
(2022,05,7,'2022-04-10','2022-05-09','2022-05-17'),
(2022,05,8,'2022-04-11','2022-05-10','2022-05-18'),
(2022,05,9,'2022-04-12','2022-05-11','2022-05-19'),
(2022,06,0,'2022-05-03','2022-06-02','2022-06-10'),
(2022,06,1,'2022-05-04','2022-06-03','2022-06-11'),
(2022,06,2,'2022-05-05','2022-06-04','2022-06-12'),
(2022,06,3,'2022-05-06','2022-06-05','2022-06-13'),
(2022,06,4,'2022-05-07','2022-06-06','2022-06-14'),
(2022,06,5,'2022-05-08','2022-06-07','2022-06-15'),
(2022,06,6,'2022-05-09','2022-06-08','2022-06-16'),
(2022,06,7,'2022-05-10','2022-06-09','2022-06-17'),
(2022,06,8,'2022-05-11','2022-06-10','2022-06-18'),
(2022,06,9,'2022-05-12','2022-06-11','2022-06-19'),
(2022,07,0,'2022-06-03','2022-07-02','2022-07-10'),
(2022,07,1,'2022-06-04','2022-07-03','2022-07-11'),
(2022,07,2,'2022-06-05','2022-07-04','2022-07-12'),
(2022,07,3,'2022-06-06','2022-07-05','2022-07-13'),
(2022,07,4,'2022-06-07','2022-07-06','2022-07-14'),
(2022,07,5,'2022-06-08','2022-07-07','2022-07-15'),
(2022,07,6,'2022-06-09','2022-07-08','2022-07-16'),
(2022,07,7,'2022-06-10','2022-07-09','2022-07-17'),
(2022,07,8,'2022-06-11','2022-07-10','2022-07-18'),
(2022,07,9,'2022-06-12','2022-07-11','2022-07-19'),
(2022,08,0,'2022-07-03','2022-08-02','2022-08-10'),
(2022,08,1,'2022-07-04','2022-08-03','2022-08-11'),
(2022,08,2,'2022-07-05','2022-08-04','2022-08-12'),
(2022,08,3,'2022-07-06','2022-08-05','2022-08-13'),
(2022,08,4,'2022-07-07','2022-08-06','2022-08-14'),
(2022,08,5,'2022-07-08','2022-08-07','2022-08-15'),
(2022,08,6,'2022-07-09','2022-08-08','2022-08-16'),
(2022,08,7,'2022-07-10','2022-08-09','2022-08-17'),
(2022,08,8,'2022-07-11','2022-08-10','2022-08-18'),
(2022,08,9,'2022-07-12','2022-08-11','2022-08-19'),
(2022,09,0,'2022-08-03','2022-09-02','2022-09-10'),
(2022,09,1,'2022-08-04','2022-09-03','2022-09-11'),
(2022,09,2,'2022-08-05','2022-09-04','2022-09-12'),
(2022,09,3,'2022-08-06','2022-09-05','2022-09-13'),
(2022,09,4,'2022-08-07','2022-09-06','2022-09-14'),
(2022,09,5,'2022-08-08','2022-09-07','2022-09-15'),
(2022,09,6,'2022-08-09','2022-09-08','2022-09-16'),
(2022,09,7,'2022-08-10','2022-09-09','2022-09-17'),
(2022,09,8,'2022-08-11','2022-09-10','2022-09-18'),
(2022,09,9,'2022-08-12','2022-09-11','2022-09-19'),
(2022,10,0,'2022-09-03','2022-10-02','2022-10-10'),
(2022,10,1,'2022-09-04','2022-10-03','2022-10-11'),
(2022,10,2,'2022-09-05','2022-10-04','2022-10-12'),
(2022,10,3,'2022-09-06','2022-10-05','2022-10-13'),
(2022,10,4,'2022-09-07','2022-10-06','2022-10-14'),
(2022,10,5,'2022-09-08','2022-10-07','2022-10-15'),
(2022,10,6,'2022-09-09','2022-10-08','2022-10-16'),
(2022,10,7,'2022-09-10','2022-10-09','2022-10-17'),
(2022,10,8,'2022-09-11','2022-10-10','2022-10-18'),
(2022,10,9,'2022-09-12','2022-10-11','2022-10-19'),
(2022,11,0,'2022-10-03','2022-11-02','2022-11-10'),
(2022,11,1,'2022-10-04','2022-11-03','2022-11-11'),
(2022,11,2,'2022-10-05','2022-11-04','2022-11-12'),
(2022,11,3,'2022-10-06','2022-11-05','2022-11-13'),
(2022,11,4,'2022-10-07','2022-11-06','2022-11-14'),
(2022,11,5,'2022-10-08','2022-11-07','2022-11-15'),
(2022,11,6,'2022-10-09','2022-11-08','2022-11-16'),
(2022,11,7,'2022-10-10','2022-11-09','2022-11-17'),
(2022,11,8,'2022-10-11','2022-11-10','2022-11-18'),
(2022,11,9,'2022-10-12','2022-11-11','2022-11-19'),
(2022,12,0,'2022-11-03','2022-12-02','2022-12-10'),
(2022,12,1,'2022-11-04','2022-12-03','2022-12-11'),
(2022,12,2,'2022-11-05','2022-12-04','2022-12-12'),
(2022,12,3,'2022-11-06','2022-12-05','2022-12-13'),
(2022,12,4,'2022-11-07','2022-12-06','2022-12-14'),
(2022,12,5,'2022-11-08','2022-12-07','2022-12-15'),
(2022,12,6,'2022-11-09','2022-12-08','2022-12-16'),
(2022,12,7,'2022-11-10','2022-12-09','2022-12-17'),
(2022,12,8,'2022-11-11','2022-12-10','2022-12-18'),
(2022,12,9,'2022-11-12','2022-12-11','2022-12-19');`)

	if err != nil {
		log.Fatal(err)
	}
}

//función para crear las fuciones
func crearFun() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec(`create or replace function autorizacion_de_compra (nrotarj char(16),
codseg char(4), nrocom int,mon decimal(7,2))
returns bool as $$
declare 
lim numeric;
tarj record;
total numeric;
tot numeric;
desde text;
hasta text;
vald date;
valh date;
begin
select validadesde into desde  from tarjeta where nrotarjeta=nrotarj;
select validahasta into hasta from tarjeta where nrotarjeta=nrotarj;
select(substring(desde from 1 for 4)||substring(desde from 5)||'01')::date into vald ;
select(substring(hasta from 1 for 4)||substring(hasta from 5)||'28')::date into valh;
if current_date < vald or current_date > valh then
insert into rechazo (nrorechazo,nrotarjeta,nrocomercio,fecha,monto,motivo) 
values(default,nrotarj,nrocom,current_timestamp,mon,'plazo de vigencia expirado');
return false;
else
select t.estado into tarj from tarjeta t where t.nrotarjeta= nrotarj and estado='Suspendida';
if found then
insert into rechazo (nrorechazo,nrotarjeta,nrocomercio,fecha,monto,motivo) 
values(default,nrotarj,nrocom,current_timestamp,mon,'la tarjeta se encuentra suspendida');
return false;
else
select t.nrotarjeta,t.estado into tarj from tarjeta t where t.nrotarjeta= nrotarj and estado='Vigente';
if not found then
insert into rechazo (nrorechazo,nrotarjeta,nrocomercio,fecha,monto,motivo) 
values(default,nrotarj,nrocom,current_timestamp,mon,'tarjeta no valida o no vigente');
return false;
else
select t.codseguridad into tarj from tarjeta t where t.codseguridad=codseg and t.nrotarjeta=nrotarj;
if not found then
insert into rechazo (nrorechazo,nrotarjeta,nrocomercio,fecha,monto,motivo) 
values(default,nrotarj,nrocom,current_timestamp,mon,'codigo de seguridad invalido');
return false;
else
select limitecompra into lim from tarjeta where nrotarjeta=nrotarj;
if mon>lim then
insert into rechazo (nrorechazo,nrotarjeta,nrocomercio,fecha,monto,motivo) 
values(default,nrotarj,nrocom,current_timestamp,mon,'Supera limite de tarjeta');
return false;
else
select sum(c.monto) into total from compra c where c.nrotarjeta=nrotarj;
tot:=mon+total;
if tot>lim then
insert into rechazo (nrorechazo,nrotarjeta,nrocomercio,fecha,monto,motivo) 
values(default,nrotarj,nrocom,current_timestamp,mon,'Supera limite de tarjeta');
return false;
else
insert into compra (nrooperacion,nrotarjeta,nrocomercio,fecha,monto,pagado) 
values(default,nrotarj,nrocom,current_timestamp,mon,'true');
return true;
end if;
end if;
end if;
end if;
end if;
end if;
end;
$$ language plpgsql;`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create or replace function resumen(nrocli int, periodo int) 
returns void as $$
declare
client record;
det record;
num int;
n char(16);
tot decimal(7,2);
mon decimal(7,2);
fec int;
cl char(16);
begin
select nrocliente into num from cliente where nrocliente=nrocli;
if not found then
raise 'El numero de cliente % no existe',nrocli;
else 
if periodo <1 or periodo>12 then
raise 'Periodo invalido';
else
select c.nrotarjeta into cl from compra c,tarjeta t,cliente cl where c.nrotarjeta=t.nrotarjeta and t.nrocliente=cl.nrocliente and cl.nrocliente=nrocli;
if not found then
raise 'El cliente no posee compras';
else
select extract(month from c.fecha) into fec from compra c,tarjeta t where c.nrotarjeta=t.nrotarjeta and t.nrocliente=nrocli;
if fec!=periodo then
raise 'No hay compras para este periodo';
else
select nrotarjeta into n from tarjeta t where nrocli=t.nrocliente;
select(substring(n from 16))::int into num;
select c.nombre,c.apellido,c.domicilio, t.nrotarjeta,ci.fechainicio,ci.fechacierre,ci.fechavto into client from cliente c, tarjeta t,cierre ci,compra co 
where c.nrocliente=nrocli and c.nrocliente=t.nrocliente and ci.terminacion=num and ci.mes=periodo;
if found then
insert into cabecera(nroresumen,nombre,apellido,domicilio,nrotarjeta,desde,hasta,vence)
values(default,client.nombre,client.apellido,client.domicilio,client.nrotarjeta,client.fechainicio,client.fechacierre,client.fechavto);


for det in select ca.nroresumen,c.fecha,co.nombre,c.monto from compra c,comercio co,cabecera ca,tarjeta t 
where co.nrocomercio=c.nrocomercio and t.nrotarjeta=c.nrotarjeta and t.nrocliente=nrocli and ca.nrotarjeta=c.nrotarjeta
loop
insert into detalle(nroresumen,nrolinea,fecha,nombrecomercio,monto)
values(det.nroresumen,default,det.fecha,det.nombre,det.monto);
end loop;
select sum(c.monto) into tot from compra c,tarjeta t,cliente cl where c.nrotarjeta=t.nrotarjeta and cl.nrocliente=t.nrocliente and cl.nrocliente=nrocli;
update cabecera
set total=tot
where nroresumen=det.nroresumen;
end if;
end if;
end if;
end if;
end if;
end;
$$ language plpgsql;`)

	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create or replace function alerta_cliente() returns trigger as $$
declare
res record;
begin
insert into alerta(nroalerta,nrotarjeta,fecha,nrorechazo,codalerta,descripcion)
values(default,new.nrotarjeta,new.fecha,new.nrorechazo,0,new.motivo);
select  count(nrotarjeta) into res from rechazo group by nrotarjeta having count(*) >1;
if found then 
update tarjeta set estado='suspendida' where nrotarjeta=new.nrotarjeta;
insert into alerta(nroalerta,nrotarjeta,fecha,nrorechazo,codalerta,descripcion)
values(default,new.nrotarjeta,new.fecha,NULL,32,'dos limites superados el mismo dia');
end if;
return new;
end;
$$ language plpgsql;

create or replace trigger alerta
after insert on rechazo
for each row
execute procedure alerta_cliente();
`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create or replace function alerta_compras() returns trigger as $$
declare
com record;
codpos char(8);
nrocom int;
begin
select codigopostal into codpos from comercio where nrocomercio=new.nrocomercio;
select nrocomercio into nrocom from comercio where nrocomercio!=new.nrocomercio and
codigopostal=codpos;
select * into com from compra where nrotarjeta=new.nrotarjeta and
extract(minute from new.fecha)-extract(minute from fecha)<=1 and
extract(hour from new.fecha)=extract(hour from fecha) and
extract(day from new.fecha)=extract(day from fecha) and nrocomercio!=nrocom;
if found then
insert into alerta(nroalerta,nrotarjeta,fecha,nrorechazo,codalerta,descripcion)values
(default,com.nrotarjeta,com.fecha,NULL,1,'Compra 1 min mismo cod. postal');
else
select codigopostal into codpos from comercio where nrocomercio=new.nrocomercio;
select nrocomercio into nrocom from comercio where nrocomercio!=new.nrocomercio and
codigopostal!=codpos;
select * into com from compra where nrotarjeta=new.nrotarjeta and
extract(minute from new.fecha)-extract(minute from fecha)<=5 and
extract(hour from new.fecha)=extract(hour from fecha) and
extract(day from new.fecha)=extract(day from fecha) and nrocomercio!=nrocom;
if found then
insert into alerta(nroalerta,nrotarjeta,fecha,nrorechazo,codalerta,descripcion)values
(default,com.nrotarjeta,com.fecha,NULL,5,'Compra 5 min diferente cod. postal');
end if;
end if;
return new;
end;
$$ language plpgsql;

create or replace trigger alerta_compra
before insert on compra
for each row
execute procedure alerta_compras();`)

	if err != nil {
		log.Fatal(err)
	}

}

//función que prueba la función autorización de compra
func probarFun() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec(`select autorizacion_de_compra('4861740374483652','2346',337,10600);
select autorizacion_de_compra('4101436112271841','1896',597,5750);
select autorizacion_de_compra('4115513260891341','1124',266,12000);
select autorizacion_de_compra('4115513260891341','1124',374,9000);
select autorizacion_de_compra('4662451537132351','5123',143,800);
select autorizacion_de_compra('4405938055039775','3452',307,1500);
select autorizacion_de_compra('4405938055039775','3452',231,1500);
select autorizacion_de_compra('4405938055039775','3452',597,4000);
select autorizacion_de_compra('4511737412472598','1679',298,6000);
select autorizacion_de_compra('4101305928066852','1349',794,6300);
select autorizacion_de_compra('4524910120323240','7891',794,26300);
select autorizacion_de_compra('4524910120323240','7891',657,16300);`)

	if err != nil {
		log.Fatal(err)
	}
}

//función que genera los resúmenes
func generarResumen() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	_, err = db.Exec(`select resumen(13,11);
	select resumen(03,11);
	select resumen(11,11);`)

	if err != nil {
		log.Fatal(err)
	}
}
func main() {

	var opc int

	for {

		fmt.Printf("MENU DE DATA BASE\n")
		fmt.Printf("1...Crear la base de datos\n")
		fmt.Printf("2...Crear las tablas\n")
		fmt.Printf("3...Crear primary keys y foreing keys\n")
		fmt.Printf("4...Borrar las primary keys y foreings keys\n")
		fmt.Printf("5...Cargar datos de las tablas\n")
		fmt.Printf("6...Crear funciones\n")
		fmt.Printf("7...Para probar funciones y consumos\n")
		fmt.Printf("8...Generar los resumenes\n")
		fmt.Printf("9..Salir\n")
		fmt.Printf("Ingrese opcion: ")
		fmt.Scanf("%d", &opc)
		switch opc {
		case 1:
			createDatabase()
			db, err := sql.Open("postgres", "user=postgres host=localhost dbname=tp sslmode=disable")
			if err != nil {
				log.Fatal(err)
			}
			defer db.Close()
			break
		case 2:
			crearTablas()
			break
		case 3:
			crearKeys()
			break
		case 4:
			borrarKeys()
		case 5:
			cargarDatos()
			break
		case 6:
			crearFun()
			break
		case 7:
			probarFun()
			break
		case 8:
			generarResumen()
			break
		case 9:
		break	
		}
		if opc < 1 && opc > 9 {
			break

		}

	}
}
