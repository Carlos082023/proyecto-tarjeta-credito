drop database if exists tp;
create database tp;

\c tp

create table cliente(
	nrocliente int,
	nombre text,
	apellido text,
	domicilio text,
	telefono char(12)
);

create table tarjeta(
    nrotarjeta char(16),
    nrocliente int,
    validadesde char(6),
    validahasta char(6),
    codseguridad char(4),
    limitecompra decimal(8,2),
    estado char(10)
);

create table comercio(
    nrocomercio int,
    nombre text,
    domicilio text,
    codigopostal char(8),
    telefono char(12)
);
create sequence compra_nrooperacion_seq;
create table compra(
    nrooperacion int not null default nextval('compra_nrooperacion_seq'),
    nrotarjeta char(16),
    nrocomercio int,
    fecha timestamp,
    monto decimal(7,2),
    pagado boolean
);
create sequence rechazo_nrorechazo_seq;
create table rechazo(
    nrorechazo int not null default nextval('rechazo_nrorechazo_seq'),
    nrotarjeta char(16),
    nrocomercio int,
    fecha timestamp,
    monto decimal(7,2),
    motivo text
);

create table cierre(
    anio int,
    mes int,
    terminacion int,
    fechainicio date,
    fechacierre date,
    fechavto date
);
create sequence cabecera_nroresumen_seq;
create table cabecera(
    nroresumen int not null default nextval('cabecera_nroresumen_seq'),
    nombre text,
    apellido text,
    domicilio text,
    nrotarjeta char(16),
    desde date,
    hasta date,
    vence date,
    total decimal(8,2)
);

create table detalle(
    nroresumen int,
    nrolinea int,
    fecha date,
    nombrecomercio text,
    monto decimal(7,2)
 );
 
 create sequence alerta_nroalerta_seq;
 create table alerta(
    nroalerta int not null default nextval('alerta_nroalerta_seq'),
    nrotarjeta char(16),
    fecha timestamp,
    nrorechazo int,
    codalerta int,
    descripcion text
);

create table consumo(
	nrotarjeta char (16),
	codseguridad char(4),
	nrocomercio int,
	monto decimal(7,2) 
    
);

alter table cliente  add constraint cliente_pk  primary key (nrocliente);
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
alter table alerta   add constraint alerta_nrorechazo_fk   foreign key (nrorechazo)  references rechazo(nrorechazo);


insert into cliente values
(1,'Sofia','Ciechomski','Rawson 1869','47903732'),
(2,'Nicolas','Di Cesare','Entre Rios 2721','47906564'),
(3,'Carlos','Vladislavic','Warnes 546','47904546'),
(4,'Patricia','Chaca','Juan de Garay 4213','47903232'),
(5,'Florencia','Rodriguez','Libertador 14500','47983432'),
(6,'Julieta','Fimiani','Sarmiento 4091','46531243'),
(7,'Roberto','Clerici','Cabildo 533','47924378'),
(8,'Santiago','Desimone','Cramer 2343','47885456'),
(9,'Lucia','Tapia','Warnes 2198','47963344'),
(10,'Cristina','Mac cormack','Marmol 2541','47995577'),
(11,'Miriam','Ucañan','Quintana 1650','47952211'),
(12,'Leandro','Benitez','Parana 1122','47901122'),
(13,'Lidia','Lau','San Martin 2830','47992233'),
(14,'Marcos','Zavaleta','Jose M. Bosch 4530','47885687'),
(15,'Romina','Velasquez','Psje el Cano 2831','47924532'),
(16,'Roberto','Gutierrez','Aldo de la Rosa 2381','47945657'),
(17,'Antonio','Moreno','Laprida 1443','47994351'),
(18,'Daniel','Acosta','Pedernera 3322','47934546'),
(19,'Ivan','Soria','Juarez 4630','47965477'),
(20,'Marilin','Rodriguez','Lavalle 124','47953104');

insert into comercio values
(102,'Williamburg','Arieta 3545','B1754APC','47901213'),
(143,'Todo Moda','Av. Rivadavia 14450','B1704ERA','47921244'),
(188,'YPF','Luis Guemes 369','B1706EYH','47919988'),
(212,'Bucare','Ramon Falcon 7145','C1406GNA','47327711'),
(231,'Chinin','Pueyrredon 4316','B1650KUA','47984322'),
(266,'Dash','Alvear 2693','B1653FVA','47924455'),
(295,'Repuestos Ĺeandro SRL','Juarez 4917','B1650KUA','46672324'),
(298,'HP','Independencia 4602','B1653FVA','49873435'),
(307,'Santa Marta','Belgrano 3329','B1650KUA','47552329'),
(319,'Cuesta Blanca','Av. Cabildo 2128','C1428AAQ','47934566'),
(337,'Samsung','Vedia 3600','C1430DAF','47988822'),
(358,'Morita','Calle 9 de julio 1415','B1820KJG','47038871'),
(374,'Carrefour','Laprida 342','B1832HOH','47047790'),
(396,'Fiestisima','Av. Cordoba 2331','C1120AAF','47804433'),
(416,'Ver','Gallo 1330','C1425EFD','47924435'),
(455,'Elefante Bar','Eduardo Costa 2024','B1640BAP','49982341'),
(597,'Coppel','Av Maipu 2841','B1636AAH','47925005'),
(642,'Biblos','Av. Maipu 3001','B1636AAK','47992002'),
(657,'Pizza Cero','Av Libertador 1800','C1112ABP','47963300'),
(794,'Howard Johnson','Aristobulo del Valle 1259','B1640EQS','47754332'); 

insert into tarjeta values
('4662451537132351',1,'202001','202210','123',5000,'Vigente'),
('4861740374483652',2,'201911','202312','234',8000,'Vigente'),
('4405938055039775',3,'202201','202702','345',10000,'Vigente'),
('4652319479526440',4,'202112','202503','456',25000,'Vigente'),
('4405203219837445',5,'201811','202603','567',30000,'Vigente'),
('4144516916019635',6,'202004','202709','678',7000,'Vigente'),
('4524910120323240',7,'201803','202503','789',12500,'Vigente'),
('4667844291721309',8,'202201','202807','890',9000,'Vigente'),
('4127661197753240',9,'202112','202609','910',34000,'Vigente'),
('4964853426625332',10,'201909','202710','101',17900,'Vigente'),
('4115513260891341',11,'202205','202804','112',16000,'Vigente'),
('4662793715981189',12,'202105','202703','193',20000,'Vigente'),
('4101305928066852',13,'202004','202703','134',13000,'Vigente'),
('4127299546411227',14,'201802','202801','145',19000,'Vigente'),
('4775163311895101',15,'202008','202403','156',20000,'Vigente'),
('4511737412472598',16,'202204','202803','167',15000,'Suspendida'),
('4839930728243240',17,'202207','202708','178',9000,'Vigente'),
('4101436112271841',18,'202004','202604','189',12500,'Anulada'),
('4534744527996438',19,'201906','202504','192',14000,'Vigente'),
('4183533125986089',20,'201803','202304','201',12500,'Vigente'),
('4320906496542598',1,'201801','202209','212',12500,'Vigente'),
('4661737412272589',20,'202206','202801','223',15000,'Vigente');

insert into consumo values
('4861740374483652','234',337,10600),
('4101436112271841','189',597,5750),
('4661737412272589','223',266,12000),
('4661737412272589','223',374,45000),
('4101305928066852','134',794,63000);


create or replace function autorizacion_de_compra (nrotarj char(16),
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
select(substring(hasta from 1 for 4)||substring(hasta from 5)||'30')::date into valh;
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
if mon<lim then
select sum(c.monto) into total from compra c where c.nrotarjeta=nrotarj;
tot:=mon+total;
else 
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
$$ language plpgsql;

create or replace function resumen(nrocli int, periodo int) 
--validar fechas de compra y hacer if secuenciales.
returns void as $$
declare
client record;
det record;
num int;
n char(16);
tot decimal(7,2);
fec int;
begin
select nrotarjeta into n from tarjeta t where nrocli=t.nrocliente;
select(substring(n from 16))::int into num;
--select extract(month from fecha) into fec from compra;
for periodo in (select extract(month from fecha) from compra) loop
if found then
select c.nombre,c.apellido,c.domicilio, t.nrotarjeta,ci.fechainicio,ci.fechacierre,ci.fechavto into client from cliente c, tarjeta t,cierre ci 
where c.nrocliente=nrocli and c.nrocliente=t.nrocliente and ci.terminacion=num and ci.mes=periodo;
end loop;
insert into cabecera(nroresumen,nombre,apellido,domicilio,nrotarjeta,desde,hasta,vence)
values(default,client.nombre,client.apellido,client.domicilio,client.nrotarjeta,client.fechainicio,client.fechacierre,client.fechavto);
select ca.nroresumen,c.fecha,co.nombre,c.monto into det from compra c,comercio co,cabecera ca where co.nrocomercio=c.nrocomercio and ca.nrotarjeta=c.nrotarjeta;
insert into detalle(nroresumen,nrolinea,fecha,nombrecomercio,monto)
values(det.nroresumen,default,det.fecha,det.nombre,det.monto);
select sum(d.monto) into tot from detalle d,cabecera c where d.nroresumen=c.nroresumen;
update cabecera
set total=tot
where nroresumen=det.nroresumen;

--raise '%',fec;
end;
$$ language plpgsql;

create or replace function alerta_cliente() returns trigger as $$
declare
res record;
begin
select nrotarjeta,fecha,nrorechazo,motivo into res from rechazo; 
insert into alerta(nroalerta,nrotarjeta,fecha,nrorechazo,codalerta,descripcion)
values(default,res.nrotarjeta,res.fecha,res.nrorechazo,0,res.motivo);
return new;
end;
$$ language plpgsql;

create or replace trigger alerta
after insert on rechazo
for each row
execute procedure alerta_cliente();

