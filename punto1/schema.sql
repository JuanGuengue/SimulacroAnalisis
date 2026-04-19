-- Drop table

-- DROP TABLE public.dim_producto;

CREATE TABLE public.dim_producto (
	id_producto serial4 NOT NULL,
	sku varchar(50) NOT NULL,
	nombre varchar(50) NOT NULL,
	categoria varchar(50) NOT NULL,
	talla varchar(50) NOT NULL,
	CONSTRAINT dim_producto_pkey PRIMARY KEY (id_producto)
);

ALTER TABLE public.fact_ventas ADD amount numeric(12, 2) NULL;

ALTER TABLE public.dim_tiempo ADD trimestre int4 NULL;

ALTER TABLE public.fact_ventas ADD id_producto int4 NULL;

ALTER TABLE public.dim_tiempo ADD id_tiempo serial4 NOT NULL;

-- Drop table

-- DROP TABLE public.fact_ventas;

CREATE TABLE public.fact_ventas (
	order_id int4 NOT NULL,
	id_tiempo int4 NULL,
	id_producto int4 NULL,
	id_envio int4 NULL,
	amount numeric(12, 2) NULL,
	qty int4 NULL,
	ticket_promedio numeric(12, 2) NULL,
	CONSTRAINT fact_ventas_pkey PRIMARY KEY (order_id),
	CONSTRAINT fact_ventas_id_envio_fkey FOREIGN KEY (id_envio) REFERENCES public.dim_envio(id_envio),
	CONSTRAINT fact_ventas_id_producto_fkey FOREIGN KEY (id_producto) REFERENCES public.dim_producto(id_producto),
	CONSTRAINT fact_ventas_id_tiempo_fkey FOREIGN KEY (id_tiempo) REFERENCES public.dim_tiempo(id_tiempo)
);
CREATE INDEX idx_fact_ventas_tiempo ON public.fact_ventas USING btree (id_tiempo);

ALTER TABLE public.dim_producto ADD id_producto serial4 NOT NULL;

ALTER TABLE public.dim_envio ADD ship_service_level varchar(50) NULL;

ALTER TABLE public.dim_producto ADD talla varchar(50) NOT NULL;

ALTER TABLE public.dim_producto ADD categoria varchar(50) NOT NULL;

ALTER TABLE public.dim_envio ADD id_envio serial4 NOT NULL;

ALTER TABLE public.fact_ventas ADD id_tiempo int4 NULL;

ALTER TABLE public.dim_tiempo ADD anio int4 NULL;

ALTER TABLE public.dim_producto ADD sku varchar(50) NOT NULL;

ALTER TABLE public.dim_envio ADD ship_state varchar(50) NULL;

ALTER TABLE public.dim_tiempo ADD mes int4 NULL;

ALTER TABLE public.fact_ventas ADD qty int4 NULL;

ALTER TABLE public.dim_envio ADD ciudad varchar(100) NULL;

ALTER TABLE public.dim_tiempo ADD fecha_completa date NOT NULL;

ALTER TABLE public.dim_producto ADD nombre varchar(50) NOT NULL;

-- Drop table

-- DROP TABLE public.dim_envio;

CREATE TABLE public.dim_envio (
	id_envio serial4 NOT NULL,
	ship_service_level varchar(50) NULL,
	ship_state varchar(50) NULL,
	ciudad varchar(100) NULL,
	pais varchar(50) NULL,
	CONSTRAINT dim_envio_pkey PRIMARY KEY (id_envio)
);

ALTER TABLE public.fact_ventas ADD ticket_promedio numeric(12, 2) NULL;

ALTER TABLE public.fact_ventas ADD order_id int4 NOT NULL;

ALTER TABLE public.fact_ventas ADD id_envio int4 NULL;

ALTER TABLE public.dim_tiempo ADD dia int4 NULL;

ALTER TABLE public.dim_envio ADD pais varchar(50) NULL;