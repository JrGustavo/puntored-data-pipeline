# Puntored Data Pipeline

Pipeline de datos tipo Medallion (Bronze → Silver → Gold) para procesar
transacciones financieras de Punto Red.

## Arquitectura
```
Fuentes de datos
      ↓
  [BRONZE]  ← Datos crudos particionados por fecha (Parquet)
      ↓
  [SILVER]  ← Datos limpios con reglas de negocio aplicadas
      ↓
  [GOLD]    ← KPIs y métricas analíticas listos para consumo
      ↓
  [QUALITY] ← 18 validaciones automáticas con reporte CSV
      ↓
  [AIRFLOW] ← Orquestación diaria 6AM con reintentos automáticos
```

## Stack Tecnológico

| Herramienta | Uso |
|---|---|
| Python 3.12 | Lenguaje principal |
| pandas | Transformación de datos |
| DuckDB | Motor SQL analítico en capa Gold |
| Faker | Generación de datos sintéticos |
| PyArrow / Parquet | Almacenamiento columnar particionado |
| Apache Airflow | Orquestación del pipeline |
| python-dotenv | Gestión de variables de entorno |

## Estructura del Proyecto
```
puntored-data-pipeline/
├── data/
│   ├── bronze/          # Datos crudos particionados por fecha
│   ├── silver/          # Datos limpios
│   └── gold/            # KPIs y métricas
├── src/
│   ├── ingesta/         # Nivel 0: generación e ingesta Bronze
│   ├── transformacion/  # Nivel 1: limpieza Silver
│   ├── modelado/        # Nivel 2: KPIs Gold con DuckDB
│   ├── calidad/         # Nivel 3: 18 quality checks
│   └── utils/           # Logger y Config
├── dags/                # Nivel 4: DAG de Airflow
├── logs/                # Logs del pipeline y reportes de calidad
└── main.py              # Ejecutor principal
```

## Instalación
```bash
git clone https://github.com/TU_USUARIO/puntored-data-pipeline.git
cd puntored-data-pipeline
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ejecución
```bash
python main.py
```

## Modelo de Datos

### Bronze (datos crudos)
- `users` — 100 usuarios con datos colombianos
- `transactions` — 500+ transacciones con duplicados intencionales
- `transaction_details` — detalles por canal y método de pago

### Silver (datos limpios)
Reglas aplicadas:
- ✅ Eliminación de duplicados por `transaction_id`
- ✅ `amount > 0` obligatorio
- ✅ `status` estandarizado a lowercase
- ✅ Integridad referencial users → transactions → details
- ✅ Manejo de nulls en campos no obligatorios

### Gold (KPIs analíticos)
- `gold_user_metrics` — métricas por usuario
- `gold_channel_metrics` — métricas por canal (web/mobile/api)
- `gold_payment_method_metrics` — métricas por método de pago
- `gold_summary` — resumen ejecutivo del negocio

## Calidad de Datos

18 checks automáticos con 100% pass rate:
- Conteo de registros por capa
- Trazabilidad Bronze → Silver
- Null checks en campos obligatorios
- Validación de reglas de negocio
- Integridad referencial
- Freshness check

## Orquestación (Airflow)

DAG: `pipeline_puntored_medallion`
- Schedule: `0 6 * * *` — todos los días a las 6AM
- Reintentos: 3 con delay de 5 minutos
- Max active runs: 1
- Flujo: inicio → bronze → silver → gold → quality → fin

## Decisiones Técnicas

**DuckDB sobre SQLite:** Motor OLAP moderno optimizado para queries
analíticas, sin servidor, perfecto para capas Gold locales.

**Parquet particionado por fecha:** Permite procesamiento incremental
eficiente y es el estándar en arquitecturas Lakehouse (Databricks/Delta Lake).

**Faker con locale es_CO:** Datos sintéticos realistas con nombres,
emails y fechas colombianas para simular el contexto real de Punto Red.

**Separación de capas en módulos independientes:** Cada nivel es
independiente y testeable — principio de responsabilidad única.

## Autor

Gustavo Adolfo Alvarado Medina
Senior Data Engineer
gadolfoalvarado@gmail.com
