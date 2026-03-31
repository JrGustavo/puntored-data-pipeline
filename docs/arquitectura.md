# Diagrama de Arquitectura — Puntored Data Pipeline

## Flujo Medallion
```
┌─────────────────────────────────────────────────────────┐
│                    FUENTES DE DATOS                      │
│  Faker (es_CO) · APIs · CSV · Base de datos             │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                  CAPA BRONZE                             │
│  • Datos crudos sin transformación                       │
│  • Formato: Parquet particionado por fecha               │
│  • Partición: date=YYYY-MM-DD                            │
│  • Incremental: omite si ya existe data del día          │
│                                                          │
│  data/bronze/                                            │
│  ├── users/date=2026-03-30/users.parquet                 │
│  ├── transactions/date=2026-03-30/transactions.parquet   │
│  └── transaction_details/date=.../details.parquet        │
└──────────────────────┬──────────────────────────────────┘
                       │  6 reglas de negocio aplicadas
                       ▼
┌─────────────────────────────────────────────────────────┐
│                  CAPA SILVER                             │
│  • Eliminación de duplicados (transaction_id)            │
│  • amount > 0 obligatorio                                │
│  • status estandarizado a lowercase                      │
│  • Integridad referencial users→transactions→details     │
│  • Manejo de nulls en campos no obligatorios             │
│  • Trazabilidad: _source + _silver_processed_at          │
│                                                          │
│  data/silver/                                            │
│  ├── users/date=2026-03-30/users.parquet                 │
│  ├── transactions/date=2026-03-30/transactions.parquet   │
│  └── transaction_details/date=.../details.parquet        │
└──────────────────────┬──────────────────────────────────┘
                       │  DuckDB SQL Analytics
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   CAPA GOLD                              │
│  • gold_user_metrics      — KPIs por usuario             │
│  • gold_channel_metrics   — métricas web/mobile/api      │
│  • gold_payment_metrics   — card/pse/cash/nequi/daviplata│
│  • gold_summary           — resumen ejecutivo negocio    │
│                                                          │
│  Expuesto via DuckDB SQL para consumo analítico          │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              CALIDAD Y OBSERVABILIDAD                    │
│  • 18 validaciones automáticas · 100% pass rate         │
│  • Conteo de registros por capa                          │
│  • Null checks en campos obligatorios                    │
│  • Freshness check                                       │
│  • Integridad referencial                                │
│  • Reporte CSV en logs/                                  │
│  • Logging estructurado timestamp | modulo | nivel       │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│              ORQUESTACIÓN — APACHE AIRFLOW               │
│                                                          │
│  DAG: pipeline_puntored_medallion                        │
│  Schedule: 0 6 * * * (todos los días 6AM)                │
│                                                          │
│  inicio → bronze → silver → gold → quality → fin        │
│                                                          │
│  • Reintentos: 3 con delay de 5 minutos                  │
│  • Max active runs: 1                                    │
│  • XCom para pasar conteos entre tasks                   │
│  • Falla en quality si pass rate < 100%                  │
└─────────────────────────────────────────────────────────┘

## Modelo de Datos

### Relaciones
users (1) ──── (N) transactions (1) ──── (N) transaction_details

### Reglas de negocio
| Regla | Implementación |
|---|---|
| amount > 0 | Silver — filtra negativos y ceros |
| No duplicados | Silver — drop_duplicates por transaction_id |
| Status lowercase | Silver — str.lower().strip() |
| Integridad referencial | Silver — merge con usuarios válidos |
| Nulls en no obligatorios | Silver — fillna("unknown") |

## Decisiones Técnicas

| Decisión | Alternativa | Razón |
|---|---|---|
| DuckDB | SQLite / PostgreSQL | Motor OLAP moderno, sin servidor, ideal para Gold local |
| Parquet | CSV / JSON | Columnar, comprimido, estándar Lakehouse (Databricks) |
| Faker es_CO | Datos estáticos | Simula contexto colombiano real de Punto Red |
| Módulos independientes | Script monolítico | Principio responsabilidad única, testeable por capa |
| Incremental por fecha | Full refresh | Evita reprocesar datos ya procesados |
```


