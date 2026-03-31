from src.ingesta.bronze_loader import run_bronze
from src.transformacion.silver_transformer import run_silver
from src.modelado.gold_builder import run_gold
from src.calidad.quality_checks import run_quality_checks

if __name__ == "__main__":
    print("\n🔶 EJECUTANDO BRONZE...")
    run_bronze()

    print("\n🥈 EJECUTANDO SILVER...")
    run_silver()

    print("\n🥇 EJECUTANDO GOLD...")
    run_gold()

    print("\n🔍 EJECUTANDO QUALITY CHECKS...")
    report = run_quality_checks()
    summary = report.summary()
    print(f"\n{'='*40}")
    print(f"  CALIDAD: {summary['passed']}/{summary['total_checks']} checks pasados ({summary['pass_rate']}%)")
    print(f"{'='*40}")