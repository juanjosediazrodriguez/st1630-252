"""gen_dataset.py — Lab 4a (ST1630-2026-2, S11)

Genera el dataset que usa este lab: ventas_colombia_lab4a.csv, con el
mismo espíritu de columnas que el Bronze del Lab 1b (pedido_id, fecha,
categoria, producto, cantidad, precio_unit, total, metodo_pago,
devuelto, calificacion, region, canal, vendedor_id), pero autocontenido:
no depende de tu bucket de S3 ni de tu clúster EMR de los Labs 1a/1b,
que para esta semana del curso probablemente ya no sigan vivos.

El dataset cubre 10 días de "ventas" (2026-01-01 a 2026-01-10) para que
puedas correr un backfill real sobre varias fechas con `airflow dags
backfill`. A diferencia del dataset de Lab 1b, este ya viene con
region/canal en su forma canónica (ese ejercicio de normalización ya lo
hiciste en Lab 1b) -- el reto de esta semana es otro: idempotencia,
calidad de datos y orquestación, no limpiar texto otra vez.

Uso:
    python3 gen_dataset.py

Esto (re)genera ./ventas_colombia_lab4a.csv de forma determinista
(semilla fija: 1630) -- si lo borras, correr este script de nuevo
produce exactamente el mismo archivo, fila por fila.
"""

import csv
import random
from datetime import date, timedelta

random.seed(1630)

FECHA_INICIO = date(2026, 1, 1)
DIAS = 10
FILAS_POR_DIA_RANGO = (300, 600)

REGIONES = ["BOGOTÁ", "MEDELLÍN", "CALI", "BARRANQUILLA", "BUCARAMANGA", "OTRO"]
CANALES = ["app_movil", "web", "tienda_fisica", "telefono"]
CATEGORIAS = ["Electrónica", "Hogar", "Moda", "Deportes", "Mercado"]
METODOS_PAGO = ["tarjeta_credito", "tarjeta_debito", "pse", "efectivo"]

# (producto, categoria, precio_unit_base) -- precio_unit_base se usa para
# que el "total" tenga sentido de negocio (cantidad * precio_unit), no un
# número aleatorio sin relación con el producto.
PRODUCTOS = [
    ("Audífonos Bluetooth", "Electrónica", 89900),
    ("Smart TV 50 pulgadas", "Electrónica", 1899900),
    ("Licuadora 5 velocidades", "Hogar", 129900),
    ("Cafetera eléctrica", "Hogar", 159900),
    ("Tenis running", "Deportes", 219900),
    ("Balón de fútbol", "Deportes", 69900),
    ("Camiseta algodón", "Moda", 49900),
    ("Jean clásico", "Moda", 109900),
    ("Arroz 5kg", "Mercado", 24900),
    ("Aceite de cocina 1L", "Mercado", 11900),
]


def generar_filas():
    filas = []
    pedido_id = 1
    for offset in range(DIAS):
        fecha = FECHA_INICIO + timedelta(days=offset)
        n_filas = random.randint(*FILAS_POR_DIA_RANGO)
        for _ in range(n_filas):
            producto, categoria_sugerida, precio_base = random.choice(PRODUCTOS)
            cantidad = random.randint(1, 5)
            # +/- 10% de variación sobre el precio base, como pasaría con
            # descuentos/promos reales -- redondeado a centenas de peso.
            precio_unit = round(precio_base * random.uniform(0.9, 1.1), -2)
            total = round(cantidad * precio_unit, 2)
            devuelto = random.random() < 0.04  # ~4% de devoluciones
            filas.append(
                {
                    "pedido_id": f"PED-{pedido_id:06d}",
                    "fecha": fecha.isoformat(),
                    "categoria": categoria_sugerida,
                    "producto": producto,
                    "cantidad": cantidad,
                    "precio_unit": precio_unit,
                    "total": total,
                    "metodo_pago": random.choice(METODOS_PAGO),
                    "devuelto": devuelto,
                    "calificacion": random.randint(1, 5),
                    "region": random.choice(REGIONES),
                    "canal": random.choice(CANALES),
                    "vendedor_id": f"V{random.randint(1, 25):03d}",
                }
            )
            pedido_id += 1
    return filas


def main():
    filas = generar_filas()
    columnas = list(filas[0].keys())
    with open("ventas_colombia_lab4a.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columnas)
        writer.writeheader()
        writer.writerows(filas)
    print(f"Generadas {len(filas):,} filas en ventas_colombia_lab4a.csv "
          f"({DIAS} días, {FECHA_INICIO.isoformat()} a "
          f"{(FECHA_INICIO + timedelta(days=DIAS - 1)).isoformat()})")


if __name__ == "__main__":
    main()
