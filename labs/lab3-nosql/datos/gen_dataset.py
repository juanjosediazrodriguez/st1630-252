"""gen_dataset.py — Lab 3 (ST1630-2026-2)

Genera TU dataset personal para este lab: el mismo dominio de ventas de
los labs anteriores (clientes, productos, pedidos con ítems), más un
stream de eventos de venta en vivo para la Parte 3.

Cada estudiante obtiene datos distintos: la semilla sale de tu usuario
EAFIT. Por eso todos los números que cites en tu entrega (conteos,
tiempos, recomendaciones) son tuyos y de nadie más -- y por eso tu
usuario tiene que ser el mismo en todos los pasos del lab.

Uso (desde labs/lab3-nosql/, dentro del contenedor app):
    python datos/gen_dataset.py --usuario <tu-usuario-eafit>

Genera datos/<tu-usuario>/{clientes,productos,pedidos,eventos_stream}.json
Es determinista: correrlo de nuevo con el mismo usuario produce los mismos
archivos.
"""

import argparse
import hashlib
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

FECHA_FIN = datetime(2026, 10, 15, 23, 59, 59)
DIAS = 60
N_CLIENTES = 1500
N_PEDIDOS = 15000
N_EVENTOS = 5000
TASA_REENTREGA = 0.20  # fracción de eventos que llegan duplicados (at-least-once)

REGIONES = ["BOGOTÁ", "MEDELLÍN", "CALI", "BARRANQUILLA", "BUCARAMANGA"]
PESO_REGION = [0.40, 0.22, 0.17, 0.12, 0.09]
CANALES = ["app_movil", "web", "tienda_fisica", "telefono"]
SEGMENTOS = ["masivo", "premium", "empresarial"]
NOMBRES = ["Ana", "Carlos", "Laura", "Andrés", "Valentina", "Juan", "Camila", "Santiago",
           "Daniela", "Mateo", "Sara", "Felipe", "Mariana", "Sebastián", "Isabella", "David"]
APELLIDOS = ["Gómez", "Rodríguez", "López", "Martínez", "García", "Pérez", "Sánchez",
             "Ramírez", "Torres", "Restrepo", "Vélez", "Ospina", "Cardona", "Zapata"]
VARIANTES = [("Lite", 0.7), ("Básico", 1.0), ("Plus", 1.3), ("Pro", 1.8), ("Max", 2.5)]
GENERACIONES = [2023, 2024, 2025, 2026]
CATEGORIAS = {
    "Electrónica": ["Audífonos", "Cargador", "Mouse", "Teclado", "Parlante", "Smartwatch",
                    "Cámara web", "Power bank", "Monitor", "Tablet", "Router", "Memoria USB"],
    "Hogar": ["Licuadora", "Sartén", "Cafetera", "Lámpara", "Toalla", "Almohada",
              "Olla a presión", "Juego de vasos", "Cortina", "Ventilador", "Aspiradora", "Plancha"],
    "Moda": ["Camiseta", "Jean", "Chaqueta", "Tenis", "Gorra", "Bufanda",
             "Vestido", "Cinturón", "Medias", "Sudadera", "Bolso", "Gafas de sol"],
    "Deportes": ["Balón", "Mancuernas", "Tapete yoga", "Bicicleta estática", "Termo", "Raqueta",
                 "Guantes boxeo", "Cuerda saltar", "Casco ciclismo", "Banda elástica", "Morral", "Rodillera"],
    "Mercado": ["Café 500g", "Arroz 5kg", "Aceite 1L", "Panela", "Chocolate", "Avena",
                "Atún x3", "Pasta 1kg", "Lentejas", "Queso", "Arepas x10", "Huevos x30"],
}


def semilla(usuario: str) -> int:
    return int(hashlib.sha256(usuario.strip().lower().encode()).hexdigest()[:8], 16)


def generar(usuario: str) -> dict:
    rng = random.Random(semilla(usuario))

    productos = []
    for categoria, nombres in CATEGORIAS.items():
        for nombre in nombres:
            base = rng.randrange(8_000, 600_000, 100)
            for variante, factor in VARIANTES:
                for generacion in GENERACIONES:
                    productos.append({
                        "producto_id": f"P{len(productos) + 1:04d}",
                        "nombre": f"{nombre} {variante} {generacion}",
                        "categoria": categoria,
                        "precio": int(base * factor * (0.8 + 0.1 * (generacion - 2023))) // 100 * 100,
                    })
    # Popularidad desigual: algunos productos venden mucho más que otros
    popularidad = [rng.paretovariate(2.5) for _ in productos]
    indice = {p["producto_id"]: i for i, p in enumerate(productos)}

    clientes = []
    for i in range(N_CLIENTES):
        region = rng.choices(REGIONES, PESO_REGION)[0]
        clientes.append({
            "cliente_id": f"C{i + 1:05d}",
            "nombre": f"{rng.choice(NOMBRES)} {rng.choice(APELLIDOS)}",
            "email": f"cliente{i + 1:05d}@correo.co",
            "region": region,
            "segmento": rng.choices(SEGMENTOS, [0.7, 0.25, 0.05])[0],
        })
    # Cada cliente tiene "gustos": 2 categorías favoritas, para que las
    # compras compartidas (y las recomendaciones) tengan estructura
    gustos = {c["cliente_id"]: rng.sample(list(CATEGORIAS), 2) for c in clientes}
    por_categoria = {cat: [p for p in productos if p["categoria"] == cat] for cat in CATEGORIAS}

    inicio = FECHA_FIN - timedelta(days=DIAS)
    pedidos = []
    for i in range(N_PEDIDOS):
        cliente = rng.choice(clientes)
        fecha = inicio + timedelta(seconds=rng.randint(0, DIAS * 86400 - 1))
        n_items = rng.choices([1, 2, 3, 4], [0.45, 0.30, 0.15, 0.10])[0]
        elegidos = {}
        for _ in range(n_items):
            if rng.random() < 0.8:
                pool = por_categoria[rng.choice(gustos[cliente["cliente_id"]])]
            else:
                pool = productos
            pesos = [popularidad[indice[p["producto_id"]]] for p in pool]
            p = rng.choices(pool, pesos)[0]
            elegidos[p["producto_id"]] = p
        items = [{"producto_id": p["producto_id"], "cantidad": rng.choices([1, 2, 3], [0.7, 0.2, 0.1])[0],
                  "precio_unit": p["precio"]} for p in elegidos.values()]
        pedidos.append({
            "pedido_id": f"PED{i + 1:06d}",
            "cliente_id": cliente["cliente_id"],
            "fecha": fecha.isoformat(timespec="seconds"),
            "region": cliente["region"],
            "canal": rng.choice(CANALES),
            "items": items,
            "total": sum(it["cantidad"] * it["precio_unit"] for it in items),
        })
    pedidos.sort(key=lambda p: p["fecha"])

    # Stream de ventas en vivo (Parte 3): eventos únicos + reentregas
    # at-least-once (el mismo event_id llega más de una vez, a veces tarde)
    base = datetime(2026, 10, 16, 8, 0, 0)
    unicos = []
    for i in range(N_EVENTOS):
        p = rng.choices(productos, popularidad)[0]
        unicos.append({
            "event_id": f"EV{i + 1:06d}",
            "producto_id": p["producto_id"],
            "cantidad": rng.choices([1, 2, 3], [0.7, 0.2, 0.1])[0],
            "ts": (base + timedelta(milliseconds=i * 700)).isoformat(timespec="milliseconds"),
        })
    eventos = list(unicos)
    for ev in rng.sample(unicos, int(N_EVENTOS * TASA_REENTREGA)):
        pos = min(len(eventos), unicos.index(ev) + rng.randint(1, 400))
        eventos.insert(pos, dict(ev))

    return {"clientes": clientes, "productos": productos, "pedidos": pedidos, "eventos_stream": eventos}


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera el dataset personal del Lab 3")
    parser.add_argument("--usuario", required=True, help="tu usuario EAFIT (la parte antes de @eafit.edu.co)")
    args = parser.parse_args()
    usuario = args.usuario.strip().lower()

    destino = Path(__file__).parent / usuario
    destino.mkdir(parents=True, exist_ok=True)
    for nombre, datos in generar(usuario).items():
        (destino / f"{nombre}.json").write_text(json.dumps(datos, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"✓ {destino / (nombre + '.json')}: {len(datos):,} registros")
    print(f"\nDataset de '{usuario}' listo (semilla {semilla(usuario)}).")


if __name__ == "__main__":
    main()
