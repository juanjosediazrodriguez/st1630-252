"""Maestros diarios (RF-04): catálogo, inventario, clientes y vendedores.

Los snapshots son deterministas por (seed, día): el día N se obtiene aplicando N
pasos de deriva (cambios de precio y de categoría, productos y clientes nuevos)
al snapshot base. Así la carga diaria produce cambios reales para probar SCD2
(dim_product) sin guardar estado entre ejecuciones.

Los identificadores de cliente están seudonimizados: no se genera PII.
"""
from __future__ import annotations

import csv
import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

CATEGORIES = {1: "Tecnología", 2: "Hogar", 3: "Moda", 4: "Belleza", 5: "Deportes"}

# (ciudad, departamento, peso)
CITIES = [
    ("Bogotá", "Cundinamarca", 30), ("Medellín", "Antioquia", 18), ("Cali", "Valle del Cauca", 12),
    ("Barranquilla", "Atlántico", 8), ("Cartagena", "Bolívar", 5), ("Bucaramanga", "Santander", 5),
    ("Pereira", "Risaralda", 4), ("Manizales", "Caldas", 3), ("Cúcuta", "Norte de Santander", 3),
    ("Santa Marta", "Magdalena", 3), ("Ibagué", "Tolima", 3), ("Villavicencio", "Meta", 2),
]
WAREHOUSES = ["BOG-01", "MED-01", "CLO-01", "BAQ-01"]

_NOUNS = {
    1: ["Audífonos", "Celular", "Smartwatch", "Teclado", "Monitor", "Cargador", "Tablet", "Parlante"],
    2: ["Licuadora", "Lámpara", "Sartén", "Cojín", "Aspiradora", "Organizador", "Cafetera", "Toallas"],
    3: ["Camiseta", "Jean", "Chaqueta", "Tenis", "Vestido", "Gorra", "Bolso", "Buso"],
    4: ["Crema facial", "Perfume", "Labial", "Shampoo", "Protector solar", "Serum", "Esmalte", "Mascarilla"],
    5: ["Balón", "Mancuernas", "Bicicleta", "Tapete de yoga", "Guantes", "Casco", "Termo", "Cuerda"],
}
_ADJ = ["Pro", "Plus", "Lite", "Max", "Eco", "Sport", "Classic", "Smart", "Ultra", "Mini"]
_PRICE_MEDIAN = {1: 280_000, 2: 90_000, 3: 120_000, 4: 60_000, 5: 110_000}

PRODUCT_FIELDS = ["product_id", "name", "category_id", "category", "seller_id", "price", "attributes", "updated_at"]
CUSTOMER_FIELDS = ["customer_id", "city", "department", "created_at"]
SELLER_FIELDS = ["seller_id", "name", "city", "department"]
INVENTORY_FIELDS = ["product_id", "warehouse_id", "stock"]

# Productos fijos para las pruebas de RF (ver scenarios.py). Se incluyen siempre salvo fixtures=False.
FIXTURE_PRODUCTS = {
    "TEST-RF07": dict(category_id=1, price=50_000, stock=320),
    "TEST-RF09-A": dict(category_id=1, price=100_000, stock=100_000),
    "TEST-RF09-B": dict(category_id=3, price=50_000, stock=100_000),
    "TEST-RF13-A": dict(category_id=2, price=40_000, stock=100_000),
    "TEST-RF13-B": dict(category_id=2, price=35_000, stock=100_000),
}
FIXTURE_WAREHOUSE = WAREHOUSES[0]
FIXTURE_CUSTOMERS = {  # clientes de prueba: ciudad conocida para verificar el enriquecimiento (RF-06)
    "U-TEST-A": ("Bogotá", "Cundinamarca"), "U-TEST-B": ("Medellín", "Antioquia"),
    "U-TEST-C": ("Cali", "Valle del Cauca"), "U-TEST-NOISE": ("Bogotá", "Cundinamarca"),
}


@dataclass
class Masters:
    products: dict[str, dict] = field(default_factory=dict)
    customers: dict[str, dict] = field(default_factory=dict)
    sellers: dict[str, dict] = field(default_factory=dict)
    inventory: dict[tuple[str, str], int] = field(default_factory=dict)

    def counts(self) -> dict[str, int]:
        return {"products": len(self.products), "customers": len(self.customers),
                "sellers": len(self.sellers), "inventory": len(self.inventory)}

    def stock_total(self, product_id: str) -> int:
        return sum(s for (p, _), s in self.inventory.items() if p == product_id)


def _pick_city(rng: random.Random) -> tuple[str, str]:
    c = rng.choices(CITIES, weights=[w for *_, w in CITIES])[0]
    return c[0], c[1]


def _attributes(rng: random.Random, category_id: int) -> dict:
    """Atributos que cambian por categoría (variedad que motiva el esquema flexible)."""
    if category_id == 1:
        return {"garantia_meses": rng.choice([6, 12, 24]), "ram_gb": rng.choice([None, 4, 8, 16]),
                "marca": rng.choice(["Aurum", "Nexo", "Vento"])}
    if category_id == 3:
        return {"talla": rng.choice(["S", "M", "L", "XL"]), "color": rng.choice(["negro", "azul", "rojo", "blanco"])}
    if category_id == 4:
        return {"volumen_ml": rng.choice([30, 50, 100, 250]), "vegano": rng.random() < 0.3}
    if category_id == 5:
        return {"peso_kg": round(rng.uniform(0.2, 12), 1)}
    return {"material": rng.choice(["acero", "plástico", "algodón", "madera"])}


def _make_product(rng: random.Random, n: int, seller_ids: list[str], day: str) -> dict:
    cat = rng.choice(list(CATEGORIES))
    price = round(rng.lognormvariate(0, 0.6) * _PRICE_MEDIAN[cat] / 100) * 100
    return {
        "product_id": f"P{n:06d}",
        "name": f"{rng.choice(_NOUNS[cat])} {rng.choice(_ADJ)} {n % 1000}",
        "category_id": cat, "category": CATEGORIES[cat],
        "seller_id": rng.choice(seller_ids), "price": max(price, 5_000),
        "attributes": json.dumps(_attributes(rng, cat), ensure_ascii=False),
        "updated_at": day,
    }


def build_masters(seed: int = 42, day_index: int = 0, n_products: int = 2000, n_customers: int = 20000,
                  n_sellers: int = 150, fixtures: bool = True) -> Masters:
    rng = random.Random(f"{seed}-base")
    base_day = "2026-01-01"
    m = Masters()
    for i in range(1, n_sellers + 1):
        city, dep = _pick_city(rng)
        m.sellers[f"S{i:04d}"] = {"seller_id": f"S{i:04d}", "name": f"Vendedor {i:04d}", "city": city, "department": dep}
    seller_ids = list(m.sellers)
    for i in range(1, n_products + 1):
        p = _make_product(rng, i, seller_ids, base_day)
        m.products[p["product_id"]] = p
        for wh in rng.sample(WAREHOUSES, k=rng.randint(1, 3)):
            m.inventory[(p["product_id"], wh)] = rng.randint(20, 400)
    for i in range(1, n_customers + 1):
        city, dep = _pick_city(rng)
        cid = "U" + hashlib.sha256(f"{seed}-{i}".encode()).hexdigest()[:10]
        m.customers[cid] = {"customer_id": cid, "city": city, "department": dep, "created_at": base_day}

    for step in range(1, day_index + 1):  # deriva diaria
        drng = random.Random(f"{seed}-day-{step}")
        day = date.fromordinal(date(2026, 1, 1).toordinal() + step).isoformat()
        for pid in drng.sample(list(m.products), k=max(1, len(m.products) // 50)):  # 2 % cambia de precio
            old = m.products[pid]["price"]
            m.products[pid]["price"] = max(5_000, round(old * drng.uniform(0.85, 1.15) / 100) * 100)
            m.products[pid]["updated_at"] = day
        for pid in drng.sample(list(m.products), k=max(1, len(m.products) // 200)):  # 0,5 % cambia de categoría (SCD2)
            cat = drng.choice([c for c in CATEGORIES if c != m.products[pid]["category_id"]])
            m.products[pid].update(category_id=cat, category=CATEGORIES[cat], updated_at=day,
                                   attributes=json.dumps(_attributes(drng, cat), ensure_ascii=False))
        next_p = len(m.products) + 1
        for k in range(max(1, n_products // 100)):  # 1 % de productos nuevos
            p = _make_product(drng, next_p + k, seller_ids, day)
            m.products[p["product_id"]] = p
            m.inventory[(p["product_id"], drng.choice(WAREHOUSES))] = drng.randint(20, 400)
        for k in range(max(1, n_customers // 200)):  # 0,5 % de clientes nuevos
            city, dep = _pick_city(drng)
            cid = "U" + hashlib.sha256(f"{seed}-d{step}-{k}".encode()).hexdigest()[:10]
            m.customers[cid] = {"customer_id": cid, "city": city, "department": dep, "created_at": day}
        for key in drng.sample(list(m.inventory), k=max(1, len(m.inventory) // 20)):  # reposición
            m.inventory[key] += drng.randint(10, 100)

    if fixtures:
        for pid, f in FIXTURE_PRODUCTS.items():
            m.products[pid] = {"product_id": pid, "name": f"Producto de prueba {pid}", "category_id": f["category_id"],
                               "category": CATEGORIES[f["category_id"]], "seller_id": seller_ids[0], "price": f["price"],
                               "attributes": "{}", "updated_at": base_day}
            m.inventory[(pid, FIXTURE_WAREHOUSE)] = f["stock"]
        for cid, (city, dep) in FIXTURE_CUSTOMERS.items():
            m.customers[cid] = {"customer_id": cid, "city": city, "department": dep, "created_at": base_day}
    return m


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def write_masters(m: Masters, out_dir: Path, day: str, products_format: str = "csv") -> dict:
    """Escribe snapshots versionados `<entidad>/dt=<día>/<entidad>.<ext>` y un manifiesto.

    El manifiesto se escribe al final y es la señal de carga completa; su conteo es la referencia de RF-04.
    """
    out_dir = Path(out_dir)
    files: dict[str, Path] = {}
    specs = [("products", PRODUCT_FIELDS, list(m.products.values())),
             ("customers", CUSTOMER_FIELDS, list(m.customers.values())),
             ("sellers", SELLER_FIELDS, list(m.sellers.values())),
             ("inventory", INVENTORY_FIELDS,
              [{"product_id": p, "warehouse_id": w, "stock": s} for (p, w), s in m.inventory.items()])]
    for name, fields, rows in specs:
        d = out_dir / name / f"dt={day}"
        d.mkdir(parents=True, exist_ok=True)
        if name == "products" and products_format == "json":
            path = d / "products.json"
            path.write_text(json.dumps([{**r, "attributes": json.loads(r["attributes"])} for r in rows],
                                       ensure_ascii=False), encoding="utf-8")
        else:
            path = d / f"{name}.csv"
            _write_csv(path, fields, rows)
        files[name] = path
    manifest = {"day": day, "counts": m.counts(),
                "files": {k: v.relative_to(out_dir).as_posix() for k, v in files.items()}}
    (out_dir / f"_manifest_dt={day}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def upload_to_s3(out_dir: Path, bucket: str, prefix: str = "landing/masters", client=None) -> int:
    """Sube todo `out_dir` a s3://bucket/prefix/. El manifiesto va al final (marca de carga completa)."""
    out_dir = Path(out_dir)
    if client is None:
        import boto3  # diferido: no es necesario para generar archivos
        client = boto3.client("s3")
    files = sorted((p for p in out_dir.rglob("*") if p.is_file()), key=lambda p: (p.name.startswith("_manifest"), str(p)))
    for p in files:
        client.upload_file(str(p), bucket, f"{prefix}/{p.relative_to(out_dir).as_posix()}")
    return len(files)
