from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import psycopg2
import psycopg2.extras
import psycopg2.pool
import os
import threading
import time
from typing import Optional

app = FastAPI()

DB_URL = os.environ["DB_URL"]
S = os.environ.get("DB_SCHEMA", "public")

_pool = psycopg2.pool.ThreadedConnectionPool(2, 10, DB_URL)
_cache: dict = {}
_cache_lock = threading.Lock()
CACHE_TTL = 300

# IDs dos 4 eixos estratégicos em tbl_acaoprioritaria.tagid
EIXOS_IDS = [101, 102, 103, 104]

# Mapeamento nome → tagid (para filtro por eixo selecionado)
EIXO_TO_TAGID = {
    "Eixo 1 - Trabalho e Desenvolvimento Econômico": 101,
    "Eixo 2 - Desenvolvimento Humano e Social":       102,
    "Eixo 3 - Infraestrutura e Sustentabilidade":     103,
    "Eixo 4 - Gestão, Governança e Inovação":         104,
}

# Nomes para exibição (ordem fixa)
EIXOS = [
    "Eixo 1 - Trabalho e Desenvolvimento Econômico",
    "Eixo 2 - Desenvolvimento Humano e Social",
    "Eixo 3 - Infraestrutura e Sustentabilidade",
    "Eixo 4 - Gestão, Governança e Inovação",
]


def get_conn():
    return _pool.getconn()


def release(c):
    _pool.putconn(c)


def rows(cur, sql, params=()):
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry["ts"] < CACHE_TTL:
            return entry["data"]
        return None


def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time()}


@app.get("/")
def index():
    return FileResponse("frontend/index.html")


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/filtros")
def filtros():
    key = "filtros"
    cached = cache_get(key)
    if cached:
        return cached

    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Unidades presentes nos projetos estratégicos ativos
        unidades = rows(cur, f"""
            SELECT DISTINCT p.acronym
            FROM {S}.tbl_planooperativo p
            JOIN {S}.tbl_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            WHERE p.ativo = true
              AND a.tagid = ANY(%s)
              AND a.ativo = 'true'
              AND p.acronym IS NOT NULL
            ORDER BY p.acronym
        """, (EIXOS_IDS,))

        eixos_db = rows(cur, f"""
            SELECT DISTINCT tagid, tag
            FROM {S}.tbl_acaoprioritaria
            WHERE tagid = ANY(%s) AND ativo = 'true'
            ORDER BY tagid
        """, (EIXOS_IDS,))

        trimestres = rows(cur, f"""
            SELECT DISTINCT
                EXTRACT(QUARTER FROM termino_previsto)::int AS trimestre,
                EXTRACT(YEAR  FROM termino_previsto)::int AS ano
            FROM {S}.tbl_encaminhamentos
            WHERE termino_previsto IS NOT NULL
              AND EXTRACT(YEAR FROM termino_previsto) BETWEEN 2023 AND 2030
            ORDER BY ano, trimestre
        """)

        meses = rows(cur, f"""
            SELECT DISTINCT
                EXTRACT(MONTH FROM termino_previsto)::int AS mes,
                EXTRACT(YEAR  FROM termino_previsto)::int AS ano,
                TO_CHAR(DATE_TRUNC('month', termino_previsto), 'Mon/YYYY') AS label
            FROM {S}.tbl_encaminhamentos
            WHERE termino_previsto IS NOT NULL
              AND EXTRACT(YEAR FROM termino_previsto) BETWEEN 2023 AND 2030
            ORDER BY ano, mes
        """)

        result = {
            "unidades": [r["acronym"] for r in unidades],
            "eixos": [r["tag"] for r in eixos_db] or EIXOS,  # nomes para o frontend
            "trimestres": trimestres,
            "meses": meses,
        }
        cache_set(key, result)
        return result
    finally:
        release(conn)


@app.get("/api/painel-geral")
def painel_geral(
    unidade: Optional[str] = Query(None),
    eixo: Optional[str] = Query(None),
):
    key = f"painel_geral|{unidade}|{eixo}"
    cached = cache_get(key)
    if cached:
        return cached

    # Resolve tagids — filtro por eixo específico ou todos os 4 eixos
    tagids = [EIXO_TO_TAGID[eixo]] if eixo and eixo in EIXO_TO_TAGID else EIXOS_IDS
    ua = ("AND a.acronym = %s", [unidade]) if unidade else ("", [])
    up = ("AND p.acronym = %s", [unidade]) if unidade else ("", [])
    ut = ("AND t.acronym = %s", [unidade]) if unidade else ("", [])
    ue = ("AND e.sigla  = %s", [unidade]) if unidade else ("", [])

    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Projetos
        proj_donut = rows(cur, f"""
            SELECT a.status, COUNT(DISTINCT a.uuid_) AS qtd
            FROM {S}.tbl_acaoprioritaria a
            JOIN {S}.tbl_planooperativo p ON p.fatheruuid = a.uuid_ AND p.ativo = true
            WHERE a.tagid = ANY(%s) AND a.ativo = 'true' {ua[0]}
            GROUP BY a.status ORDER BY qtd DESC
        """, [tagids] + ua[1])

        # Metas
        metas_donut = rows(cur, f"""
            SELECT p.status, COUNT(*) AS qtd
            FROM {S}.tbl_planooperativo p
            JOIN {S}.tbl_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            WHERE p.ativo = true AND a.tagid = ANY(%s) AND a.ativo = 'true' {up[0]}
            GROUP BY p.status ORDER BY qtd DESC
        """, [tagids] + up[1])

        # Ações — tbl_tarefa standalone (fatheruuid não cruza com outras tabelas)
        acoes_donut = rows(cur, f"""
            SELECT t.status, COUNT(*) AS qtd
            FROM {S}.tbl_tarefa t
            WHERE t.ativo = true {ut[0]}
            GROUP BY t.status ORDER BY qtd DESC
        """, ut[1])

        # Pontos de Controle
        ctrl_donut = rows(cur, f"""
            SELECT e.status, COUNT(*) AS qtd
            FROM {S}.tbl_encaminhamentos e
            WHERE e.tipo_encaminhamento = 'DEMAND' {ue[0]}
            GROUP BY e.status ORDER BY qtd DESC
        """, ue[1])

        # Pontos de Balanço
        balanco_donut = rows(cur, f"""
            SELECT e.status, COUNT(*) AS qtd
            FROM {S}.tbl_encaminhamentos e
            WHERE e.tipo_encaminhamento = 'INCIDENT' {ue[0]}
            GROUP BY e.status ORDER BY qtd DESC
        """, ue[1])

        # Tabela Projetos
        tabela = rows(cur, f"""
            SELECT DISTINCT ON (a.uuid_)
                a.entidade AS projeto,
                a.responsavel,
                a.status,
                a.acronym AS unidade,
                a.tag AS eixo
            FROM {S}.tbl_acaoprioritaria a
            JOIN {S}.tbl_planooperativo p ON p.fatheruuid = a.uuid_ AND p.ativo = true
            WHERE a.tagid = ANY(%s) AND a.ativo = 'true' {ua[0]}
            ORDER BY a.uuid_, a.entidade
        """, [tagids] + ua[1])

        def total(d): return sum(r["qtd"] for r in d)

        result = {
            "projetos_donut": proj_donut,
            "metas_donut": metas_donut,
            "acoes_donut": acoes_donut,
            "ctrl_donut": ctrl_donut,
            "balanco_donut": balanco_donut,
            "totais": {
                "projetos": total(proj_donut),
                "metas": total(metas_donut),
                "acoes": total(acoes_donut),
                "ctrl": total(ctrl_donut),
                "balanco": total(balanco_donut),
            },
            "tabela": tabela,
        }
        cache_set(key, result)
        return result
    finally:
        release(conn)


@app.get("/api/metas")
def metas(
    unidade: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    key = f"metas|{unidade}|{search}"
    cached = cache_get(key)
    if cached:
        return cached

    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        w = ["p.ativo = true", "a.tagid = ANY(%s)", "a.ativo = 'true'"]
        params: list = [EIXOS_IDS]
        if unidade:
            w.append("p.acronym = %s")
            params.append(unidade)
        if search:
            w.append("p.entidade ILIKE %s")
            params.append(f"%{search}%")
        where = "WHERE " + " AND ".join(w)

        donut = rows(cur, f"""
            SELECT p.status, COUNT(*) AS qtd
            FROM {S}.tbl_planooperativo p
            JOIN {S}.tbl_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            {where}
            GROUP BY p.status ORDER BY qtd DESC
        """, params)

        tabela = rows(cur, f"""
            SELECT
                p.entidade AS meta,
                p.responsavel,
                p.status,
                p.acronym AS unidade,
                p.terminoprevisto AS termino_previsto
            FROM {S}.tbl_planooperativo p
            JOIN {S}.tbl_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            {where}
            ORDER BY p.entidade
        """, params)

        result = {"donut": donut, "total": sum(r["qtd"] for r in donut), "tabela": tabela}
        cache_set(key, result)
        return result
    finally:
        release(conn)


@app.get("/api/acoes")
def acoes(
    unidade: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    key = f"acoes|{unidade}|{search}"
    cached = cache_get(key)
    if cached:
        return cached

    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # tbl_tarefa não possui join funcional com acaoprioritaria
        # filtro por acronym (unidade) e ativo=true
        w = ["t.ativo = true"]
        params: list = []
        if unidade:
            w.append("t.acronym = %s")
            params.append(unidade)
        if search:
            w.append("t.entidade ILIKE %s")
            params.append(f"%{search}%")
        where = "WHERE " + " AND ".join(w)

        donut = rows(cur, f"""
            SELECT t.status, COUNT(*) AS qtd
            FROM {S}.tbl_tarefa t
            {where}
            GROUP BY t.status ORDER BY qtd DESC
        """, params)

        tabela = rows(cur, f"""
            SELECT
                t.entidade AS acao,
                t.responsavel,
                t.terminoprevisto AS termino_previsto,
                t.status,
                t.acronym AS unidade
            FROM {S}.tbl_tarefa t
            {where}
            ORDER BY t.entidade
            LIMIT 2000
        """, params)

        result = {"donut": donut, "total": sum(r["qtd"] for r in donut), "tabela": tabela}
        cache_set(key, result)
        return result
    finally:
        release(conn)


@app.get("/api/pontos")
def pontos(
    tipo: str = Query("DEMAND"),
    unidade: Optional[str] = Query(None),
    trimestre: Optional[int] = Query(None),
    mes: Optional[int] = Query(None),
    search: Optional[str] = Query(None),
):
    key = f"pontos|{tipo}|{unidade}|{trimestre}|{mes}|{search}"
    cached = cache_get(key)
    if cached:
        return cached

    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        w = ["e.tipo_encaminhamento = %s"]
        params: list = [tipo]
        if unidade:
            w.append("e.sigla = %s")
            params.append(unidade)
        if trimestre:
            w.append("EXTRACT(QUARTER FROM e.termino_previsto)::int = %s")
            params.append(trimestre)
        if mes:
            w.append("EXTRACT(MONTH FROM e.termino_previsto)::int = %s")
            params.append(mes)
        if search:
            w.append("e.encaminhamento ILIKE %s")
            params.append(f"%{search}%")
        where = "WHERE " + " AND ".join(w)

        donut = rows(cur, f"""
            SELECT e.status, COUNT(*) AS qtd
            FROM {S}.tbl_encaminhamentos e
            {where}
            GROUP BY e.status ORDER BY qtd DESC
        """, params)

        tabela = rows(cur, f"""
            SELECT
                e.sigla AS unidade,
                e.encaminhamento,
                e.responsavel,
                e.ultimo_comentario,
                e.termino_previsto,
                e.status,
                e.entidades,
                e.url
            FROM {S}.tbl_encaminhamentos e
            {where}
            ORDER BY e.sigla, e.termino_previsto NULLS LAST
            LIMIT 3000
        """, params)

        result = {"donut": donut, "total": sum(r["qtd"] for r in donut), "tabela": tabela}
        cache_set(key, result)
        return result
    finally:
        release(conn)


app.mount("/static", StaticFiles(directory="static"), name="static")
