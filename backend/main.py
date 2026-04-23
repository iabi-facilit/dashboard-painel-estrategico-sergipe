from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import intersystems_iris.dbapi as iris_db
import os
import threading
import time
import queue
from typing import Optional

app = FastAPI()

DB_HOST      = os.environ.get("DB_HOST",      "172.19.4.72")
DB_PORT      = int(os.environ.get("DB_PORT",  "1972"))
DB_NAMESPACE = os.environ.get("DB_NAMESPACE", "SERGIPE")
DB_USER      = os.environ.get("DB_USER",      "bi-team")
DB_PASS      = os.environ.get("DB_PASS",      "fG&E2p[#Qb_dm")
S            = os.environ.get("DB_SCHEMA",    "SQLUser")

# ── CONNECTION POOL ───────────────────────────────────
class _Pool:
    def __init__(self, minconn=2, maxconn=8):
        self._kwargs = dict(
            hostname=DB_HOST, port=DB_PORT,
            namespace=DB_NAMESPACE,
            username=DB_USER, password=DB_PASS,
        )
        self._q = queue.Queue(maxsize=maxconn)
        for _ in range(minconn):
            self._q.put(self._new())

    def _new(self):
        return iris_db.connect(**self._kwargs)

    def getconn(self):
        try:
            return self._q.get_nowait()
        except queue.Empty:
            return self._new()

    def putconn(self, c):
        try:
            self._q.put_nowait(c)
        except queue.Full:
            try: c.close()
            except: pass

_pool = _Pool()
_cache: dict = {}
_cache_lock = threading.Lock()
CACHE_TTL = 300

# ── EIXOS ─────────────────────────────────────────────
EIXOS_IDS = [101, 102, 103, 104]
EIXOS_PH  = ",".join(["?"] * len(EIXOS_IDS))   # "?,?,?,?"

EIXO_TO_TAGID = {
    "Eixo 1 - Trabalho e Desenvolvimento Econômico": 101,
    "Eixo 2 - Desenvolvimento Humano e Social":       102,
    "Eixo 3 - Infraestrutura e Sustentabilidade":     103,
    "Eixo 4 - Gestão, Governança e Inovação":         104,
}

EIXOS = list(EIXO_TO_TAGID.keys())

# ── HELPERS ───────────────────────────────────────────
def get_conn():
    return _pool.getconn()

def release(c):
    _pool.putconn(c)

def rows(cur, sql, params=()):
    cur.execute(sql, list(params))
    if not cur.description:
        return []
    cols = [d[0].lower() for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry["ts"] < CACHE_TTL:
            return entry["data"]
        return None

def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time()}

def q_trim(field):
    """Quarter expression compatible with IRIS SQL."""
    return (
        f"CASE WHEN MONTH({field}) IN (1,2,3) THEN 1 "
        f"WHEN MONTH({field}) IN (4,5,6) THEN 2 "
        f"WHEN MONTH({field}) IN (7,8,9) THEN 3 "
        f"ELSE 4 END"
    )

# ── ROUTES ────────────────────────────────────────────
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
        cur = conn.cursor()

        unidades = rows(cur, f"""
            SELECT DISTINCT p.acronym
            FROM {S}.tbl_planooperativo p
            JOIN {S}.tbl_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            WHERE p.ativo = 'true'
              AND a.tagid IN ({EIXOS_PH})
              AND a.ativo = 'true'
              AND p.acronym IS NOT NULL
            ORDER BY p.acronym
        """, EIXOS_IDS)

        eixos_db = rows(cur, f"""
            SELECT DISTINCT tagid, tag
            FROM {S}.tbl_acaoprioritaria
            WHERE tagid IN ({EIXOS_PH}) AND ativo = 'true'
            ORDER BY tagid
        """, EIXOS_IDS)

        trimestres = rows(cur, f"""
            SELECT DISTINCT
                {q_trim('termino_previsto')} AS trimestre,
                YEAR(termino_previsto) AS ano
            FROM {S}.tbl_encaminhamentos
            WHERE termino_previsto IS NOT NULL
              AND YEAR(termino_previsto) BETWEEN 2023 AND 2030
            ORDER BY ano, trimestre
        """)

        meses = rows(cur, f"""
            SELECT DISTINCT
                MONTH(termino_previsto) AS mes,
                YEAR(termino_previsto) AS ano
            FROM {S}.tbl_encaminhamentos
            WHERE termino_previsto IS NOT NULL
              AND YEAR(termino_previsto) BETWEEN 2023 AND 2030
            ORDER BY ano, mes
        """)

        result = {
            "unidades": [r["acronym"] for r in unidades],
            "eixos": [r["tag"] for r in eixos_db] or EIXOS,
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
    eixo:    Optional[str] = Query(None),
):
    key = f"painel_geral|{unidade}|{eixo}"
    cached = cache_get(key)
    if cached:
        return cached

    tagids    = [EIXO_TO_TAGID[eixo]] if eixo and eixo in EIXO_TO_TAGID else EIXOS_IDS
    tagids_ph = ",".join(["?"] * len(tagids))

    ua = ("AND a.acronym = ?", [unidade]) if unidade else ("", [])
    up = ("AND p.acronym = ?", [unidade]) if unidade else ("", [])
    ut = ("AND t.acronym = ?", [unidade]) if unidade else ("", [])
    ue = ("AND e.sigla   = ?", [unidade]) if unidade else ("", [])

    conn = get_conn()
    try:
        cur = conn.cursor()

        proj_donut = rows(cur, f"""
            SELECT a.status, COUNT(DISTINCT a.uuid_) AS qtd
            FROM {S}.tbl_acaoprioritaria a
            JOIN {S}.tbl_planooperativo p ON p.fatheruuid = a.uuid_ AND p.ativo = 'true'
            WHERE a.tagid IN ({tagids_ph}) AND a.ativo = 'true' {ua[0]}
            GROUP BY a.status ORDER BY qtd DESC
        """, tagids + ua[1])

        metas_donut = rows(cur, f"""
            SELECT p.status, COUNT(*) AS qtd
            FROM {S}.tbl_planooperativo p
            JOIN {S}.tbl_acaoprioritaria a ON a.uuid_ = p.fatheruuid
            WHERE p.ativo = 'true' AND a.tagid IN ({tagids_ph}) AND a.ativo = 'true' {up[0]}
            GROUP BY p.status ORDER BY qtd DESC
        """, tagids + up[1])

        acoes_donut = rows(cur, f"""
            SELECT t.status, COUNT(*) AS qtd
            FROM {S}.tbl_tarefa t
            WHERE t.ativo = 'true' {ut[0]}
            GROUP BY t.status ORDER BY qtd DESC
        """, ut[1])

        ctrl_donut = rows(cur, f"""
            SELECT e.status, COUNT(*) AS qtd
            FROM {S}.tbl_encaminhamentos e
            WHERE e.tipo_encaminhamento = 'DEMAND' {ue[0]}
            GROUP BY e.status ORDER BY qtd DESC
        """, ue[1])

        balanco_donut = rows(cur, f"""
            SELECT e.status, COUNT(*) AS qtd
            FROM {S}.tbl_encaminhamentos e
            WHERE e.tipo_encaminhamento = 'INCIDENT' {ue[0]}
            GROUP BY e.status ORDER BY qtd DESC
        """, ue[1])

        tabela = rows(cur, f"""
            SELECT DISTINCT
                a.entidade AS projeto,
                a.responsavel,
                a.status,
                a.acronym  AS unidade,
                a.tag      AS eixo
            FROM {S}.tbl_acaoprioritaria a
            JOIN {S}.tbl_planooperativo p ON p.fatheruuid = a.uuid_ AND p.ativo = 'true'
            WHERE a.tagid IN ({tagids_ph}) AND a.ativo = 'true' {ua[0]}
            ORDER BY a.entidade
        """, tagids + ua[1])

        def total(d): return sum(r["qtd"] for r in d)

        result = {
            "projetos_donut": proj_donut,
            "metas_donut":    metas_donut,
            "acoes_donut":    acoes_donut,
            "ctrl_donut":     ctrl_donut,
            "balanco_donut":  balanco_donut,
            "totais": {
                "projetos": total(proj_donut),
                "metas":    total(metas_donut),
                "acoes":    total(acoes_donut),
                "ctrl":     total(ctrl_donut),
                "balanco":  total(balanco_donut),
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
    search:  Optional[str] = Query(None),
):
    key = f"metas|{unidade}|{search}"
    cached = cache_get(key)
    if cached:
        return cached

    conn = get_conn()
    try:
        cur = conn.cursor()

        w = [f"p.ativo = 'true'", f"a.tagid IN ({EIXOS_PH})", "a.ativo = 'true'"]
        params: list = list(EIXOS_IDS)
        if unidade:
            w.append("p.acronym = ?")
            params.append(unidade)
        if search:
            w.append("UPPER(p.entidade) LIKE UPPER(?)")
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
                p.entidade       AS meta,
                p.responsavel,
                p.status,
                p.acronym        AS unidade,
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
    search:  Optional[str] = Query(None),
):
    key = f"acoes|{unidade}|{search}"
    cached = cache_get(key)
    if cached:
        return cached

    conn = get_conn()
    try:
        cur = conn.cursor()

        w = ["t.ativo = 'true'"]
        params: list = []
        if unidade:
            w.append("t.acronym = ?")
            params.append(unidade)
        if search:
            w.append("UPPER(t.entidade) LIKE UPPER(?)")
            params.append(f"%{search}%")
        where = "WHERE " + " AND ".join(w)

        donut = rows(cur, f"""
            SELECT t.status, COUNT(*) AS qtd
            FROM {S}.tbl_tarefa t
            {where}
            GROUP BY t.status ORDER BY qtd DESC
        """, params)

        tabela = rows(cur, f"""
            SELECT TOP 2000
                t.entidade        AS acao,
                t.responsavel,
                t.terminoprevisto AS termino_previsto,
                t.status,
                t.acronym         AS unidade
            FROM {S}.tbl_tarefa t
            {where}
            ORDER BY t.entidade
        """, params)

        result = {"donut": donut, "total": sum(r["qtd"] for r in donut), "tabela": tabela}
        cache_set(key, result)
        return result
    finally:
        release(conn)


@app.get("/api/pontos")
def pontos(
    tipo:      str           = Query("DEMAND"),
    unidade:   Optional[str] = Query(None),
    trimestre: Optional[int] = Query(None),
    mes:       Optional[int] = Query(None),
    search:    Optional[str] = Query(None),
):
    key = f"pontos|{tipo}|{unidade}|{trimestre}|{mes}|{search}"
    cached = cache_get(key)
    if cached:
        return cached

    conn = get_conn()
    try:
        cur = conn.cursor()

        w = ["e.tipo_encaminhamento = ?"]
        params: list = [tipo]
        if unidade:
            w.append("e.sigla = ?")
            params.append(unidade)
        if trimestre:
            w.append(f"{q_trim('e.termino_previsto')} = ?")
            params.append(trimestre)
        if mes:
            w.append("MONTH(e.termino_previsto) = ?")
            params.append(mes)
        if search:
            w.append("UPPER(e.encaminhamento) LIKE UPPER(?)")
            params.append(f"%{search}%")
        where = "WHERE " + " AND ".join(w)

        donut = rows(cur, f"""
            SELECT e.status, COUNT(*) AS qtd
            FROM {S}.tbl_encaminhamentos e
            {where}
            GROUP BY e.status ORDER BY qtd DESC
        """, params)

        tabela = rows(cur, f"""
            SELECT TOP 3000
                e.sigla           AS unidade,
                e.encaminhamento,
                e.responsavel,
                e.ultimo_comentario,
                e.termino_previsto,
                e.status,
                e.entidades,
                e.url
            FROM {S}.tbl_encaminhamentos e
            {where}
            ORDER BY e.sigla, e.termino_previsto
        """, params)

        result = {"donut": donut, "total": sum(r["qtd"] for r in donut), "tabela": tabela}
        cache_set(key, result)
        return result
    finally:
        release(conn)


app.mount("/static", StaticFiles(directory="static"), name="static")
