from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import iris
import os
import threading
import time
import queue
import re
from typing import Optional

app = FastAPI()

DB_HOST      = os.environ.get("DB_HOST",      "172.19.4.72")
DB_PORT      = int(os.environ.get("DB_PORT",  "1972"))
DB_NAMESPACE = os.environ.get("DB_NAMESPACE", "SERGIPE")
DB_USER      = os.environ.get("DB_USER",      "bi-team")
DB_PASS      = os.environ.get("DB_PASS",      "fG&E2p[#Qb_dm")
S            = os.environ.get("DB_SCHEMA",    "SQLUser")
CLIENT_ID    = 202   # GOVSE – Governo do Estado de Sergipe (produção)

# ── CONNECTION POOL ───────────────────────────────────
class _Pool:
    def __init__(self, minconn=2, maxconn=8):
        self._dsn  = f"{DB_HOST}:{DB_PORT}/{DB_NAMESPACE}"
        self._user = DB_USER
        self._pass = DB_PASS
        self._q    = queue.Queue(maxsize=maxconn)
        for _ in range(minconn):
            self._q.put(self._new())

    def _new(self):
        return iris.connect(self._dsn, self._user, self._pass)

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
EIXOS_PH  = ",".join(["?"] * len(EIXOS_IDS))

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

def strip_html(text):
    """Remove HTML tags from text."""
    if not text:
        return text
    return re.sub(r'<[^>]+>', '', str(text)).strip()

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

        # Unidades: acronym das agencies com acaoprioritaria ativa nos eixos
        unidades = rows(cur, f"""
            SELECT DISTINCT ag.acronym
            FROM {S}.acaoprioritaria a
            LEFT JOIN {S}.tagtaggablerel ttr ON ttr.ownerUuid = a.uuid_
            LEFT JOIN {S}.tag t ON t.tagId = ttr.tagId AND t.active_ = 1
            LEFT JOIN {S}.agency ag ON ag.agencyId = a.agencyId
            WHERE a.clientId = ? AND a.systemManagedChild = 0
              AND a.ativo = 1 AND a.ano IN ('2025','2026')
              AND t.tagId IN ({EIXOS_PH})
              AND ag.acronym IS NOT NULL
            ORDER BY ag.acronym
        """, [CLIENT_ID] + EIXOS_IDS)

        # Eixos: tags 101–104
        eixos_db = rows(cur, f"""
            SELECT tagId, name
            FROM {S}.tag
            WHERE tagId IN ({EIXOS_PH})
            ORDER BY tagId
        """, EIXOS_IDS)

        # Trimestres: a partir de predictDate dos encaminhamentos
        trimestres = rows(cur, f"""
            SELECT DISTINCT
                {q_trim('a.predictDate')} AS trimestre,
                YEAR(a.predictDate) AS ano
            FROM {S}.assignment a
            WHERE a.predictDate IS NOT NULL
              AND a.assignmentTypeValue IN ('DEMAND','INCIDENT')
              AND a.clientId = ? AND a.deleted = 0 AND a.active_ = 1
              AND YEAR(a.predictDate) BETWEEN 2023 AND 2030
            ORDER BY ano, trimestre
        """, [CLIENT_ID])

        meses = rows(cur, f"""
            SELECT DISTINCT
                MONTH(a.predictDate) AS mes,
                YEAR(a.predictDate) AS ano
            FROM {S}.assignment a
            WHERE a.predictDate IS NOT NULL
              AND a.assignmentTypeValue IN ('DEMAND','INCIDENT')
              AND a.clientId = ? AND a.deleted = 0 AND a.active_ = 1
              AND YEAR(a.predictDate) BETWEEN 2023 AND 2030
            ORDER BY ano, mes
        """, [CLIENT_ID])

        result = {
            "unidades": [r["acronym"] for r in unidades],
            "eixos": [r["name"] for r in eixos_db] or EIXOS,
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

    ua_clause = "AND ag_a.acronym = ?" if unidade else ""
    up_clause = "AND ag_p.acronym = ?" if unidade else ""
    ut_clause = "AND ag_t.acronym = ?" if unidade else ""
    ue_clause = "AND ag_e.acronym = ?" if unidade else ""

    ua = ([unidade] if unidade else [])
    up = ([unidade] if unidade else [])
    ut = ([unidade] if unidade else [])
    ue = ([unidade] if unidade else [])

    conn = get_conn()
    try:
        cur = conn.cursor()

        # Projetos (acaoprioritaria N2) – donut
        proj_donut = rows(cur, f"""
            SELECT s.nome AS status, COUNT(DISTINCT a.uuid_) AS qtd
            FROM {S}.acaoprioritaria a
            LEFT JOIN {S}.tagtaggablerel ttr ON ttr.ownerUuid = a.uuid_
            LEFT JOIN {S}.tag t ON t.tagId = ttr.tagId AND t.active_ = 1
            LEFT JOIN {S}.agency ag_a ON ag_a.agencyId = a.agencyId
            LEFT JOIN {S}.status s ON s.codigoStatus = a.codigoStatus
            WHERE a.clientId = ? AND a.systemManagedChild = 0
              AND a.ativo = 1 AND a.ano IN ('2025','2026')
              AND t.tagId IN ({tagids_ph}) {ua_clause}
            GROUP BY s.nome ORDER BY qtd DESC
        """, [CLIENT_ID] + tagids + ua)

        # Metas (planooperativo N3) – donut — usa N2 como pai com filtros corretos
        metas_donut = rows(cur, f"""
            SELECT s.nome AS status, COUNT(*) AS qtd
            FROM {S}.planooperativo p
            JOIN {S}.acaoprioritaria a ON a.uuid_ = p.fatherUuid
              AND a.clientId = ? AND a.systemManagedChild = 0
              AND a.ativo = 1 AND a.ano IN ('2025','2026')
            LEFT JOIN {S}.tagtaggablerel ttr ON ttr.ownerUuid = a.uuid_
            LEFT JOIN {S}.tag t ON t.tagId = ttr.tagId AND t.active_ = 1
            LEFT JOIN {S}.agency ag_p ON ag_p.agencyId = p.agencyId
            LEFT JOIN {S}.status s ON s.codigoStatus = p.codigoStatus
            WHERE p.ativo = 1
              AND t.tagId IN ({tagids_ph}) {up_clause}
            GROUP BY s.nome ORDER BY qtd DESC
        """, [CLIENT_ID] + tagids + up)

        # Ações (tarefa) – donut
        acoes_donut = rows(cur, f"""
            SELECT s.nome AS status, COUNT(*) AS qtd
            FROM {S}.tarefa t
            JOIN {S}.agency ag_t ON ag_t.agencyId = t.agencyId
            LEFT JOIN {S}.status s ON s.codigoStatus = t.codigoStatus
            WHERE t.ativo = 1 AND t.deleted = 0 {ut_clause}
            GROUP BY s.nome ORDER BY qtd DESC
        """, ut)

        # Pontos de Controle (DEMAND) – donut
        ctrl_donut = rows(cur, f"""
            SELECT s.nome AS status, COUNT(DISTINCT a.assignmentId) AS qtd
            FROM {S}.assignment a
            LEFT JOIN {S}.status s ON s.codigoStatus = a.statusId
            LEFT JOIN (
              SELECT assignmentId, MAX(historyId) AS maxH
              FROM {S}.assignment_history GROUP BY assignmentId
            ) lh ON lh.assignmentId = a.assignmentId
            LEFT JOIN {S}.assignment_agency_history aah ON aah.historyId = lh.maxH
            LEFT JOIN {S}.agency ag_e ON ag_e.agencyId = aah.agencyId
            WHERE a.assignmentTypeValue = 'DEMAND'
              AND a.clientId = ? AND a.deleted = 0 AND a.active_ = 1 {ue_clause}
            GROUP BY s.nome ORDER BY qtd DESC
        """, [CLIENT_ID] + ue)

        # Pontos de Balanço (INCIDENT) – donut
        balanco_donut = rows(cur, f"""
            SELECT s.nome AS status, COUNT(DISTINCT a.assignmentId) AS qtd
            FROM {S}.assignment a
            LEFT JOIN {S}.status s ON s.codigoStatus = a.statusId
            LEFT JOIN (
              SELECT assignmentId, MAX(historyId) AS maxH
              FROM {S}.assignment_history GROUP BY assignmentId
            ) lh ON lh.assignmentId = a.assignmentId
            LEFT JOIN {S}.assignment_agency_history aah ON aah.historyId = lh.maxH
            LEFT JOIN {S}.agency ag_e ON ag_e.agencyId = aah.agencyId
            WHERE a.assignmentTypeValue = 'INCIDENT'
              AND a.clientId = ? AND a.deleted = 0 AND a.active_ = 1 {ue_clause}
            GROUP BY s.nome ORDER BY qtd DESC
        """, [CLIENT_ID] + ue)

        # Tabela de projetos (N2 – campos completos)
        tabela = rows(cur, f"""
            SELECT
                a.codigoacaoprioritaria,
                a.nome              AS entidade,
                a.ano,
                a.descricao,
                a.inicioprevisto,
                a.iniciorealizado,
                a.terminoprevisto,
                a.terminorealizado,
                s.codigoStatus      AS codigo_status,
                s.nome              AS status,
                ag_a.agencyId,
                ag_a.name           AS agency,
                ag_a.acronym,
                a.modifiedDate,
                a.createDate,
                a.uuid_,
                a.originUuid,
                a.percentualConclusao,
                a.responsavelId,
                r.nome              AS responsavel,
                a.fatherUuid,
                t.tagId,
                t.name              AS tag,
                a.ativo
            FROM {S}.acaoprioritaria a
            LEFT JOIN {S}.tagtaggablerel ttr ON ttr.ownerUuid = a.uuid_
            LEFT JOIN {S}.tag t ON t.tagId = ttr.tagId AND t.active_ = 1
            LEFT JOIN {S}.agency ag_a ON ag_a.agencyId = a.agencyId
            LEFT JOIN {S}.responsavel r ON r.responsavelId = a.responsavelId
            LEFT JOIN {S}.status s ON s.codigoStatus = a.codigoStatus
            WHERE a.clientId = ? AND a.systemManagedChild = 0
              AND a.ativo = 1 AND a.ano IN ('2025','2026')
              AND t.tagId IN ({tagids_ph}) {ua_clause}
            ORDER BY a.nome
        """, [CLIENT_ID] + tagids + ua)

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

        # N2 base conditions (pai dos planos)
        n2_join = f"""
            JOIN {S}.acaoprioritaria a ON a.uuid_ = p.fatherUuid
              AND a.clientId = ? AND a.systemManagedChild = 0
              AND a.ativo = 1 AND a.ano IN ('2025','2026')
            LEFT JOIN {S}.tagtaggablerel ttr ON ttr.ownerUuid = a.uuid_
            LEFT JOIN {S}.tag t ON t.tagId = ttr.tagId AND t.active_ = 1
        """

        w      = ["p.ativo = 1", f"t.tagId IN ({EIXOS_PH})"]
        params: list = [CLIENT_ID] + list(EIXOS_IDS)

        if unidade:
            w.append("ag.acronym = ?")
            params.append(unidade)
        if search:
            w.append("UPPER(p.nome) LIKE UPPER(?)")
            params.append(f"%{search}%")
        where = "WHERE " + " AND ".join(w)

        donut = rows(cur, f"""
            SELECT s.nome AS status, COUNT(*) AS qtd
            FROM {S}.planooperativo p
            {n2_join}
            LEFT JOIN {S}.agency ag ON ag.agencyId = p.agencyId
            LEFT JOIN {S}.status s ON s.codigoStatus = p.codigoStatus
            {where}
            GROUP BY s.nome ORDER BY qtd DESC
        """, params)

        tabela = rows(cur, f"""
            SELECT
                p.nome            AS meta,
                r.nome            AS responsavel,
                s.nome            AS status,
                ag.acronym        AS unidade,
                p.terminoprevisto AS termino_previsto
            FROM {S}.planooperativo p
            {n2_join}
            LEFT JOIN {S}.agency ag ON ag.agencyId = p.agencyId
            LEFT JOIN {S}.responsavel r ON r.responsavelId = p.responsavelId
            LEFT JOIN {S}.status s ON s.codigoStatus = p.codigoStatus
            {where}
            ORDER BY p.nome
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

        w      = ["t.ativo = 1", "t.deleted = 0"]
        params: list = []
        if unidade:
            w.append("ag.acronym = ?")
            params.append(unidade)
        if search:
            w.append("UPPER(t.nome) LIKE UPPER(?)")
            params.append(f"%{search}%")
        where = "WHERE " + " AND ".join(w)

        donut = rows(cur, f"""
            SELECT s.nome AS status, COUNT(*) AS qtd
            FROM {S}.tarefa t
            JOIN {S}.agency ag ON ag.agencyId = t.agencyId
            LEFT JOIN {S}.status s ON s.codigoStatus = t.codigoStatus
            {where}
            GROUP BY s.nome ORDER BY qtd DESC
        """, params)

        tabela = rows(cur, f"""
            SELECT TOP 2000
                t.nome            AS acao,
                r.nome            AS responsavel,
                t.terminoprevisto AS termino_previsto,
                s.nome            AS status,
                ag.acronym        AS unidade
            FROM {S}.tarefa t
            JOIN {S}.agency ag ON ag.agencyId = t.agencyId
            LEFT JOIN {S}.responsavel r ON r.responsavelId = t.responsavelId
            LEFT JOIN {S}.status s ON s.codigoStatus = t.codigoStatus
            {where}
            ORDER BY t.nome
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

        w      = ["a.assignmentTypeValue = ?", "a.clientId = ?",
                  "a.deleted = 0", "a.active_ = 1"]
        params: list = [tipo, CLIENT_ID]

        if unidade:
            w.append("ag_e.acronym = ?")
            params.append(unidade)
        if trimestre:
            w.append(f"{q_trim('a.predictDate')} = ?")
            params.append(trimestre)
        if mes:
            w.append("MONTH(a.predictDate) = ?")
            params.append(mes)
        if search:
            w.append("UPPER(a.text_) LIKE UPPER(?)")
            params.append(f"%{search}%")
        where = "WHERE " + " AND ".join(w)

        # Sub-select para agency via latest history (evita linhas duplicadas)
        agency_join = f"""
            LEFT JOIN (
              SELECT assignmentId, MAX(historyId) AS maxH
              FROM {S}.assignment_history GROUP BY assignmentId
            ) lh ON lh.assignmentId = a.assignmentId
            LEFT JOIN {S}.assignment_agency_history aah ON aah.historyId = lh.maxH
            LEFT JOIN {S}.agency ag_e ON ag_e.agencyId = aah.agencyId
        """

        donut = rows(cur, f"""
            SELECT s.nome AS status, COUNT(DISTINCT a.assignmentId) AS qtd
            FROM {S}.assignment a
            LEFT JOIN {S}.status s ON s.codigoStatus = a.statusId
            {agency_join}
            {where}
            GROUP BY s.nome ORDER BY qtd DESC
        """, params)

        tabela = rows(cur, f"""
            SELECT TOP 3000
                ag_e.acronym                AS sigla,
                a.text_                     AS encaminhamento,
                r.nome                      AS responsavel,
                ahx.description             AS ultimo_comentario,
                CAST(a.predictDate AS DATE) AS termino_previsto,
                s.nome                      AS status,
                aeh.name                    AS entidades,
                a.uuid_                     AS url
            FROM {S}.assignment a
            LEFT JOIN {S}.status s ON s.codigoStatus = a.statusId
            LEFT JOIN {S}.assignment_engager_rel er ON er.assignmentId = a.assignmentId
              AND er.type_ = 'RESPONSAVEL' AND er.main = 1
            LEFT JOIN {S}.responsavel r ON r.uuid_ = er.referenceUuid
            {agency_join}
            LEFT JOIN {S}.assignment_history ahx ON ahx.historyId = lh.maxH
            LEFT JOIN {S}.assignment_entity_history aeh ON aeh.historyId = lh.maxH
            {where}
            ORDER BY ag_e.acronym, a.predictDate
        """, params)

        # Remover HTML do campo encaminhamento
        for row in tabela:
            row["encaminhamento"] = strip_html(row.get("encaminhamento") or "")

        result = {"donut": donut, "total": sum(r["qtd"] for r in donut), "tabela": tabela}
        cache_set(key, result)
        return result
    finally:
        release(conn)


app.mount("/static", StaticFiles(directory="static"), name="static")
