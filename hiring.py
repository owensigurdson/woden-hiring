import base64
from fastapi import FastAPI, Request, Response, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
import anthropic
import sqlite3
import json
import os
import io
import time
import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

class BasicAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in ("/favicon.ico",):
            return await call_next(request)
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                creds = base64.b64decode(auth[6:]).decode("utf-8")
                username, password = creds.split(":", 1)
                expected = os.getenv("APP_PASSWORD", "")
                if username == "woden" and password == expected and expected:
                    return await call_next(request)
            except Exception:
                pass
        return Response("Unauthorized", status_code=401,
                        headers={"WWW-Authenticate": 'Basic realm="Woden Hiring"'})

app.add_middleware(BasicAuthMiddleware)
client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
executor = ThreadPoolExecutor(max_workers=4)

DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(os.getenv("DATA_DIR", DIR), "hiring.db")

# ── Database ──────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            role TEXT,
            status TEXT DEFAULT 'Applied',
            resume_text TEXT,
            score INTEGER,
            score_reasoning TEXT,
            phone TEXT,
            email TEXT,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS subcontractors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            specialty TEXT,
            location TEXT,
            phone TEXT,
            email TEXT,
            website TEXT,
            source TEXT,
            notes TEXT,
            status TEXT DEFAULT 'Found',
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

# ── Hiring agent tools ────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "save_candidate",
        "description": "Save a new candidate after reviewing their resume or application.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "role": {"type": "string", "description": "Deck Builder, Fence Installer, Landscaper, or General Labour"},
                "resume_text": {"type": "string"},
                "score": {"type": "integer", "description": "1-10 fit score"},
                "score_reasoning": {"type": "string"},
                "phone": {"type": "string"},
                "email": {"type": "string"},
                "notes": {"type": "string"}
            },
            "required": ["name", "role", "score", "score_reasoning"]
        }
    },
    {
        "name": "update_candidate",
        "description": "Update a candidate's pipeline status or add notes",
        "input_schema": {
            "type": "object",
            "properties": {
                "candidate_id": {"type": "integer"},
                "status": {"type": "string", "enum": ["Applied", "Screened", "Interview Scheduled", "Interviewed", "Hired", "Rejected"]},
                "notes": {"type": "string"}
            },
            "required": ["candidate_id"]
        }
    },
    {
        "name": "get_candidates",
        "description": "Retrieve candidates, optionally filtered",
        "input_schema": {
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "role": {"type": "string"}
            }
        }
    }
]

# ── DB helpers — candidates ───────────────────────────────────────────────────

def db_save_candidate(name, role, score, score_reasoning, resume_text="", phone="", email="", notes=""):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.datetime.now().isoformat()
    c.execute("""
        INSERT INTO candidates (name, role, status, resume_text, score, score_reasoning, phone, email, notes, created_at, updated_at)
        VALUES (?, ?, 'Applied', ?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, role, resume_text, score, score_reasoning, phone, email, notes, now, now))
    conn.commit()
    cid = c.lastrowid
    conn.close()
    return cid

def db_update_candidate(candidate_id, status=None, notes=None):
    conn = sqlite3.connect(DB_PATH)
    now = datetime.datetime.now().isoformat()
    updates, params = ["updated_at=?"], [now]
    if status:
        updates.insert(0, "status=?")
        params.insert(0, status)
    if notes:
        updates.insert(0, "notes=?")
        params.insert(0, notes)
    params.append(candidate_id)
    conn.execute(f"UPDATE candidates SET {', '.join(updates)} WHERE id=?", params)
    conn.commit()
    conn.close()

def db_get_candidates(status=None, role=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    query = "SELECT id, name, role, status, score, score_reasoning, phone, email, notes, created_at FROM candidates"
    params, conds = [], []
    if status:
        conds.append("status=?"); params.append(status)
    if role:
        conds.append("role LIKE ?"); params.append(f"%{role}%")
    if conds:
        query += " WHERE " + " AND ".join(conds)
    query += " ORDER BY score DESC, created_at DESC"
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "role": r[2], "status": r[3], "score": r[4],
             "score_reasoning": r[5], "phone": r[6], "email": r[7], "notes": r[8], "created_at": r[9]}
            for r in rows]

# ── DB helpers — subcontractors ───────────────────────────────────────────────

def db_save_sub(name, specialty="", location="", phone="", email="", website="", source="", notes=""):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    now = datetime.datetime.now().isoformat()
    c.execute("""
        INSERT INTO subcontractors (name, specialty, location, phone, email, website, source, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, specialty, location, phone, email, website, source, notes, now))
    conn.commit()
    sid = c.lastrowid
    conn.close()
    return sid

def db_get_subs():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, name, specialty, location, phone, email, website, source, notes, status, created_at FROM subcontractors ORDER BY created_at DESC")
    rows = c.fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "specialty": r[2], "location": r[3], "phone": r[4],
             "email": r[5], "website": r[6], "source": r[7], "notes": r[8], "status": r[9], "created_at": r[10]}
            for r in rows]

def db_update_sub(sub_id, status=None, notes=None):
    conn = sqlite3.connect(DB_PATH)
    updates, params = [], []
    if status:
        updates.append("status=?"); params.append(status)
    if notes:
        updates.append("notes=?"); params.append(notes)
    if updates:
        params.append(sub_id)
        conn.execute(f"UPDATE subcontractors SET {', '.join(updates)} WHERE id=?", params)
        conn.commit()
    conn.close()

# ── Tool processor ────────────────────────────────────────────────────────────

def process_tool(name, inputs):
    if name == "save_candidate":
        cid = db_save_candidate(
            name=inputs.get("name", "Unknown"), role=inputs.get("role", "General Labour"),
            score=inputs.get("score", 5), score_reasoning=inputs.get("score_reasoning", ""),
            resume_text=inputs.get("resume_text", ""), phone=inputs.get("phone", ""),
            email=inputs.get("email", ""), notes=inputs.get("notes", "")
        )
        return {"success": True, "candidate_id": cid}
    elif name == "update_candidate":
        db_update_candidate(**inputs)
        return {"success": True}
    elif name == "get_candidates":
        return {"candidates": db_get_candidates(**inputs)}
    return {"error": "Unknown tool"}

# ── Hiring agent ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are the hiring manager for Woden Contracting, a residential contracting company in Calgary, Alberta, Canada. Owen (the owner) relies on you to find and evaluate quality in-house employees for field work.

Woden's services: decks (PT and composite), fences (wood privacy, vinyl, chain link, aluminum), landscaping (sod, topsoil, mulch).

Roles hired for:
- Deck Builder — framing, decking, railings, stairs
- Fence Installer — post setting, panel/board installation, gates
- Landscaper — sod laying, topsoil/mulch, general outdoor labour
- General Labour — support across all trades

Strong candidate signals (score higher):
- Relevant experience: outdoor construction, framing, labour, trades
- Steady employment with no large unexplained gaps
- Physical fitness signals — hard outdoor work in Alberta weather
- Verifiable past employers or references
- Clean driver's license (strong asset)
- Own tools (bonus)

Scoring: 8-10 strong, 5-7 screening call, 1-4 pass.

When a resume is pasted: extract info, score 1-10, call save_candidate, give direct recommendation.
Pipeline stages: Applied → Screened → Interview Scheduled → Interviewed → Hired / Rejected

You can also: write Indeed job postings, generate interview questions, draft offer/rejection letters, summarize pipeline.

Be direct. Owen runs a small business — no fluff."""

conversation_history = []

def run_agent(message: str) -> dict:
    global conversation_history
    conversation_history.append({"role": "user", "content": message})
    messages = list(conversation_history)
    pipeline_changed = False

    while True:
        response = client.messages.create(
            model="claude-sonnet-4-6", max_tokens=2048,
            system=SYSTEM_PROMPT, tools=TOOLS, messages=messages
        )
        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = process_tool(block.name, block.input)
                    tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": json.dumps(result)})
                    if block.name in ["save_candidate", "update_candidate"]:
                        pipeline_changed = True
            assistant_content = []
            for block in response.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    assistant_content.append({"type": "tool_use", "id": block.id, "name": block.name, "input": block.input})
            messages.append({"role": "assistant", "content": assistant_content})
            messages.append({"role": "user", "content": tool_results})
        else:
            assistant_content = [{"type": "text", "text": b.text} for b in response.content if hasattr(b, "text")]
            messages.append({"role": "assistant", "content": assistant_content})
            conversation_history = messages
            break

    text = "".join(b.text for b in response.content if hasattr(b, "text"))
    return {"response": text, "pipeline_changed": pipeline_changed}

# ── File extraction ───────────────────────────────────────────────────────────

def extract_text(file_bytes: bytes, filename: str) -> str:
    name = filename.lower()
    if name.endswith(".pdf"):
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
                return "\n".join(page.extract_text() or "" for page in pdf.pages)
        except ImportError:
            raise RuntimeError("pdfplumber not installed — run setup.bat")
    elif name.endswith(".docx"):
        try:
            from docx import Document
            return "\n".join(p.text for p in Document(io.BytesIO(file_bytes)).paragraphs)
        except ImportError:
            raise RuntimeError("python-docx not installed — run setup.bat")
    elif name.endswith(".txt"):
        return file_bytes.decode("utf-8", errors="ignore")
    raise RuntimeError("Unsupported file type. Drop a PDF, DOCX, or TXT.")

# ── Crew search ───────────────────────────────────────────────────────────────

RADIUS_LOCATIONS = {
    25:  ["Calgary"],
    50:  ["Calgary", "Airdrie", "Cochrane", "Okotoks", "Chestermere"],
    100: ["Calgary", "Airdrie", "Cochrane", "Okotoks", "High River", "Canmore", "Strathmore"],
    999: ["Alberta"],
}

JOB_KEYWORDS = {
    "Deck Builder":    ["deck builder", "deck contractor", "decking contractor", "deck construction"],
    "Fence Installer": ["fence installer", "fence contractor", "fencing contractor", "fence company"],
    "Landscaper":      ["landscaper", "landscaping company", "landscaping contractor", "lawn and garden"],
    "General Labour":  ["general contractor", "construction crew", "labour contractor", "handyman"],
}

JOB_INDIVIDUAL = {
    "Deck Builder":    ["deck builder", "deck carpenter", "deck craftsman"],
    "Fence Installer": ["fence installer", "fence builder", "fencing specialist"],
    "Landscaper":      ["landscaper", "groundskeeper", "lawn care"],
    "General Labour":  ["handyman", "general labourer", "construction worker"],
}

def get_locations(radius_km: int) -> list:
    for r in sorted(RADIUS_LOCATIONS):
        if radius_km <= r:
            return RADIUS_LOCATIONS[r]
    return RADIUS_LOCATIONS[999]

def run_searches(job_type: str, radius_km: int) -> list:
    from ddgs import DDGS
    keywords = JOB_KEYWORDS.get(job_type, [job_type.lower()])
    locs = get_locations(radius_km)
    city = locs[0]
    nearby = " ".join(locs[1:3]) if len(locs) > 1 else ""

    ind = JOB_INDIVIDUAL.get(job_type, [keywords[0]])
    queries = [
        # companies and crews
        f"{keywords[0]} {city} Alberta",
        f"{keywords[0]} {city} Alberta hire",
        f"{keywords[1]} {city} Alberta",
        f"{city} {keywords[0]} company",
        f"kijiji {keywords[0]} {city} Alberta",
        f"homestars {keywords[0]} {city}",
        f"{keywords[0]} {city} contact phone",
        f"{keywords[2]} {city} Alberta",
        # sole proprietors and individuals offering services
        f"independent {ind[0]} {city} Alberta",
        f"freelance {ind[0]} {city} Alberta",
        f"self employed {ind[0]} {city}",
        f"kijiji {ind[0]} {city} for hire",
        f"independent contractor {ind[0]} {city}",
        # people actively seeking work (job wanted)
        f"kijiji {city} {ind[0]} looking for work",
        f"kijiji {city} construction labour jobs wanted",
        f"{ind[0]} available for hire {city} Alberta",
        f"{ind[1]} seeking work {city} Alberta",
        f"skilled labourer looking for work {city} Alberta",
        f"kijiji {city} jobs wanted construction",
        f"carpenter available {city} Alberta",
        f"construction worker looking for work {city}",
        f"reddit {city} {ind[0]} looking for work",
        f"facebook {city} trades worker available",
    ]
    if nearby:
        queries.append(f"{keywords[0]} {nearby} Alberta")
        queries.append(f"{ind[0]} looking for work {nearby} Alberta")

    all_results, seen = [], set()
    print(f"[search] Running {len(queries)} queries for '{job_type}' near {city}")
    with DDGS() as ddgs:
        for query in queries:
            try:
                batch = list(ddgs.text(query, max_results=10))
                print(f"  [{len(batch)} results] {query}")
                for r in batch:
                    if r.get("href") and r["href"] not in seen:
                        seen.add(r["href"])
                        all_results.append(r)
                time.sleep(1.0)
            except Exception as e:
                print(f"  [error] {query}: {e}")
    print(f"[search] Total unique results: {len(all_results)}")
    return all_results

def analyze_leads(results: list, job_type: str, radius_km: int) -> list:
    locs = get_locations(radius_km)
    block = "\n\n".join(
        f"URL: {r.get('href','')}\nTitle: {r.get('title','')[:120]}\nSnippet: {r.get('body','')[:200]}"
        for r in results[:40]
    )
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8000,
        messages=[{"role": "user", "content": f"""You are helping find {job_type} talent near {locs[0]}, Alberta for a small residential contracting company.

From these web search results, extract every plausible lead. This includes:
- Companies and crews of any size
- Sole proprietors and owner-operators
- Independent tradespeople and craftsmen
- Anyone advertising {job_type.lower()} services on Kijiji, HomeStars, Facebook, or their own site
- Handymen or generalists who include {job_type.lower()} in their services
- IMPORTANT: Individuals or pairs actively SEEKING work or posting their availability ("looking for work", "available for hire", "jobs wanted") — these are high-value leads

Be INCLUSIVE. When in doubt, include it. Someone posting "looking for deck work" on Kijiji is just as valuable as a company website.
Only skip results that are clearly 100% unrelated (different industry entirely, or clearly outside Alberta).

Results:
{block}

Return a JSON array only — no markdown, no extra text. Each item:
{{
  "name": "business or person name (use URL domain if no name found)",
  "specialty": "what they do",
  "location": "city/area — use {locs[0]} if unclear but they seem local",
  "phone": "phone number if visible, else empty string",
  "email": "email if visible, else empty string",
  "website": "use the URL from the result",
  "source": "Kijiji if kijiji.ca, HomeStars if homestars.com, Facebook if facebook.com, else Web",
  "type": "Company if a business with employees, Individual if a sole proprietor or one-person operation, Seeking Work if they posted looking for a job or to be hired",
  "notes": "any useful info — reviews, years in business, whether they seem to be a one-person shop or larger crew, contact details"
}}

Return [] only if every single result is completely unrelated to {job_type.lower()} work.
"""}]
    )
    raw = response.content[0].text.strip()
    print(f"[analyze] Claude returned {len(raw)} chars")
    # strip markdown code fences
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("["):
                raw = part
                break
    raw = raw.strip()
    try:
        data = json.loads(raw)
        # normalise field names Claude sometimes varies
        normalised = []
        for item in data:
            normalised.append({
                "name":     item.get("name") or item.get("business_name") or item.get("company") or "Unknown",
                "specialty":item.get("specialty") or item.get("services") or "",
                "location": item.get("location") or item.get("city") or locs[0],
                "phone":    item.get("phone") or item.get("telephone") or "",
                "email":    item.get("email") or "",
                "website":  item.get("website") or item.get("url") or item.get("href") or "",
                "source":   item.get("source") or "Web",
                "type":     item.get("type") or "Company",
                "notes":    item.get("notes") or item.get("snippet") or item.get("description") or "",
            })
        print(f"[analyze] Parsed {len(normalised)} leads")
        return normalised
    except Exception as e:
        print(f"[analyze] Parse error: {e}")
        print(f"[analyze] Raw start: {raw[:300]}")
        return []

def do_search(job_type: str, radius_km: int) -> dict:
    results = run_searches(job_type, radius_km)
    if not results:
        return {"leads": [], "searched": 0}
    leads = analyze_leads(results, job_type, radius_km)
    return {"leads": leads, "searched": len(results)}

# ── Endpoints ─────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str

class SearchRequest(BaseModel):
    job_type: str
    radius_km: int

class SubRequest(BaseModel):
    name: str
    specialty: str = ""
    location: str = ""
    phone: str = ""
    email: str = ""
    website: str = ""
    source: str = ""
    notes: str = ""

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(os.path.join(DIR, "woden_hiring.ico"), media_type="image/x-icon")

@app.get("/", response_class=HTMLResponse)
async def root():
    with open(os.path.join(DIR, "index.html"), encoding="utf-8") as f:
        return f.read()

@app.post("/chat")
async def chat(request: ChatRequest):
    return run_agent(request.message)

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    content = await file.read()
    try:
        text = extract_text(content, file.filename)
    except RuntimeError as e:
        return {"response": str(e), "pipeline_changed": False}
    if not text.strip():
        return {"response": "Could not extract text from that file.", "pipeline_changed": False}
    return run_agent(f"Here is a resume to screen:\n\n{text}")

@app.post("/search-crews")
async def search_crews(request: SearchRequest):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, do_search, request.job_type, request.radius_km)
    return result

@app.get("/candidates")
async def candidates():
    return db_get_candidates()

@app.patch("/candidates/{candidate_id}")
async def update_candidate(candidate_id: int, status: str = None, notes: str = None):
    db_update_candidate(candidate_id, status=status, notes=notes)
    return {"success": True}

@app.get("/subcontractors")
async def subcontractors():
    return db_get_subs()

@app.post("/subcontractors")
async def save_sub(req: SubRequest):
    sid = db_save_sub(**req.dict())
    return {"success": True, "id": sid}

@app.patch("/subcontractors/{sub_id}")
async def update_sub(sub_id: int, status: str = None, notes: str = None):
    db_update_sub(sub_id, status=status, notes=notes)
    return {"success": True}

@app.post("/reset")
async def reset():
    global conversation_history
    conversation_history = []
    return {"success": True}
