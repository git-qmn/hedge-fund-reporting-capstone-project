from __future__ import annotations
import json
from typing import Iterable, List, Tuple
from dotenv import load_dotenv
from src.db_connection import get_snowflake_connection
from src.open_ai_interactions import get_openai_client_obj, interact_with_chat_application

load_dotenv()

# Firm + Strategy

FIRM_SECTION_PROMPTS = {
    "Overview": (
        "Write 2–3 sentences describing Novalon (hedge fund manager) for an institutional fact sheet. "
        "Keep factual, no hype, no 'we'. Mention diversified alternatives and disciplined process."
    ),
    "Philosophy": (
        "Write 3 sentences: alpha source, investment horizon, and constraints (risk, liquidity, cost). "
        "Avoid promises ('will', 'guarantee'). No marketing buzzwords."
    ),
    "Team": (
        "Write 2 sentences: team composition (PMs/analysts/ops) and decision-making model."
    ),
    "Risk": (
        "Return exactly 3 bullets, <=10 words each, noun-led (no 'we'): "
        "position limits; liquidity checks; scenario/VAR monitoring."
    ),
}

STRATEGY_NAMES: List[str] = [
    "130/30 Long-Short Equity",
    "Global Growth Stocks",
    "Emerging Markets Equity",
    "Convertible Bond Arbitrage",
    "Multi-Asset Long/Short",
    "ESG Focused Equity",
    "Technology Growth",
    "Hedge Fund Core",
]

STRATEGY_SECTIONS = ["Investment Process", "Team", "Risk Management"]

STRAT_SECTION_PROMPTS = {
    "Investment Process": (
        "Strategy: {name}. Write 3 sentences: universe & signals, portfolio construction "
        "(position sizing, diversification), and turnover/liquidity. No fluff."
    ),
    "Team": (
        "Strategy: {name}. Write 1–2 sentences on coverage and key PM responsibilities."
    ),
    "Risk Management": (
        "Strategy: {name}. Return 3 bullets, <=10 words each, on risk controls."
    ),
}

def gen_text(client, system_msg: str, user_msg: str) -> str:
    """Single call to the model, plain text only."""
    r = interact_with_chat_application(user_msg, client, system_msg)
    return r.choices[0].message.content.strip()

def gen_strategy_codes_with_gpt(client, names: List[str], prefix: str = "NOV") -> List[Tuple[str, str]]:
    """
    Return list of (code, name). Uses GPT to create mnemonic codes like NOV13030.
    Falls back to deterministic codes if JSON parsing fails.
    """
    sys = "You generate short ticker-like codes. Return strict JSON; no prose."
    user = (
        f"Create unique strategy codes for these strategies with prefix '{prefix}'. "
        "Rules: CODE must be 4–7 chars, uppercase A–Z/0–9, start with the prefix, "
        "be mnemonic (digits allowed, e.g., 13030). "
        'Return JSON array of objects: [{"code":"NOV13030","name":"130/30 Long-Short Equity"}, ...].\n\n'
        f"Strategies: {names}"
    )
    try:
        resp = interact_with_chat_application(user, client, sys)
        items = json.loads(resp.choices[0].message.content)
        pairs = [(x["code"], x["name"]) for x in items if "code" in x and "name" in x]
        if pairs:
            return pairs
    except Exception:
        pass  # fall back below

    # Fallback: deterministic codes
    pairs = []
    for n in names:
        base = (
            n.upper()
             .replace("LONG-SHORT", "LS")
             .replace("EQUITY", "EQ")
             .replace(" ", "")
             .replace("-", "")
        )
        pairs.append((prefix + base[:5], n))
    return pairs

def upsert_firm(conn, section: str, content: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM FIRMINFO WHERE SECTION = %s", (section,))
        cur.execute("INSERT INTO FIRMINFO (SECTION, CONTENT) VALUES (%s, %s)", (section, content))

def upsert_strategy(conn, code: str, section: str, content: str) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM STRATEGYINFO WHERE STRATEGYCODE = %s AND SECTION = %s", (code, section))
        cur.execute(
            "INSERT INTO STRATEGYINFO (STRATEGYCODE, SECTION, CONTENT) VALUES (%s, %s, %s)",
            (code, section, content),
        )

def main() -> None:
    conn = get_snowflake_connection()
    client = get_openai_client_obj()

    # Firm content
    for section, prompt in FIRM_SECTION_PROMPTS.items():
        text = gen_text(client, "Professional, factual. No markdown.", prompt)
        upsert_firm(conn, section, text)
        print(f"FIRMINFO -> {section}")

    # Strategy codes + content
    code_name_pairs = gen_strategy_codes_with_gpt(client, STRATEGY_NAMES, prefix="NOV")
    for code, name in code_name_pairs:
        for section in STRATEGY_SECTIONS:
            user = STRAT_SECTION_PROMPTS[section].format(name=name)
            text = gen_text(client, "Professional, factual. No markdown.", user)
            upsert_strategy(conn, code, section, text)
            print(f"STRATEGYINFO -> {code} / {section}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
