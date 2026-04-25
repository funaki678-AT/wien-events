"""
Wien Events Scraper (Version 4 – Email + Telegram + Spotify)
=============================================================
Quellen: barracudamusic.at, arcadia-live.com, volume.at, planet.tt, chelsea.co.at
"""

import re
import time
import hashlib
import logging
import smtplib
import ssl
import urllib.parse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, date, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────
# KONFIGURATION
# ─────────────────────────────────────────────

GOOGLE_CREDENTIALS_FILE = "google_credentials.json"
SPREADSHEET_NAME        = "Wien Events"
SHEET_NAME              = "Events"

# ── Email ────────────────────────────────────
import os
EMAIL_AKTIVIERT       = True
EMAIL_ABSENDER        = "marco.magharai@gmail.com"
EMAIL_PASSWORT        = os.environ.get("EMAIL_PASSWORT", "gzpj tjgr ihkk mudk")
EMAIL_EMPFAENGER      = "marco.magharai@gmail.com"
EMAIL_SMTP_SERVER     = "smtp.gmail.com"
EMAIL_SMTP_PORT       = 587
EMAIL_MIN_NEUE_EVENTS = 1

# ── Telegram ─────────────────────────────────
TELEGRAM_AKTIVIERT  = True
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_TOKEN", "8637523604:AAHRfaomk6nG9oTAyDsg2-1vU60oiXKxrHU")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "5158343454")

# ── Spotify ──────────────────────────────────
SPOTIFY_AKTIVIERT     = True
SPOTIFY_CLIENT_ID     = os.environ.get("SPOTIFY_CLIENT_ID", "DEINE_SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET", "DEIN_SPOTIFY_CLIENT_SECRET")

# ─────────────────────────────────────────────

WIEN_VENUES = [
    "wien", "vienna", "gasometer", "stadthalle", "arena wien",
    "szene wien", "chelsea", "flex", "b 72", "b72", "simm city",
    "porgy", "wuk", "w.u.k.", "flucc", "metastadt", "konzerthaus",
    "musikverein", "volksoper", "burgtheater", "ronacher",
    "pratersauna", "grelle forelle", "flex cafe", "flex halle",
    "viper room", "club lucia", "muth", "radiokulturhaus",
    "prater", "ottakringer", "planet music"
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("wien_scraper.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# HILFSFUNKTIONEN
# ─────────────────────────────────────────────

MONATE_DE = {
    "jan": 1, "jaen": 1, "feb": 2, "mrz": 3, "mar": 3, "maer": 3,
    "apr": 4, "mai": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "okt": 10, "nov": 11, "dez": 12
}


def parse_datum(text: str) -> Optional[str]:
    t = text.strip().lower()
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", t)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).strftime("%d.%m.%Y")
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})\.\s*([a-z]{3})\.?\s+(\d{4})", t)
    if m:
        monat = MONATE_DE.get(m.group(2)[:3])
        if monat:
            try:
                return date(int(m.group(3)), monat, int(m.group(1))).strftime("%d.%m.%Y")
            except ValueError:
                pass
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", t)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).strftime("%d.%m.%Y")
        except ValueError:
            pass
    return None


def ist_wien(venue: str, name: str = "") -> bool:
    text = (venue + " " + name).lower()
    return any(w in text for w in WIEN_VENUES)


def make_id(name: str, datum: str, venue: str) -> str:
    raw = f"{name}-{datum}-{venue}".lower()
    raw = re.sub(r"[^a-z0-9\-]", "-", raw)
    raw = re.sub(r"-+", "-", raw).strip("-")
    if len(raw) > 80:
        h = hashlib.md5(raw.encode()).hexdigest()[:8]
        raw = raw[:70] + "-" + h
    return raw


def heute() -> str:
    return datetime.now().strftime("%d.%m.%Y")


def get(url: str) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r
    except Exception as e:
        log.error(f"  Fehler beim Laden: {url}  ({e})")
        return None


# ─────────────────────────────────────────────
# SPOTIFY
# ─────────────────────────────────────────────

def hole_spotify_kuenstler() -> set:
    """Holt alle Künstler aus der Spotify-Bibliothek (gespeicherte Alben + Künstler)."""
    if not SPOTIFY_AKTIVIERT:
        return set()
    if "DEINE" in SPOTIFY_CLIENT_ID:
        log.warning("  Spotify: Client ID noch nicht konfiguriert!")
        return set()

    log.info("🎧 Spotify-Bibliothek wird geladen...")

    # Client Credentials Flow (kein User-Login nötig für öffentliche Daten)
    # Für persönliche Bibliothek brauchen wir Authorization Code Flow
    # Wir verwenden einen vereinfachten Ansatz: gespeicherte Künstler via Token

    try:
        # Access Token holen
        token_url = "https://accounts.spotify.com/api/token"
        token_data = {
            "grant_type": "client_credentials",
            "client_id": SPOTIFY_CLIENT_ID,
            "client_secret": SPOTIFY_CLIENT_SECRET,
        }
        token_r = requests.post(token_url, data=token_data, timeout=10)
        token_r.raise_for_status()
        access_token = token_r.json()["access_token"]
        headers = {"Authorization": f"Bearer {access_token}"}

        kuenstler = set()

        # Gespeicherte Alben laden (enthält Künstlernamen)
        url = "https://api.spotify.com/v1/me/albums?limit=50"
        # Hinweis: /me Endpoints brauchen User-Token, nicht Client Token
        # Wir laden stattdessen die Follow-Liste
        url = "https://api.spotify.com/v1/me/following?type=artist&limit=50"

        # Da Client Credentials keinen Zugriff auf /me haben,
        # lesen wir die Künstler aus einer lokalen Cache-Datei
        # die beim ersten manuellen Setup erstellt wird
        import os
        cache_datei = "spotify_kuenstler.txt"

        if os.path.exists(cache_datei):
            with open(cache_datei, "r", encoding="utf-8") as f:
                kuenstler = set(line.strip().lower() for line in f if line.strip())
            log.info(f"  → {len(kuenstler)} Künstler aus Cache geladen.")
        else:
            log.warning(f"  Spotify Cache nicht gefunden: {cache_datei}")
            log.warning("  Bitte einmalig 'python spotify_setup.py' ausführen!")

        return kuenstler

    except Exception as e:
        log.error(f"  Spotify Fehler: {e}")
        return set()


def pruefe_spotify_matches(events: list, spotify_kuenstler: set) -> list:
    """Prüft welche Events Künstler aus der Spotify-Bibliothek sind."""
    if not spotify_kuenstler:
        return []

    matches = []
    for ev in events:
        name = ev.get("Name", "").lower()
        # Künstlername im Event-Namen suchen
        for kuenstler in spotify_kuenstler:
            if kuenstler and kuenstler in name:
                matches.append(ev)
                log.info(f"  🎵 Spotify-Match: {ev['Name']} ({ev.get('Datum', '')})")
                break
    return matches


# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────

def sende_telegram(nachricht: str):
    """Sendet eine Telegram-Nachricht."""
    if not TELEGRAM_AKTIVIERT:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": nachricht,
            "parse_mode": "HTML"
        }, timeout=10)
        r.raise_for_status()
        log.info("  ✅ Telegram-Nachricht gesendet!")
    except Exception as e:
        log.error(f"  ❌ Telegram Fehler: {e}")


def sende_telegram_spotify_matches(matches: list):
    """Sendet Telegram-Benachrichtigung für Spotify-Matches."""
    if not matches:
        return

    log.info(f"📱 Sende Telegram für {len(matches)} Spotify-Matches...")

    for ev in matches:
        preis = f" · {ev.get('Günstigster Preis (€)', '')} €" if ev.get("Günstigster Preis (€)") else ""
        link  = ev.get("Ticket-Link", "")

        nachricht = (
            f"🎵 <b>Konzert-Alarm!</b>\n\n"
            f"<b>{ev.get('Name', '?')}</b>\n"
            f"📅 {ev.get('Datum', '?')}"
            f"{' · ' + ev.get('Uhrzeit', '') if ev.get('Uhrzeit') else ''}\n"
            f"📍 {ev.get('Venue', '?')}{preis}\n"
            f"🔗 <a href='{link}'>Tickets</a>" if link else ""
        )

        sende_telegram(nachricht)
        time.sleep(1)  # Kurze Pause zwischen Nachrichten


# ─────────────────────────────────────────────
# EMAIL
# ─────────────────────────────────────────────

def sende_email(alle_events: list, neue_events: list, stats: dict):
    """Sendet eine HTML-Email mit der Scraping-Zusammenfassung."""
    if not EMAIL_AKTIVIERT:
        log.info("  Email deaktiviert")
        return

    if len(neue_events) < EMAIL_MIN_NEUE_EVENTS:
        log.info(f"  Email übersprungen: nur {len(neue_events)} neue Events "
                 f"(Minimum: {EMAIL_MIN_NEUE_EVENTS})")
        return

    log.info(f"📧 Sende Email an {EMAIL_EMPFAENGER}...")

    def datum_sort(ev):
        try:
            return datetime.strptime(ev.get("Datum", ""), "%d.%m.%Y")
        except:
            return datetime.max

    neue_sortiert = sorted(neue_events, key=datum_sort)[:20]
    heute_str = datetime.now().strftime("%d.%m.%Y %H:%M")

    neue_html = ""
    if neue_sortiert:
        for ev in neue_sortiert:
            preis = f" · {ev.get('Günstigster Preis (€)', '')} €" if ev.get("Günstigster Preis (€)") else ""
            link  = ev.get("Ticket-Link", "")
            name_teil = f'<a href="{link}" style="color:#d4a843;text-decoration:none;">{ev["Name"]}</a>' if link else ev["Name"]
            neue_html += f"""
            <tr>
              <td style="padding:8px 4px;border-bottom:1px solid #2d1f0e;color:#f5ede0;">{name_teil}</td>
              <td style="padding:8px 4px;border-bottom:1px solid #2d1f0e;color:#8a7a6a;white-space:nowrap;">{ev.get("Datum", "")}</td>
              <td style="padding:8px 4px;border-bottom:1px solid #2d1f0e;color:#8a7a6a;">{ev.get("Venue", "")}{preis}</td>
            </tr>"""
    else:
        neue_html = '<tr><td colspan="3" style="padding:8px;color:#8a7a6a;">Keine neuen Events.</td></tr>'

    stats_html = ""
    for quelle, anzahl in stats.items():
        stats_html += f"""
        <tr>
          <td style="padding:4px 8px;color:#f5ede0;">{quelle}</td>
          <td style="padding:4px 8px;color:#d4a843;text-align:right;">{anzahl}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="de"><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#1a1208;font-family:'Segoe UI',Arial,sans-serif;">
  <div style="max-width:600px;margin:0 auto;padding:24px 16px;">
    <div style="border-bottom:2px solid #d4a843;padding-bottom:16px;margin-bottom:24px;">
      <h1 style="margin:0;font-size:24px;color:#d4a843;">🎵 Wien Events</h1>
      <p style="margin:4px 0 0;font-size:13px;color:#8a7a6a;">Täglicher Report · {heute_str}</p>
    </div>
    <div style="background:#2d1f0e;border-radius:6px;padding:16px;margin-bottom:24px;">
      <h2 style="margin:0 0 12px;font-size:16px;color:#f5ede0;">📊 Zusammenfassung</h2>
      <table style="width:100%;border-collapse:collapse;">
        {stats_html}
        <tr style="border-top:1px solid #1a1208;">
          <td style="padding:6px 8px;color:#f5ede0;font-weight:bold;">Gesamt</td>
          <td style="padding:6px 8px;color:#d4a843;font-weight:bold;text-align:right;">{len(alle_events)}</td>
        </tr>
        <tr>
          <td style="padding:4px 8px;color:#c0392b;font-weight:bold;">🆕 Neu heute</td>
          <td style="padding:4px 8px;color:#c0392b;font-weight:bold;text-align:right;">{len(neue_events)}</td>
        </tr>
      </table>
    </div>
    <div style="margin-bottom:24px;">
      <h2 style="margin:0 0 12px;font-size:16px;color:#f5ede0;">🆕 Neue Events</h2>
      <table style="width:100%;border-collapse:collapse;">
        <thead>
          <tr style="background:#2d1f0e;">
            <th style="padding:8px 4px;text-align:left;color:#d4a843;font-size:12px;">Event</th>
            <th style="padding:8px 4px;text-align:left;color:#d4a843;font-size:12px;">Datum</th>
            <th style="padding:8px 4px;text-align:left;color:#d4a843;font-size:12px;">Venue · Preis</th>
          </tr>
        </thead>
        <tbody>{neue_html}</tbody>
      </table>
    </div>
    <div style="border-top:1px solid #2d1f0e;padding-top:16px;text-align:center;">
      <p style="font-size:12px;color:#8a7a6a;margin:0;">Wien Events Scraper · Automatisch generiert</p>
    </div>
  </div>
</body></html>"""

    text = f"Wien Events – Report {heute_str}\nGesamt: {len(alle_events)}\nNeu: {len(neue_events)}\n"
    for ev in neue_sortiert:
        text += f"  • {ev['Name']} – {ev.get('Datum', '')} – {ev.get('Venue', '')}\n"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"🎵 Wien Events: {len(neue_events)} neue Events · {heute()}"
    msg["From"]    = EMAIL_ABSENDER
    msg["To"]      = EMAIL_EMPFAENGER
    msg.attach(MIMEText(text, "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(EMAIL_ABSENDER, EMAIL_PASSWORT)
            server.sendmail(EMAIL_ABSENDER, EMAIL_EMPFAENGER, msg.as_string())
        log.info("  ✅ Email erfolgreich gesendet!")
    except smtplib.SMTPAuthenticationError:
        log.error("  ❌ Email-Auth fehlgeschlagen! Gmail App-Passwort prüfen.")
    except Exception as e:
        log.error(f"  ❌ Email Fehler: {e}")


# ─────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────

SPALTEN = [
    "ID", "Name", "Datum", "Uhrzeit", "Venue", "Kategorie", "Genre",
    "Günstigster Preis (€)", "Preis-Quelle", "Ticket-Link",
    "Datenquelle(n)", "Zuletzt aktualisiert"
]


def verbinde_sheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds   = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scopes)
    client  = gspread.authorize(creds)
    tabelle = client.open(SPREADSHEET_NAME)
    try:
        sheet = tabelle.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        sheet = tabelle.add_worksheet(title=SHEET_NAME, rows=5000, cols=20)
    if not sheet.get_all_values() or sheet.cell(1, 1).value != "ID":
        sheet.clear()
        sheet.append_row(SPALTEN)
        log.info("  Überschriften gesetzt.")
    return sheet


def lade_ids(sheet) -> dict:
    alle = sheet.get_all_values()
    return {z[0]: i + 2 for i, z in enumerate(alle[1:]) if z and z[0]}


def speichere(sheet, events: list) -> list:
    if not events:
        log.info("  Keine Events zu speichern.")
        return []

    bestehende    = lade_ids(sheet)
    neue_zeilen   = []
    update_zeilen = []
    neue_events   = []

    for ev in events:
        zeile = [
            ev.get("ID", ""), ev.get("Name", ""), ev.get("Datum", ""),
            ev.get("Uhrzeit", ""), ev.get("Venue", ""),
            ev.get("Kategorie", "Konzert"), ev.get("Genre", ""),
            ev.get("Günstigster Preis (€)", ""), ev.get("Preis-Quelle", ""),
            ev.get("Ticket-Link", ""), ev.get("Datenquelle(n)", ""), heute()
        ]
        ev_id = ev.get("ID", "")
        if ev_id in bestehende:
            update_zeilen.append((bestehende[ev_id], zeile))
        else:
            neue_zeilen.append(zeile)
            neue_events.append(ev)
            bestehende[ev_id] = -1

    if neue_zeilen:
        sheet.append_rows(neue_zeilen, value_input_option="USER_ENTERED")
        log.info(f"  {len(neue_zeilen)} neue Events geschrieben.")
        time.sleep(2)

    for row_num, zeile in update_zeilen:
        sheet.update(values=[zeile], range_name=f"A{row_num}:L{row_num}")
        time.sleep(1.5)

    log.info(f"  → {len(neue_zeilen)} neu, {len(update_zeilen)} aktualisiert.")
    return neue_events


# ─────────────────────────────────────────────
# SCRAPER: BARRACUDA
# ─────────────────────────────────────────────

def scrape_barracuda() -> list:
    log.info("🎸 barracudamusic.at – wird gelesen...")
    events   = []
    gesehen  = set()
    heute_dt = datetime.now()

    for offset in range(9):  # 9 Monate voraus
        ziel = (heute_dt.replace(day=1) + timedelta(days=32 * offset)).replace(day=1)
        url  = f"https://www.barracudamusic.at/shows/{ziel.year}-{ziel.month:02d}/"
        r = get(url)
        if not r:
            continue

        soup     = BeautifulSoup(r.text, "html.parser")
        neu_hier = 0

        for li in soup.find_all("li"):
            a = li.find("a", href=re.compile(r"/event/"))
            if not a:
                continue
            h    = a.find(["h2", "h3", "h4", "strong"])
            name = h.get_text(strip=True) if h else a.get_text(separator="\n", strip=True).split("\n")[0]
            name = re.sub(r"^(Sold\s*Out|Abgesagt|Neuer\s+Termin:?)\s*", "", name, flags=re.I).strip()
            if not name:
                continue

            full  = a.get_text(separator=" ", strip=True)
            datum = parse_datum(full)
            if not datum:
                continue

            venue = ""
            m = re.search(r"[—–]\s*(.+?)$", full)
            if m:
                venue = m.group(1).strip()
            if not ist_wien(venue, name):
                continue

            href   = a.get("href", "")
            link   = href if href.startswith("http") else "https://www.barracudamusic.at" + href
            status = ""
            if li.find(string=re.compile(r"Sold\s*Out", re.I)):
                status = " (Sold Out)"
            elif li.find(string=re.compile(r"Abgesagt", re.I)):
                status = " (Abgesagt)"

            ev_id = make_id(name, datum, venue)
            if ev_id in gesehen:
                continue
            gesehen.add(ev_id)
            neu_hier += 1

            events.append({
                "ID": ev_id, "Name": name, "Datum": datum,
                "Uhrzeit": "", "Venue": venue,
                "Kategorie": f"Konzert{status}", "Genre": "",
                "Ticket-Link": link, "Datenquelle(n)": "barracudamusic.at"
            })

        log.info(f"  {ziel.strftime('%b %Y')}: {neu_hier} neue Wien-Events")
        time.sleep(1)

    log.info(f"  → Gesamt {len(events)} Wien-Events von Barracuda.")
    return events


# ─────────────────────────────────────────────
# SCRAPER: ARCADIA
# ─────────────────────────────────────────────

def scrape_arcadia() -> list:
    log.info("🎵 arcadia-live.com – wird gelesen...")
    START_URL = "https://arcadia-live.com/events/?tx_corporate_eventlist%5Bfilter%5D%5Bcity%5D=1997"
    events  = []
    gesehen = set()
    besucht = set()
    queue   = [START_URL]

    while queue:
        url = queue.pop(0)
        if url in besucht:
            continue
        besucht.add(url)
        r = get(url)
        if not r:
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "tx_corporate_eventlist" in href and "1997" in href:
                full = href if href.startswith("http") else "https://arcadia-live.com" + href
                if full not in besucht:
                    queue.append(full)

        neu = 0
        for karte in soup.find_all("a", href=re.compile(r"/artists/detail/")):
            h = karte.find(["h3", "h2", "h4"])
            if not h:
                continue
            name = h.get_text(strip=True)
            if not name:
                continue

            text  = karte.get_text(separator="\n", strip=True)
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            datum = None
            venue = ""

            for i, line in enumerate(lines):
                d = parse_datum(line)
                if d:
                    datum = d
                    rest  = re.sub(r"\d{2}\.\d{2}\.\d{4}", "", line).strip()
                    rest  = re.sub(r"^Wien\s*", "", rest, flags=re.I).strip()
                    if rest:
                        venue = rest
                    elif i + 1 < len(lines):
                        naechste = lines[i + 1]
                        if naechste not in ["Concert", "Entertainment", "Tour", "Festival",
                                            "Ausverkauft", "Abgesagt", name]:
                            venue = naechste
                    venue = f"{venue} Wien".strip() if venue else "Wien"
                    break

            if not datum:
                continue

            kategorie = "Konzert"
            for line in lines:
                if line in ["Concert", "Entertainment", "Tour", "Festival"]:
                    kategorie = line
                    break
            if re.search(r"Ausverkauft", text, re.I):
                kategorie += " (Ausverkauft)"
            elif re.search(r"Abgesagt", text, re.I):
                kategorie += " (Abgesagt)"

            uhrzeit = ""
            m = re.search(r"\b(\d{2}:\d{2})\b", text)
            if m:
                uhrzeit = m.group(1)

            href  = karte.get("href", "")
            link  = href if href.startswith("http") else "https://arcadia-live.com" + href
            ev_id = make_id(name, datum, venue)
            if ev_id in gesehen:
                continue
            gesehen.add(ev_id)
            neu += 1

            events.append({
                "ID": ev_id, "Name": name, "Datum": datum,
                "Uhrzeit": uhrzeit, "Venue": venue,
                "Kategorie": kategorie, "Genre": "",
                "Ticket-Link": link, "Datenquelle(n)": "arcadia-live.com"
            })

        log.info(f"  Seite {len(besucht)}: {neu} neue Wien-Events")
        time.sleep(1)

    log.info(f"  → Gesamt {len(events)} Wien-Events von Arcadia.")
    return events


# ─────────────────────────────────────────────
# SCRAPER: VOLUME
# ─────────────────────────────────────────────

def scrape_volume() -> list:
    log.info("🎶 volume.at – wird gelesen...")
    r = get("https://www.volume.at/konzerte-wien/")
    if not r:
        return []

    soup    = BeautifulSoup(r.text, "html.parser")
    events  = []
    gesehen = set()

    for a in soup.find_all("a", href=re.compile(r"/events/[^?#]+/?")):
        href = a.get("href", "")
        m    = re.search(r"-(\d{4}-\d{2}-\d{2})/?$", href)
        if not m:
            continue
        datum = parse_datum(m.group(1))
        if not datum:
            continue

        container = a.find_parent(["div", "article", "li", "section"])
        if not container:
            continue

        h        = container.find(["h2", "h3"])
        name_raw = h.get_text(strip=True) if h else a.get_text(strip=True)
        if not name_raw or len(name_raw) < 2:
            continue
        if re.search(r"advertorial|clvb|traumjob|gewinnspiel", name_raw, re.I):
            continue
        if "/musik/" in href or "/stories/" in href:
            continue

        venue   = ""
        venue_a = container.find("a", href=re.compile(r"/venues/"))
        if venue_a:
            venue = re.sub(r"\s+Wien\s*$", "", venue_a.get_text(strip=True)).strip()

        container_text = container.get_text(separator=" ", strip=True)
        if not re.search(r"\bWien\b", container_text):
            continue

        name = name_raw
        if venue and name_raw.endswith(venue):
            name = name_raw[:-len(venue)].strip()
        elif venue and venue in name_raw:
            name = name_raw.replace(venue, "").strip()
        if not name:
            name = name_raw

        preis  = ""
        preise = re.findall(r"(?:VVK|AK)[^\d]*(\d+(?:[.,]\d+)?)", container_text)
        if preise:
            vals = [float(p.replace(",", ".")) for p in preise if p]
            if vals:
                preis = str(min(vals)).rstrip("0").rstrip(".")

        genre = ""
        genre_links = container.find_all("a", href=re.compile(r"genre="))
        if genre_links:
            genre = ", ".join(g.get_text(strip=True) for g in genre_links[:2])

        link  = href if href.startswith("http") else "https://www.volume.at" + href
        ev_id = make_id(name, datum, venue)
        if ev_id in gesehen:
            continue
        gesehen.add(ev_id)

        events.append({
            "ID": ev_id, "Name": name, "Datum": datum,
            "Uhrzeit": "", "Venue": venue, "Kategorie": "Konzert", "Genre": genre,
            "Günstigster Preis (€)": preis,
            "Preis-Quelle": "volume.at" if preis else "",
            "Ticket-Link": link, "Datenquelle(n)": "volume.at"
        })

    log.info(f"  → {len(events)} Wien-Events von volume.at.")
    return events


# ─────────────────────────────────────────────
# SCRAPER: PLANET.TT
# ─────────────────────────────────────────────

def scrape_planet() -> list:
    log.info("🎸 planet.tt – wird gelesen...")
    r = get("https://planet.tt/")
    if not r:
        return []

    soup    = BeautifulSoup(r.text, "html.parser")
    events  = []
    gesehen = set()
    bloecke = []

    for el in soup.find_all(["div", "article", "li"]):
        t = el.get_text()
        if re.search(r"(Mo|Di|Mi|Do|Fr|Sa|So)\.,\s*\d+\.\s+\w+\s+\d{4}", t):
            parent = el.find_parent(["div", "article", "li"])
            if parent and re.search(r"(Mo|Di|Mi|Do|Fr|Sa|So)\.,\s*\d+\.\s+\w+\s+\d{4}", parent.get_text()):
                continue
            bloecke.append(el)

    for block in bloecke:
        text  = block.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        datum = None
        name  = ""
        uhrzeit = preis = status = link = ""

        for line in lines:
            d = parse_datum(line)
            if d and not datum:
                datum = d
                continue
            m = re.search(r"(\d{2}:\d{2})\s*Uhr", line)
            if m and not uhrzeit:
                uhrzeit = m.group(1)
                continue
            m = re.search(r"(\d+(?:[.,]\d+)?)\s*€", line)
            if m and not preis:
                preis = m.group(1).replace(",", ".")
                continue
            if re.search(r"^(Verlegt|Abgesagt|Ausverkauft|Sold.?Out)$", line, re.I):
                status = line
                continue
            if (len(line) > len(name) and
                not re.search(r"Uhr|€|Einlass|Beginn|Vorverkauf|Abendkassa|presented|"
                              r"Events|Filter|Suche|Kalender|mehr|Planet|Simm|szene", line, re.I) and
                not re.match(r"^(Mo|Di|Mi|Do|Fr|Sa|So)\.", line)):
                name = line

        if not datum or not name:
            continue

        a = block.find("a", href=True)
        if a:
            href = a["href"]
            link = href if href.startswith("http") else "https://planet.tt" + href

        kategorie = "Konzert"
        if status:
            kategorie += f" ({status})"

        venue = "Raiffeisen Halle Gasometer"
        if "szene" in link.lower() or "szene" in text.lower():
            venue = "((szene)) Wien"
        elif "simm" in link.lower() or "simmcity" in text.lower():
            venue = "SimmCity Wien"

        ev_id = make_id(name, datum, venue)
        if ev_id in gesehen:
            continue
        gesehen.add(ev_id)

        events.append({
            "ID": ev_id, "Name": name, "Datum": datum,
            "Uhrzeit": uhrzeit, "Venue": venue,
            "Kategorie": kategorie, "Genre": "",
            "Günstigster Preis (€)": preis,
            "Preis-Quelle": "planet.tt" if preis else "",
            "Ticket-Link": link, "Datenquelle(n)": "planet.tt"
        })

    log.info(f"  → Gesamt {len(events)} Events von planet.tt.")
    return events


# ─────────────────────────────────────────────
# SCRAPER: CHELSEA
# ─────────────────────────────────────────────

def scrape_chelsea() -> list:
    log.info("🎵 chelsea.co.at – wird gelesen...")
    r = get("https://www.chelsea.co.at/concerts.php")
    if not r:
        return []

    soup    = BeautifulSoup(r.text, "html.parser")
    events  = []
    gesehen = set()
    tabellen = soup.find_all("table")
    aktueller_monat = ""
    aktuelles_jahr  = datetime.now().year

    for tabelle in tabellen:
        kopf = tabelle.find("tr")
        if kopf:
            kopf_text = kopf.get_text(strip=True)
            m = re.match(r"(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+(\d{4})", kopf_text)
            if m:
                aktueller_monat = m.group(1)
                aktuelles_jahr  = int(m.group(2))

        for tr in tabelle.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            tag_text = tds[0].get_text(strip=True)
            if not re.match(r"^\d{1,2}$", tag_text):
                continue
            tag     = int(tag_text)
            name_td = tds[1]
            a       = name_td.find("a")
            name    = name_td.get_text(strip=True)
            if not name:
                continue

            monat = MONATE_DE.get(aktueller_monat.lower()[:3])
            if not monat:
                continue
            try:
                datum = date(aktuelles_jahr, monat, tag).strftime("%d.%m.%Y")
            except ValueError:
                continue

            anchor_id = ""
            if a:
                anchor_id = a.get("href", "").lstrip("#")
            link = f"https://www.chelsea.co.at/concerts.php#{anchor_id}" if anchor_id else \
                   "https://www.chelsea.co.at/concerts.php"

            preis = uhrzeit = ""
            if anchor_id:
                anker = soup.find(id=anchor_id)
                if anker:
                    detail = anker.get_text(separator="\n", strip=True)
                    preise = re.findall(r"(\d+)(?:[,.](?:\d+))?(?:\s*,-|\s*€)", detail)
                    if preise:
                        preis = str(min(int(p) for p in preise))
                    m = re.search(r"(\d{2}:\d{2})\s*(?:h|Uhr)", detail, re.I)
                    if m:
                        uhrzeit = m.group(1)

            ev_id = make_id(name, datum, "Chelsea Wien")
            if ev_id in gesehen:
                continue
            gesehen.add(ev_id)

            events.append({
                "ID": ev_id, "Name": name, "Datum": datum,
                "Uhrzeit": uhrzeit, "Venue": "Chelsea Wien",
                "Kategorie": "Konzert", "Genre": "",
                "Günstigster Preis (€)": preis,
                "Preis-Quelle": "chelsea.co.at" if preis else "",
                "Ticket-Link": link, "Datenquelle(n)": "chelsea.co.at"
            })

    log.info(f"  → {len(events)} Events von chelsea.co.at.")
    return events


# ─────────────────────────────────────────────
# HAUPTPROGRAMM
# ─────────────────────────────────────────────

def main():
    log.info("=" * 50)
    log.info("Wien Events Scraper v4 – Start")
    log.info(f"Datum: {heute()}")
    log.info("=" * 50)

    alle  = []
    alle += scrape_barracuda(); time.sleep(1)
    alle += scrape_arcadia();   time.sleep(1)
    alle += scrape_volume();    time.sleep(1)
    alle += scrape_planet();    time.sleep(1)
    alle += scrape_chelsea()

    quellen = ["barracudamusic.at", "arcadia-live.com", "volume.at", "planet.tt", "chelsea.co.at"]
    stats   = {}
    log.info(f"\n📊 Gesamt: {len(alle)} Wien-Events")
    for quelle in quellen:
        n = sum(1 for e in alle if e.get("Datenquelle(n)") == quelle)
        stats[quelle] = n
        log.info(f"   {quelle:25s}: {n}")

    if not alle:
        log.warning("Keine Events gefunden.")
        return

    # Spotify-Künstler laden
    spotify_kuenstler = hole_spotify_kuenstler()

    # Google Sheets
    log.info("\n☁️  Verbinde mit Google Sheets...")
    neue_events = []
    try:
        sheet       = verbinde_sheets()
        neue_events = speichere(sheet, alle)
    except FileNotFoundError:
        log.error(f"'{GOOGLE_CREDENTIALS_FILE}' nicht gefunden!")
        return
    except Exception as e:
        log.error(f"Google Sheets Fehler: {e}")
        return

    # Spotify-Abgleich nur mit NEUEN Events
    spotify_matches = pruefe_spotify_matches(neue_events, spotify_kuenstler)
    if spotify_matches:
        log.info(f"\n🎵 {len(spotify_matches)} Spotify-Matches gefunden!")
    else:
        log.info("\n🎵 Keine neuen Spotify-Matches.")

    # Telegram für Spotify-Matches
    if spotify_matches:
        log.info(f"\n📱 Telegram-Benachrichtigung für Spotify-Matches...")
        sende_telegram_spotify_matches(spotify_matches)

    # Email
    log.info(f"\n📧 Email-Benachrichtigung ({len(neue_events)} neue Events)...")
    sende_email(alle, neue_events, stats)

    log.info("\n✅ Fertig!")


if __name__ == "__main__":
    main()
