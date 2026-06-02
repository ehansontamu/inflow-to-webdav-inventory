import requests
import json
import time
import logging
import os
import signal
import faulthandler
from requests.auth import HTTPDigestAuth
from requests.adapters import HTTPAdapter
from collections import defaultdict

print("==== SCRIPT STARTED ====")

# ─── Logging Configuration ─────────────────────────────────────────────────
base_dir = os.path.dirname(os.path.abspath(__file__))          # folder where script resides
log_dir  = os.path.join(base_dir, "logs")                      # "logs" subfolder beside the script
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, 'inflow_to_webdav.log'),
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
print(f"Logging configured → {log_dir}")

# ─── Hang diagnostics (stack dump on SIGUSR1) ──────────────────────────────
hang_dump_path = os.path.join(log_dir, "hang_dump.log")
_hang_f = open(hang_dump_path, "a", buffering=1)
faulthandler.enable(file=_hang_f)

if hasattr(signal, "SIGUSR1"):
    faulthandler.register(signal.SIGUSR1, file=_hang_f, all_threads=True)
    print(
        f"Faulthandler enabled → {hang_dump_path} "
        f"(Linux: run 'kill -USR1 <pid>' to dump stacks)"
    )
else:
    print(
        f"Faulthandler enabled → {hang_dump_path} "
        f"(Windows detected: SIGUSR1 stack dumps not available)"
    )

# ─── Environment helper ────────────────────────────────────────────────────
def _env(name: str, default=None, required: bool = True):
    """
    Fetch environment variables safely.
    - required=True: raises if missing (recommended for secrets).
    - required=False: allows default.
    """
    val = os.environ.get(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

# ─── InFlow API Configuration ───────────────────────────────────────────────
API_KEY     = _env("INFLOW_API_KEY")
COMPANY_ID  = _env("INFLOW_COMPANY_ID")
BASE_URL    = _env("INFLOW_BASE_URL", "https://cloudapi.inflowinventory.com", required=False)
LOCATION_ID = _env("INFLOW_LOCATION_ID")

INFLOW_HEADERS = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type':  'application/json',
    'Accept':        'application/json;version=2026-02-24'
}
print("InFlow constants defined (from env vars).")

# ─── BigCommerce Configuration ─────────────────────────────────────────────
BC_BASE_URL = _env("BC_BASE_URL", "https://api.bigcommerce.com/stores", required=False)
BC_STORE_ID = _env("BC_STORE_ID")
BC_HEADERS  = {
    'X-Auth-Token': _env("BC_AUTH_TOKEN"),
    'Accept':       'application/json',
    'Content-Type': 'application/json'
}
print("BigCommerce constants defined (from env vars).")

# ─── WebDAV Servers ────────────────────────────────────────────────────────
webdav_servers = [
    {
        "name":     "testing",
        "url":      _env("WEBDAV_URL_TESTING"),
        "username": _env("WEBDAV_USER_TESTING"),
        "password": _env("WEBDAV_PASS_TESTING"),
    },
    {
        "name":     "prod",
        "url":      _env("WEBDAV_URL_PROD"),
        "username": _env("WEBDAV_USER_PROD"),
        "password": _env("WEBDAV_PASS_PROD"),
    }
]
print("WebDAV server configurations set (from env vars).")

# ─── YOUR TENANT'S PRICING SCHEME IDS ──────────────────────────────────────
PRICING_SCHEME_ID_MAP = {
    # Normal Price
    "45712549-61e6-47e1-84de-56baa8f52b9c": "NormalPrice",
    # Retail Price
    "48a0c18e-23a6-411e-b11a-dfc71d05c1de": "RetailPrice",
    # AB Price
    "c09e934a-1d86-4464-8126-88c860f77b7c": "ABPrice",
}

# ─── Timeouts (connect, read) ──────────────────────────────────────────────
# These prevent "random forever hangs" if a TCP/TLS/read stalls.
INFLOW_TIMEOUT = (5, 120)
BC_TIMEOUT     = (5, 120)
WEBDAV_TIMEOUT = (5, 120)

# ─── Persistent Sessions (connection reuse + bigger pools) ─────────────────
INFLOW_SESSION = requests.Session()
INFLOW_SESSION.headers.update(INFLOW_HEADERS)

BC_SESSION = requests.Session()
BC_SESSION.headers.update(BC_HEADERS)

WEBDAV_SESSION = requests.Session()

adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=0)
INFLOW_SESSION.mount("https://", adapter)
BC_SESSION.mount("https://", adapter)
WEBDAV_SESSION.mount("https://", adapter)

print("HTTP sessions configured (pooled connections + timeouts).")

# ─── Helpers for pricing ───────────────────────────────────────────────────
def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None

def extract_price_schemes(product):
    """
    Returns:
      by_id: {pricingSchemeId: unitPrice_float}
      rows:  [ { 'schemeId': str, 'priceType': str, 'unitPrice': float } ... ]
    """
    by_id = {}
    rows = []
    arr = product.get('prices') or []
    if not isinstance(arr, list):
        return by_id, rows

    for entry in arr:
        if not isinstance(entry, dict):
            continue
        sid = entry.get('pricingSchemeId')
        val = _to_float(entry.get('unitPrice') or entry.get('amount') or entry.get('price') or entry.get('value'))
        pt  = entry.get('priceType') or ''
        if sid and val is not None:
            by_id[sid] = val
            rows.append({'schemeId': sid, 'priceType': pt, 'unitPrice': val})
    return by_id, rows

# ─── retry_request helper ──────────────────────────────────────────────────
def retry_request(func, label="", retries=5, delay=5):
    """
    Retries a request-like callable. IMPORTANT: This only helps if the request
    returns or throws. A missing timeout can hang forever, so all requests in
    this script MUST include timeout=(connect, read).
    """
    last_exc = None
    for attempt in range(1, retries + 1):
        t0 = time.monotonic()
        resp = None
        try:
            resp = func()
            if hasattr(resp, "raise_for_status"):
                resp.raise_for_status()
            dt = time.monotonic() - t0
            print(f"   retry_request: success on attempt {attempt} ({label}) in {dt:.2f}s")
            return resp
        except Exception as e:
            dt = time.monotonic() - t0
            last_exc = e
            try:
                if resp is not None:
                    resp.close()
            except Exception:
                pass
            print(f"   retry_request: failed attempt {attempt} ({label}) after {dt:.2f}s — {e}")
            logging.error(f"{label} attempt {attempt}/{retries} failed after {dt:.2f}s: {e}")
            time.sleep(delay)

    logging.error(f"{label} Max retries reached. Last error: {last_exc}")
    print(f"   retry_request: all retries exhausted. ({label})")
    return None

# ─── BigCommerce Orders Status Helper (ALL CALLS RETRY) ───────────────────
def fetch_bc_status_counts(status_id):
    print(f">> fetch_bc_status_counts({status_id}) called")
    counts = defaultdict(int)
    page = 1

    while True:
        print(f"   .. requesting page {page} for status {status_id}")
        resp = retry_request(
            lambda: BC_SESSION.get(
                f"{BC_BASE_URL}/{BC_STORE_ID}/v2/orders",
                params={'status_id': status_id, 'limit': 50, 'page': page},
                timeout=BC_TIMEOUT
            ),
            label=f"BC orders status={status_id} page={page}"
        )

        if not resp:
            print("   .. breaking loop in fetch_bc_status_counts (request error)")
            break

        print(f"   .. got HTTP code {resp.status_code}")

        # Treat 204 or anything non-200 as "stop paging"
        if resp.status_code == 204:
            print("   .. got 204 (no content), breaking")
            break
        if resp.status_code != 200:
            print("   .. breaking loop in fetch_bc_status_counts (non-200)")
            break

        try:
            orders = resp.json()
        except Exception as e:
            print(f"   .. ERROR parsing orders JSON: {e}")
            logging.error(f"BC orders JSON parse failed (status={status_id}, page={page}): {e}")
            break

        if not orders:
            print("   .. no more orders, breaking")
            break

        for idx, order in enumerate(orders, start=1):
            oid = order.get('id')
            if not oid:
                continue

            prod_resp = retry_request(
                lambda: BC_SESSION.get(
                    f"{BC_BASE_URL}/{BC_STORE_ID}/v2/orders/{oid}/products",
                    timeout=BC_TIMEOUT
                ),
                label=f"BC order products oid={oid}"
            )

            if not prod_resp or prod_resp.status_code != 200:
                code = prod_resp.status_code if prod_resp else 'NO RESP'
                print(f"   .. product fetch failed for order {oid} with status {code}")
                continue

            try:
                prods = prod_resp.json()
            except Exception as e:
                print(f"   .. ERROR parsing products JSON for order {oid}: {e}")
                logging.error(f"BC products JSON parse failed for oid={oid}: {e}")
                continue

            for prod in prods:
                key = (prod.get('sku') or prod.get('name') or '').upper()
                try:
                    counts[key] += int(prod.get('quantity', 0))
                except Exception:
                    pass

        page += 1

    print(f"<< fetch_bc_status_counts({status_id}) returning {dict(counts)}")
    return counts

# ─── InFlow fetchers ───────────────────────────────────────────────────────
def fetch_all_products():
    print(">> fetch_all_products() called")
    all_products = []
    count, skip = 100, 0

    while True:
        params = {
            'include':          'category,inventoryLines,prices',
            'count':            count,
            'filter[isActive]': 'true',
            'skip':             skip
        }
        print(f"   .. requesting InFlow products skip={skip}")

        resp = retry_request(
            lambda: INFLOW_SESSION.get(
                f'{BASE_URL}/{COMPANY_ID}/products',
                params=params,
                timeout=INFLOW_TIMEOUT
            ),
            label=f"InFlow products skip={skip}"
        )

        if not resp:
            print("   .. resp is None, breaking from fetch_all_products")
            break

        try:
            batch = resp.json()
        except Exception as e:
            print(f"   .. ERROR parsing InFlow products JSON at skip={skip}: {e}")
            logging.error(f"InFlow products JSON parse failed at skip={skip}: {e}")
            break

        print(f"   .. got {len(batch)} products")
        if not batch:
            break

        all_products.extend(batch)
        skip += count

    print(f"<< fetch_all_products() returning {len(all_products)} items")
    return all_products

def fetch_summaries(products):
    print(">> fetch_summaries() called")
    payload = [
        {'productId': p['productId'], 'locationId': LOCATION_ID}
        for p in products
        if p.get('productId')
    ]
    summaries = []
    BATCH = 100

    for i in range(0, len(payload), BATCH):
        batch = payload[i:i + BATCH]
        print(f"   .. fetching summary batch {i//BATCH + 1}")

        resp = retry_request(
            lambda: INFLOW_SESSION.post(
                f'{BASE_URL}/{COMPANY_ID}/products/summary',
                headers=INFLOW_HEADERS,
                json=batch,
                timeout=INFLOW_TIMEOUT
            ),
            label=f"InFlow summary batch={i//BATCH + 1}"
        )

        if resp and resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as e:
                print(f"   .. summary batch {i//BATCH + 1} JSON parse failed: {e}")
                logging.error(f"Summary batch JSON parse failed batch={i//BATCH + 1}: {e}")
                data = []

            summaries.extend(data)
            print(f"   .. summary batch {i//BATCH + 1} succeeded ({len(data)} items)")
        else:
            code = resp.status_code if resp else 'no resp'
            print(f"   .. summary batch {i//BATCH + 1} failed: {code}")
            logging.error(f"Summary batch {i//BATCH+1} failed: {code}")

        time.sleep(0.2)

    summary_map = {s['productId']: s for s in summaries if isinstance(s, dict) and s.get('productId')}
    print(f"<< fetch_summaries() returning map with {len(summary_map)} entries")
    return summary_map

def fetch_product_list():
    print(">> fetch_product_list() called")
    products = fetch_all_products()
    if not products:
        print("   !! No products fetched; skipping rest of fetch_product_list")
        return

    summary_map = fetch_summaries(products)
    bc_status9  = fetch_bc_status_counts(9)
    bc_status7  = fetch_bc_status_counts(7)

    filtered_products = []
    for p in products:
        pid = p.get('productId')

        raw_avail = summary_map.get(pid, {}).get('quantityAvailable', 0)
        try:
            qty_avail = int(float(raw_avail))
        except (TypeError, ValueError):
            qty_avail = 0

        raw_on_po = summary_map.get(pid, {}).get('quantityOnPurchaseOrder', 0)
        try:
            qty_on_po = int(float(raw_on_po))
        except (TypeError, ValueError):
            qty_on_po = 0

        sku_val   = p.get('sku', '')
        sku_upper = sku_val.upper()

        # schemeId-based mapping
        by_id, rows = extract_price_schemes(p)

        normal_price = None
        ab_price     = None
        retail_price = None
        for scheme_id, price_val in by_id.items():
            name = PRICING_SCHEME_ID_MAP.get(scheme_id)
            if name == "NormalPrice":
                normal_price = price_val
            elif name == "ABPrice":
                ab_price = price_val
            elif name == "RetailPrice":
                retail_price = price_val

        custom_fields = p.get('customFields', {}) or {}

        # closeout + benchmark values
        closeout_val     = custom_fields.get('custom1', '')
        overall_score    = custom_fields.get('custom3', '')
        cpu_score        = custom_fields.get('custom4', '')
        gpu_score        = custom_fields.get('custom5', '')
        memory_score     = custom_fields.get('custom6', '')
        storage_score    = custom_fields.get('custom7', '')
        architecture     = custom_fields.get('custom8', '')
        product_link     = custom_fields.get('custom9', '')
        gpu_type         = custom_fields.get('custom10', '')

        filtered_products.append({
            'productId':  pid,
            'sku':        sku_val,
            'name':       p.get('name', ''),
            'Category':   p.get('category', {}).get('name', ''),
            'Qty':        qty_avail,
            'quantityOnPurchaseOrder': qty_on_po,
            'bc_status9': bc_status9.get(sku_upper, 0),
            'bc_status7': bc_status7.get(sku_upper, 0),

            'NormalPrice': normal_price,
            'ABPrice':     ab_price,
            'RetailPrice': retail_price,
            'Closeout':    closeout_val,

            'OverallScore': overall_score,
            'CPUScore':     cpu_score,
            'GPUScore':     gpu_score,
            'MemoryScore':  memory_score,
            'StorageScore': storage_score,
            'Architecture': architecture,
            'ProductLink':  product_link,
            'GPUType':      gpu_type,

            'PriceBySchemeId': by_id,
            'PriceRows':       rows
        })

    save_items_to_json(filtered_products, "filteredResponse.json")
    upload_to_webdav("filteredResponse.json")

# ─── JSON + WebDAV ────────────────────────────────────────────────────────
def save_items_to_json(items, filename):
    print(f">> save_items_to_json({filename}), item count = {len(items)}")
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(items, f, indent=4, ensure_ascii=False)
        print(f"   Data written to {filename}")
    except Exception as e:
        print(f"   ERROR writing JSON: {e}")
        logging.error(f"Failed to save {filename}: {e}")

def upload_to_webdav(filename):
    print(f">> upload_to_webdav({filename}) called")
    for server in webdav_servers:
        target = f"{server['url'].rstrip('/')}/{filename}"

        def do_put():
            with open(filename, 'rb') as f:
                return WEBDAV_SESSION.put(
                    target,
                    data=f,
                    auth=HTTPDigestAuth(server['username'], server['password']),
                    timeout=WEBDAV_TIMEOUT
                )

        resp = retry_request(do_put, label=f"WebDAV PUT {target}")

        if resp and resp.status_code in (200, 201, 204):
            print(f"   Uploaded {filename} to {server['url']}")
        else:
            if resp is not None:
                body = (resp.text or "")[:400]
                print(f"   Upload failed with {resp.status_code} — {body}")
                logging.error(f"Upload failed ({resp.status_code}) to {server['url']}: {body}")
            else:
                print(f"   No response uploading to {server['url']}")
                logging.error(f"Error uploading to {server['url']}: no response")

# ─── Main (single run) ─────────────────────────────────────────────────────
if __name__ == '__main__':
    print(">>> Running single execution")
    try:
        fetch_product_list()
        print(">>> Done")
    except Exception as e:
        print("!!! Unexpected error during execution:", e)
        logging.error(f"Unexpected error during execution: {e}")
        raise
