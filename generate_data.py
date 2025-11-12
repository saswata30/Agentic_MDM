# AgenticMDM - RAW synthetic data generator for Northbridge Financial
# Generates 5 raw datasets strictly following the demo story
# - security_master_ingest
# - etf_constituents
# - vendor_change_log
# - steward_commentary
# - holdings_report
#
# Key principles:
# - Schema fidelity, realistic distributions, reproducible, vectorized
# - Event window: 2025-08-20..2025-09-12 with triggers (BBG 2025-08-19, Moody's 2025-08-25), fix 2025-09-05 and normalization ~2025-09-10
# - Timestamps are tz-naive datetime64[ms]

import random
import string

import numpy as np
import pandas as pd
from faker import Faker
from utils import save_to_parquet

# Set environment variables for Databricks Volumes
import os
os.environ['CATALOG'] = 'demo_generator'
os.environ['SCHEMA'] = 'saswata_sengupta_agenticmdm'
os.environ['VOLUME'] = 'raw_data'



# Reproducibility
SEED = 42
np.random.seed(SEED)
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# Global time window
RANGE_START = pd.Timestamp('2025-05-01').floor('ms')
RANGE_END   = pd.Timestamp('2025-11-06').floor('ms')
EVENT_START = pd.Timestamp('2025-08-20').floor('ms')
EVENT_TRIGGER_BBG = pd.Timestamp('2025-08-19').floor('ms')
EVENT_TRIGGER_MOODY = pd.Timestamp('2025-08-25').floor('ms')
FIX_DATE = pd.Timestamp('2025-09-05').floor('ms')
RECOVERY_START = pd.Timestamp('2025-09-10').floor('ms')
EVENT_END = pd.Timestamp('2025-09-12').floor('ms')

DAYS = pd.date_range(RANGE_START.normalize(), RANGE_END.normalize(), freq='D')
DAYS = pd.to_datetime(DAYS, utc=False).tz_localize(None)

# Helper: probabilities choose

def _pchoice(values, probs, size):
    p = np.array(probs, dtype=float)
    p = p / p.sum()
    return np.random.choice(values, size=size, p=p)

# ID helpers

def gen_id(prefix: str, i: int, width: int) -> str:
    return f"{prefix}-{i:0{width}d}"

# Identifier helpers
ALPHANUM = string.ascii_uppercase + string.digits

def random_cusip():
    return ''.join(np.random.choice(list(ALPHANUM), size=9))

def random_isin(country='US'):
    country_code = country if len(country) == 2 else 'US'
    base = ''.join(np.random.choice(list(ALPHANUM), size=10))
    return country_code + base

def random_sedol():
    # 6 or 7 length
    l = np.random.choice([6, 7], p=[0.6, 0.4])
    return ''.join(np.random.choice(list(ALPHANUM), size=l))

# Bloomberg ticker formats
EMEA_SUFFIXES_PRE = [' LN', ' GR', ' NA', ' PA']  # pre-schema
EMEA_SUFFIXES_POST = ['.LN', '.GR', '.NA', '.PA']  # post-schema 2025-08-19

REGIONS = ['US', 'EMEA', 'APAC']
REGION_PROBS = [0.42, 0.38, 0.20]
VENDORS = ['FactSet', "Moodys", 'Bloomberg']
VENDOR_PROBS = [0.35, 0.20, 0.45]
SHARE_CLASSES = ['Common', 'Preferred', 'Class A', 'Class B', 'ADR']
SUB_PRODUCTS = ['ADR', 'ETF', 'ShareClass']

SECTOR_BASES = [f"MDY-SEC-24{i:02d}" for i in range(10, 40)]

# Security entity universe (canonical list used to produce ingest rows across vendors)

def build_security_universe(n_securities=9500, etf_ratio=0.08, adr_ratio=0.18):
    print(f"Building security universe ({n_securities:,} entities)...")
    # Assign region distribution non-flat
    regions = _pchoice(REGIONS, REGION_PROBS, n_securities)

    # Select ETFs subset and ADR flag
    is_etf = np.random.rand(n_securities) < etf_ratio
    is_adr = np.zeros(n_securities, dtype=bool)
    # ADR prevalent in EMEA
    emea_mask = regions == 'EMEA'
    is_adr[emea_mask] = np.random.rand(emea_mask.sum()) < (adr_ratio * 1.3)
    is_adr[~emea_mask] = np.random.rand((~emea_mask).sum()) < (adr_ratio * 0.6)

    # Share class selection for non-ETF
    share_class = np.empty(n_securities, dtype=object)
    non_etf_mask = ~is_etf
    share_class[non_etf_mask] = _pchoice(
        SHARE_CLASSES,
        [0.68, 0.06, 0.12, 0.08, 0.06],
        non_etf_mask.sum(),
    )
    # Normalize ADR flag and share_class coherence
    share_class[np.where(is_adr)] = 'ADR'

    # Identifiers per canonical security
    cusip = np.array([random_cusip() for _ in range(n_securities)], dtype=object)
    # Ensure US region tends to have valid CUSIP, others may have placeholder but we keep all strings
    us_mask = regions == 'US'
    # For non-US, reduce presence rate by putting synthetic but valid-looking values (we keep not-null strings as per contract)

    # ISIN by region prefix
    isin_prefix_map = {'US': 'US', 'EMEA': 'GB', 'APAC': 'JP'}
    isin = np.array([random_isin(isin_prefix_map[r]) for r in regions], dtype=object)

    sedol = np.array([random_sedol() for _ in range(n_securities)], dtype=object)

    # Issuer names
    issuers = np.array([fake.company() for _ in range(n_securities)], dtype=object)

    # Bloomberg base symbols (letters 3-5)
    base_sym = np.array([
        ''.join(np.random.choice(list(string.ascii_uppercase), size=np.random.randint(3, 6)))
        for _ in range(n_securities)
    ], dtype=object)

    # Compose tickers pre/post schema (applied at ingest-time later)

    # Sector code baseline (Moody's); will be reclassified after 2025-08-25 for EMEA ETFs
    sector_code = np.random.choice(SECTOR_BASES, size=n_securities)

    universe = pd.DataFrame({
        'security_id': [gen_id('SEC', i+1, 6) for i in range(n_securities)],
        'region': regions,
        'is_adr': is_adr,
        'is_etf': is_etf,
        'share_class': share_class,
        'cusip': cusip,
        'isin': isin,
        'sedol': sedol,
        'issuer_name': issuers,
        'base_symbol': base_sym,
        'sector_code': sector_code,
    })

    # Ensure CUSIP rule: US equities must have non-null CUSIP (we already assign)
    # Introduce tiny null rates on sedol/isin (<0.1%)
    small_n_null = max(1, int(n_securities * 0.001))
    null_idx_sedol = np.random.choice(n_securities, size=small_n_null, replace=False)
    null_idx_isin = np.random.choice(n_securities, size=small_n_null, replace=False)
    universe.loc[null_idx_sedol, 'sedol'] = None
    universe.loc[null_idx_isin, 'isin'] = None

    return universe


def generate_security_master_ingest(target_rows=312784) -> pd.DataFrame:
    print(f"Generating security_master_ingest (~{target_rows:,})...")
    universe = build_security_universe()

    # Vendor stream dynamics by day (weekday heavier)
    days = DAYS
    dow = days.weekday.values
    day_weight = np.where(dow < 5, np.random.uniform(1.0, 1.4, size=len(days)), np.random.uniform(0.35, 0.7, size=len(days)))

    # Event amplification for EMEA ADRs & ETFs via more conflicting ingestions
    day_mul = np.ones(len(days))
    for i, d in enumerate(days):
        dt = pd.Timestamp(d).floor('ms')
        if EVENT_START <= dt <= EVENT_END:
            day_mul[i] *= np.random.uniform(1.6, 2.2)
        elif dt > FIX_DATE and dt <= RECOVERY_START:
            day_mul[i] *= np.random.uniform(1.1, 1.3)

    base_daily = target_rows / len(days)
    per_day_counts = np.maximum(200, (base_daily * day_weight * day_mul).astype(int))

    records = []
    ingest_seq = 1
    progress_interval = max(1, len(days) // 10)

    # Precompute vendor probabilities across time with slight shift around event
    def vendor_probs_for_day(dt: pd.Timestamp):
        if dt >= EVENT_TRIGGER_BBG and dt <= EVENT_END:
            # Bloomberg share rises a bit during schema change
            return np.array([0.30, 0.18, 0.52])
        return np.array(VENDOR_PROBS)

    # Select a slightly smaller working set per day to avoid exploding dedupes
    n_uni = len(universe)

    for i, d in enumerate(days):
        if (i + 1) % progress_interval == 0 or i == 0:
            progress = ((i + 1) / len(days)) * 100
            print(f"  security_master_ingest: {progress:.0f}% ({i + 1:,}/{len(days):,})")

        dt = pd.Timestamp(d).floor('ms')
        n_rows = int(per_day_counts[i])

        # Sample securities with heavier EMEA ADR/ETF during event
        if EVENT_START <= dt <= EVENT_END:
            # Bias sampling
            emea_adr_etf = universe[(universe['region'] == 'EMEA') & (universe[['is_adr','is_etf']].any(axis=1))]
            weights = np.full(len(universe), 1.0 / n_uni)
            if len(emea_adr_etf) > 0:
                idx_bias = emea_adr_etf.index.values
                weights[idx_bias] = weights[idx_bias] * 6.0
                weights = weights / weights.sum()
            sample_idx = np.random.choice(n_uni, size=n_rows, replace=True, p=weights)
        else:
            sample_idx = np.random.choice(n_uni, size=n_rows, replace=True)

        uni_day = universe.iloc[sample_idx].copy().reset_index(drop=True)

        # Vendors per row
        vprobs = vendor_probs_for_day(dt)
        vendors = _pchoice(VENDORS, vprobs, n_rows)

        # Symbols and Bloomberg tickers with schema change on 2025-08-19 for EMEA
        base_sym = uni_day['base_symbol'].values.astype(object)
        region = uni_day['region'].values.astype(object)
        is_adr = uni_day['is_adr'].values
        is_etf = uni_day['is_etf'].values

        # Vendor-specific symbol forms
        factset_symbol = np.array([s + '-F' for s in base_sym], dtype=object)
        moodys_symbol = np.array([s + '-M' for s in base_sym], dtype=object)
        bloomberg_symbol = np.array([s for s in base_sym], dtype=object)

        symbol = np.empty(n_rows, dtype=object)
        symbol[vendors == 'FactSet'] = factset_symbol[vendors == 'FactSet']
        symbol[vendors == 'Moodys'] = moodys_symbol[vendors == 'Moodys']
        symbol[vendors == 'Bloomberg'] = bloomberg_symbol[vendors == 'Bloomberg']

        # Bloomberg ticker suffixing change (EMEA only)
        bb_mask = vendors == 'Bloomberg'
        emea_mask = region == 'EMEA'
        bb_emea_mask = bb_mask & emea_mask
        if bb_emea_mask.any():
            pre_mask = bb_emea_mask & (dt < EVENT_TRIGGER_BBG)
            post_mask = bb_emea_mask & (dt >= EVENT_TRIGGER_BBG)
            # Pre-schema: use space suffixes e.g., "VOD LN"; Post: dot suffixes "VOD.LN"
            pre_suf = np.random.choice(EMEA_SUFFIXES_PRE, size=pre_mask.sum())
            post_suf = np.random.choice(EMEA_SUFFIXES_POST, size=post_mask.sum())
            bloom_ticker = np.empty(n_rows, dtype=object)
            bloom_ticker[bb_mask] = base_sym[bb_mask]
            bloom_ticker[pre_mask] = (base_sym[pre_mask] + pre_suf)
            bloom_ticker[post_mask] = (base_sym[post_mask] + post_suf)
        else:
            bloom_ticker = np.where(bb_mask, base_sym, '')

        # For non-EMEA or non-BBG, simple ticker (may include US exchange like 'N' but we keep base)
        non_bbg_mask = ~bb_mask
        bloom_ticker[non_bbg_mask] = ''

        # For rows where vendor is not Bloomberg, fill bloomberg_ticker with vendor-symbol converted occasionally to mimic cross-vendor exposure
        # But keep it mostly empty except a small fraction to model cross-sourcing
        cross_fill_mask = (~bb_mask) & (np.random.rand(n_rows) < 0.05)
        bloom_ticker[cross_fill_mask] = base_sym[cross_fill_mask]

        # Identifier conflicts and ADR mapping disputes will be detectable by discordant IDs across vendors for same issuer/share_class during event
        cusip = uni_day['cusip'].values.astype(object)
        isin = uni_day['isin'].values.astype(object)
        sedol = uni_day['sedol'].values.astype(object)

        if EVENT_START <= dt <= EVENT_END:
            # Inject higher rate of discordance for EMEA ADRs & ETFs
            conflict_mask = (region == 'EMEA') & (is_adr | is_etf)
            flip = (np.random.rand(n_rows) < 0.35) & conflict_mask
            # Change one of the identifiers randomly to create mismatch
            if flip.any():
                which = np.random.choice(['cusip', 'isin', 'sedol'], size=flip.sum())
                for j, w in enumerate(which):
                    idx = np.where(flip)[0][j]
                    if w == 'cusip':
                        cusip[idx] = random_cusip()
                    elif w == 'isin':
                        # Change prefix or body
                        pref = 'GB' if region[idx] == 'EMEA' else 'US'
                        isin[idx] = random_isin(pref)
                    else:
                        sedol[idx] = random_sedol()
        elif dt < EVENT_START:
            # Baseline small discordance (~1%)
            flip = np.random.rand(n_rows) < 0.01
            if flip.any():
                idxs = np.where(flip)[0]
                for idx in idxs:
                    sedol[idx] = random_sedol()
        else:
            # Recovery: still minor residual
            flip = np.random.rand(n_rows) < 0.02
            if flip.any():
                idxs = np.where(flip)[0]
                for idx in idxs:
                    isin[idx] = random_isin('GB' if region[idx] == 'EMEA' else 'US')

        # Sector reclassification for Moody's on 2025-08-25 impacting EMEA ETFs
        if dt >= EVENT_TRIGGER_MOODY:
            m_mask = (vendors == 'Moodys') & (region == 'EMEA') & is_etf
            if m_mask.any():
                # Reclassify to a new sector code range 'MDY-SEC-25xx'
                new_codes = [f"MDY-SEC-25{np.random.randint(10, 99):02d}" for _ in range(m_mask.sum())]
                uni_day.loc[m_mask, 'sector_code'] = new_codes

        # issuer_name slight variations by vendor
        issuer = uni_day['issuer_name'].values.astype(object)
        issuer_vendor = issuer.copy()
        # introduce small suffix changes by vendor
        # No-op for FactSet: ensure dtype is consistent without using np.char.add (object vs unicode issue)
        issuer_vendor[vendors == 'FactSet'] = issuer_vendor[vendors == 'FactSet'].astype(object)
        issuer_vendor_m = issuer_vendor[vendors == 'Moodys'].astype(str)
        issuer_vendor_m = np.char.replace(issuer_vendor_m, ' Inc', ' Incorporated')
        issuer_vendor[vendors == 'Moodys'] = issuer_vendor_m
        issuer_vendor_b = issuer_vendor[vendors == 'Bloomberg'].astype(str)
        issuer_vendor_b = np.char.replace(issuer_vendor_b, ' PLC', ' Plc')
        issuer_vendor[vendors == 'Bloomberg'] = issuer_vendor_b

        # ingest_time within day, business hours heavier on weekdays
        if dt.weekday() < 5:
            hour_probs = np.array([
                0.005, 0.005, 0.005, 0.005, 0.008, 0.015, 0.03, 0.05, 0.07, 0.08, 0.09, 0.09,
                0.09, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.025, 0.02, 0.015, 0.01, 0.008
            ])
        else:
            hour_probs = np.array([
                0.01, 0.01, 0.01, 0.01, 0.012, 0.02, 0.03, 0.045, 0.06, 0.065, 0.07, 0.07,
                0.065, 0.06, 0.055, 0.05, 0.045, 0.04, 0.035, 0.03, 0.025, 0.02, 0.015, 0.01
            ])
        hour_probs = hour_probs / hour_probs.sum()
        hours = np.random.choice(np.arange(24), size=n_rows, p=hour_probs)
        minutes = np.random.randint(0, 60, size=n_rows)
        ingest_time = (dt.normalize() + pd.to_timedelta(hours, unit='h') + pd.to_timedelta(minutes, unit='m')).floor('ms')

        # source_record_hash deterministic style: combine vendor+ids
        base_hash = pd.util.hash_pandas_object(pd.DataFrame({
            'v': vendors,
            'i': uni_day['isin'].values,
            'c': uni_day['cusip'].values,
            's': uni_day['sedol'].values,
            't': symbol,
        }), index=False).astype(np.int64)
        source_record_hash = np.array([f"SRC-{abs(x):016d}" for x in base_hash], dtype=object)

        # Compose rows
        df_day = pd.DataFrame({
            'ingest_id': [gen_id('ING', ingest_seq + k, 7) for k in range(n_rows)],
            'ingest_time': ingest_time,
            'vendor': vendors,
            'symbol': symbol,
            'cusip': cusip,
            'isin': isin,
            'sedol': sedol,
            'bloomberg_ticker': bloom_ticker,
            'issuer_name': issuer_vendor,
            'share_class': uni_day['share_class'].values,
            'is_adr': uni_day['is_adr'].values.astype(bool),
            'is_etf': uni_day['is_etf'].values.astype(bool),
            'sector_code': uni_day['sector_code'].values,
            'region': uni_day['region'].values,
            'source_record_hash': source_record_hash,
        })
        ingest_seq += n_rows
        records.append(df_day)

    df = pd.concat(records, ignore_index=True)

    # Final normalization of datetime
    df['ingest_time'] = pd.to_datetime(df['ingest_time'], errors='coerce').dt.tz_localize(None).dt.floor('ms')

    # Shuffle slightly for realism
    df = df.sample(frac=1.0, random_state=SEED).reset_index(drop=True)

    # Ensure schema dtypes
    df['is_adr'] = df['is_adr'].astype(bool)
    df['is_etf'] = df['is_etf'].astype(bool)

    print(f"security_master_ingest rows: {len(df):,}")
    return df


def generate_etf_constituents(security_master: pd.DataFrame, target_rows=184262) -> pd.DataFrame:
    print(f"Generating etf_constituents (~{target_rows:,})...")
    # Build ETF universe ISINs from security_master universe (unique by isin for is_etf True)
    # We derive canonical by latest per ISIN from security_master
    sm = security_master[['isin', 'is_etf', 'region']].dropna(subset=['isin'])
    sm['is_etf'] = sm['is_etf'].astype(bool)
    etfs = sm[sm['is_etf']].drop_duplicates('isin')
    non_etfs = sm[~sm['is_etf']].drop_duplicates('isin')

    etf_isins = etfs['isin'].values
    etf_regions = etfs['region'].values

    if len(etf_isins) == 0:
        raise ValueError('No ETF ISINs found in security_master to seed constituents')

    # For each day, sample a subset of ETFs and assign 20-80 constituents with weights summing ~1
    days = DAYS
    rows = []
    progress_interval = max(1, len(days) // 10)

    # Pre-choose some ETF sizes
    rng = np.random.default_rng(SEED)

    for i, d in enumerate(days):
        if (i + 1) % progress_interval == 0 or i == 0:
            progress = ((i + 1) / len(days)) * 100
            print(f"  etf_constituents: {progress:.0f}% ({i + 1:,}/{len(days):,})")
        date = pd.Timestamp(d).normalize().floor('ms')

        # Sample ~15% ETFs daily (more on month-end)
        month_end_boost = 0.25 if date.is_month_end else 0.0
        pick_rate = 0.12 + month_end_boost
        pick_mask = np.random.rand(len(etf_isins)) < pick_rate
        if not pick_mask.any():
            continue
        picked_etfs = etf_isins[pick_mask]
        picked_regions = etf_regions[pick_mask]

        for j, etf_isin in enumerate(picked_etfs):
            region = picked_regions[j]
            # Constituents from same region primarily
            pool = non_etfs[non_etfs['region'] == region]['isin'].values
            if len(pool) < 50:
                pool = non_etfs['isin'].values
            n_const = int(np.clip(rng.normal(60, 15), 20, 120))
            consts = np.random.choice(pool, size=n_const, replace=False)
            # Dirichlet weights heavy-tail
            alpha = np.ones(n_const) * 0.7
            w = rng.dirichlet(alpha)
            # Introduce heavier top weights
            w_sorted_idx = np.argsort(w)[::-1]
            w[w_sorted_idx[:3]] *= 1.8
            w = w / w.sum()

            vendor = _pchoice(['FactSet', 'Bloomberg'], [0.55, 0.45], 1)[0]

            df_chunk = pd.DataFrame({
                'constituent_id': [gen_id('EC', int(rng.integers(1, 10_000_000)), 6) for _ in range(n_const)],
                'etf_isin': etf_isin,
                'constituent_isin': consts,
                'effective_date': [date] * n_const,
                'weight': w.astype(float),
                'vendor': vendor,
                'region': [region] * n_const,
            })
            rows.append(df_chunk)

    # Concat rows safely
    if len(rows) == 0:
        df = pd.DataFrame(columns=['constituent_id','etf_isin','constituent_isin','effective_date','weight','vendor','region'])
    else:
        df = pd.concat(rows, ignore_index=True)

    # Downsample or upsample to target_rows by sampling
    if len(df) > target_rows:
        df = df.sample(n=target_rows, random_state=SEED).reset_index(drop=True)
    elif len(df) < target_rows:
        # Repeat some rows with tiny noise to reach target (safe for RAW)
        need = target_rows - len(df)
        add = df.sample(n=need, replace=True, random_state=SEED).reset_index(drop=True)
        df = pd.concat([df, add], ignore_index=True)

    # Ensure dtypes
    df['effective_date'] = pd.to_datetime(df['effective_date'], errors='coerce').dt.floor('ms')

    print(f"etf_constituents rows: {len(df):,}")
    return df


def generate_vendor_change_log(target_rows=147) -> pd.DataFrame:
    print(f"Generating vendor_change_log (~{target_rows:,})...")
    rows = []

    # Pre-fill with regular weekly-ish entries across the range
    dates = pd.date_range(RANGE_START, RANGE_END, freq='7D')
    for d in dates:
        dt = pd.Timestamp(d).tz_localize(None).normalize() + pd.Timedelta(hours=int(np.random.uniform(8, 18)))
        vendor = _pchoice(['Bloomberg', 'FactSet', "Moody's", 'Internal-MDM'], [0.3, 0.3, 0.25, 0.15], 1)[0]
        change_type = _pchoice(['schema_refresh', 'taxonomy_update', 'correction', 'rule_update'], [0.25, 0.25, 0.35, 0.15], 1)[0]
        scope_region = _pchoice(['US', 'EMEA', 'APAC', 'global'], [0.25, 0.35, 0.15, 0.25], 1)[0]
        module = _pchoice(['Identifiers', 'Sectors', 'ADR mapping', 'ETF composition'], [0.35, 0.25, 0.20, 0.20], 1)[0]
        code = _pchoice(['MDM-ID-3102', 'MDM-LIN-2207', 'MDM-COM-1905'], [0.5, 0.3, 0.2], 1)[0]
        note = f"Routine {change_type}; monitors: {code}"
        cid = f"CHG-{dt.strftime('%Y%m%d')}-{vendor[:3].upper()}"
        rows.append(pd.DataFrame([{  # ensure consistent concat types
            'change_id': cid,
            'vendor': vendor,
            'change_date': dt.floor('ms'),
            'change_type': change_type,
            'scope_region': scope_region,
            'module': module,
            'notes': note,
        }]))

    # Key events
    rows.append(pd.DataFrame([{  # keep DataFrame type consistent
        'change_id': 'CHG-20250819-BBG',
        'vendor': 'Bloomberg',
        'change_date': (EVENT_TRIGGER_BBG.normalize() + pd.Timedelta(hours=9)).floor('ms'),
        'change_type': 'schema_refresh',
        'scope_region': 'EMEA',
        'module': 'Identifiers',
        'notes': 'Bloomberg ticker schema refresh; EMEA suffixing (.LN, .GR, .HK); codes MDM-ID-3102, MDM-LIN-2207',
    }]))
    rows.append(pd.DataFrame([{  # keep DataFrame type consistent
        'change_id': 'CHG-20250825-MDY',
        'vendor': "Moody's",
        'change_date': (EVENT_TRIGGER_MOODY.normalize() + pd.Timedelta(hours=11)).floor('ms'),
        'change_type': 'taxonomy_update',
        'scope_region': 'EMEA',
        'module': 'Sectors',
        'notes': "Moody's sector reclassification for EMEA ETFs; codes MDM-LIN-2207, MDM-COM-1905",
    }]))
    rows.append(pd.DataFrame([{  # keep DataFrame type consistent
        'change_id': 'CHG-20250905-MDM',
        'vendor': 'Internal-MDM',
        'change_date': (FIX_DATE.normalize() + pd.Timedelta(hours=10)).floor('ms'),
        'change_type': 'rule_update',
        'scope_region': 'global',
        'module': 'ADR mapping',
        'notes': 'Northbridge MDM rule update: stricter cross-id concordance; tie-out ADRs; resolves MDM-ID-3102',
    }]))

    # Concat rows safely for vendor_change_log
    if len(rows) == 0:
        df = pd.DataFrame(columns=['change_id','vendor','change_date','change_type','scope_region','module','notes'])
    else:
        df = pd.concat(rows, ignore_index=True)
    # If more than target_rows, sample
    if len(df) > target_rows:
        df = df.sample(n=target_rows, random_state=SEED).sort_values('change_date').reset_index(drop=True)
    elif len(df) < target_rows:
        need = target_rows - len(df)
        add = df.sample(n=need, replace=True, random_state=SEED).reset_index(drop=True)
        df = pd.concat([df, add], ignore_index=True)
        df = df.sort_values('change_date').reset_index(drop=True)

    df['change_date'] = pd.to_datetime(df['change_date'], errors='coerce').dt.floor('ms')
    print(f"vendor_change_log rows: {len(df):,}")
    return df


def generate_steward_commentary(security_master: pd.DataFrame, target_rows=1004) -> pd.DataFrame:
    print(f"Generating steward_commentary (~{target_rows:,})...")
    # Link to security_master source_record_hash
    sm = security_master[['source_record_hash', 'region', 'ingest_time']].copy()
    # ensure tz-naive ms precision
    sm['ingest_time'] = pd.to_datetime(sm['ingest_time'], errors='coerce')
    if isinstance(sm['ingest_time'], pd.Series):
        sm['ingest_time'] = sm['ingest_time'].dt.tz_localize(None).dt.floor('ms')
    else:
        sm['ingest_time'] = pd.to_datetime(sm['ingest_time']).tz_localize(None).floor('ms')

    # Prefer EMEA and event cluster after 2025-08-25
    weights = np.ones(len(sm))
    is_emea = sm['region'] == 'EMEA'
    post_moody = sm['ingest_time'] >= EVENT_TRIGGER_MOODY
    weights[(is_emea) & (post_moody)] *= 8.0
    weights[(is_emea) & (sm['ingest_time'].between(EVENT_START, EVENT_END))] *= 3.0
    weights = weights / weights.sum()

    chosen_idx = np.random.choice(len(sm), size=target_rows, replace=False, p=weights)
    chosen = sm.iloc[chosen_idx].reset_index(drop=True)

    # Attributes and approvals
    attributes = ['sector_code', 'share_class', 'is_adr', 'symbol', 'bloomberg_ticker', 'issuer_name']
    approval_status = _pchoice(['approved', 'proposed', 'rejected'], [0.68, 0.22, 0.10], target_rows)
    users = [f"U-{i:03d}" for i in range(1, 201)]

    # comment_time around ingest_time +/- some hours; cluster after EVENT_TRIGGER_MOODY
    base_times = chosen['ingest_time'].values
    jitter_hours = np.random.normal(loc=8, scale=6, size=target_rows)
    comment_time = pd.to_datetime(pd.Series(base_times)).dt.tz_localize(None) + pd.to_timedelta(jitter_hours, unit='h')
    # Ensure within global range
    comment_time = comment_time.clip(lower=RANGE_START, upper=RANGE_END).dt.floor('ms')

    # old/new values placeholders (strings)
    attr = _pchoice(attributes, [0.25, 0.10, 0.12, 0.20, 0.23, 0.10], target_rows)
    old_value = np.array(['' for _ in range(target_rows)], dtype=object)
    new_value = np.array(['' for _ in range(target_rows)], dtype=object)

    # Fill some realistic changes
    for i in range(target_rows):
        a = attr[i]
        if a == 'sector_code':
            old_value[i] = f"MDY-SEC-24{np.random.randint(10,99):02d}"
            new_value[i] = f"MDY-SEC-25{np.random.randint(10,99):02d}"
        elif a == 'share_class':
            old_value[i] = _pchoice(SHARE_CLASSES, [0.6,0.05,0.15,0.1,0.1], 1)[0]
            new_value[i] = _pchoice(SHARE_CLASSES, [0.7,0.05,0.12,0.08,0.05], 1)[0]
        elif a == 'is_adr':
            old_value[i] = str(_pchoice([True, False], [0.5, 0.5], 1)[0])
            new_value[i] = str(old_value[i] != 'True')
        elif a == 'symbol':
            old_value[i] = fake.bothify(text='????-#')
            new_value[i] = fake.bothify(text='????-#')
        elif a == 'bloomberg_ticker':
            old_value[i] = fake.bothify(text='????.??')
            new_value[i] = fake.bothify(text='???? ??')
        elif a == 'issuer_name':
            old_value[i] = fake.company()
            new_value[i] = old_value[i].replace(' Inc', ' Incorporated')

    reason_codes = _pchoice(['MDM-COM-1905', 'MDM-OVR-3301', 'MDM-OVR-3302'], [0.6, 0.25, 0.15], target_rows)
    users_pick = _pchoice(users, [1/len(users)]*len(users), target_rows)

    df = pd.DataFrame({
        'comment_id': [gen_id('COM', i+1, 6) for i in range(target_rows)],
        'source_record_hash': chosen['source_record_hash'].values,
        'attribute_name': attr,
        'old_value': old_value,
        'new_value': new_value,
        'user_id': users_pick,
        'approval_status': approval_status,
        'comment_time': comment_time,
        'region': chosen['region'].values,
        'reason_code': reason_codes,
    })

    # Normalize timestamp
    df['comment_time'] = pd.to_datetime(df['comment_time'], errors='coerce').dt.floor('ms')

    print(f"steward_commentary rows: {len(df):,}")
    return df


def generate_holdings_report(target_rows=226418) -> pd.DataFrame:
    print(f"Generating holdings_report (~{target_rows:,})...")
    # Build portfolios
    n_portfolios = 180  # PF-001..PF-180
    portfolio_ids = [f"PF-{i:03d}" for i in range(1, n_portfolios + 1)]
    regions = _pchoice(REGIONS, REGION_PROBS, n_portfolios)

    portfolios = pd.DataFrame({'portfolio_id': portfolio_ids, 'region': regions})

    # Daily rows per portfolio for selected sectors and sub_products
    days = DAYS
    sectors = SECTOR_BASES.copy()

    rows = []
    progress_interval = max(1, len(days) // 10)

    for di, d in enumerate(days):
        if (di + 1) % progress_interval == 0 or di == 0:
            progress = ((di + 1) / len(days)) * 100
            print(f"  holdings_report: {progress:.0f}% ({di + 1:,}/{len(days):,})")
        date = pd.Timestamp(d).normalize().floor('ms')
        in_event = (date >= EVENT_START) and (date <= EVENT_END)

        # Sub-products distribution
        sub_prod_probs = np.array([0.25, 0.30, 0.45])  # ADR, ETF, ShareClass baseline
        if in_event:
            sub_prod_probs = np.array([0.35, 0.35, 0.30])  # tilt ADR/ETF
        sub_prod_probs = sub_prod_probs / sub_prod_probs.sum()

        # For each portfolio generate 3-6 sector exposures
        n_rows_day = []
        for pi in range(n_portfolios):
            region = portfolios.loc[pi, 'region']
            # Pick 3-6 sectors
            n_sec = np.random.randint(3, 7)
            sec_pick = np.random.choice(sectors, size=n_sec, replace=False)
            sub_prod = _pchoice(SUB_PRODUCTS, sub_prod_probs, n_sec)

            # Exposure: log-normal with heavy tails
            base = np.random.lognormal(mean=12.0, sigma=0.8, size=n_sec)  # up to hundreds of millions
            # Scale down for non-EMEA slightly
            if region != 'EMEA':
                base *= np.random.uniform(0.6, 0.9)

            # Misclassification logic: EMEA + (ADR/ETF) during 08-20..09-12 when golden_resolved=False
            golden_resolved = np.ones(n_sec, dtype=bool)
            if date < RECOVERY_START and date >= EVENT_TRIGGER_MOODY:
                # Higher rate of unresolved in event window and shortly after
                unresolved_rate = 0.55 if in_event else 0.25
                unresolved = np.random.rand(n_sec) < unresolved_rate
                golden_resolved[unresolved] = False

            # Additional boost to misclassified exposure to approach ~$175M total over the period
            # We do not compute totals here; we let later gold compute misclassified sums where golden_resolved=False

            df_chunk = pd.DataFrame({
                'portfolio_id': np.repeat(portfolios.loc[pi, 'portfolio_id'], n_sec),
                'date': np.repeat(date, n_sec),
                'region': np.repeat(region, n_sec),
                'sector_code': sec_pick,
                'exposure_usd': base.astype(float),
                'golden_resolved': golden_resolved.astype(bool),
                'sub_product': sub_prod,
            })
            rows.append(df_chunk)

    # Concat rows safely for holdings_report
    if len(rows) == 0:
        df = pd.DataFrame(columns=['portfolio_id','date','region','sector_code','exposure_usd','golden_resolved','sub_product'])
    else:
        df = pd.concat(rows, ignore_index=True)

    # Down/up sample to target_rows
    if len(df) > target_rows:
        df = df.sample(n=target_rows, random_state=SEED).reset_index(drop=True)
    elif len(df) < target_rows:
        need = target_rows - len(df)
        add = df.sample(n=need, replace=True, random_state=SEED).reset_index(drop=True)
        df = pd.concat([df, add], ignore_index=True)

    # Normalize types
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.floor('ms')
    df['golden_resolved'] = df['golden_resolved'].astype(bool)

    print(f"holdings_report rows: {len(df):,}")
    return df


if __name__ == "__main__":
    print("Starting AgenticMDM data generation...")
    print("-" * 60)

    # 1) security_master_ingest
    smi = generate_security_master_ingest(target_rows=312784)
    save_to_parquet(smi, "security_master_ingest", num_files=10)

    # 2) etf_constituents (requires ISINs from smi)
    etf = generate_etf_constituents(smi, target_rows=184262)
    save_to_parquet(etf, "etf_constituents", num_files=8)

    # 3) vendor_change_log
    vcl = generate_vendor_change_log(target_rows=147)
    save_to_parquet(vcl, "vendor_change_log", num_files=1)

    # 4) steward_commentary (linked to source_record_hash from smi)
    com = generate_steward_commentary(smi, target_rows=1004)
    save_to_parquet(com, "steward_commentary", num_files=1)

    # 5) holdings_report
    hold = generate_holdings_report(target_rows=226418)
    save_to_parquet(hold, "holdings_report", num_files=8)

    # QA Summary
    print("\nQA Summary:")
    # Check event window volume differences in security_master_ingest
    smi['date_only'] = pd.to_datetime(smi['ingest_time'], errors='coerce').dt.tz_localize(None).dt.normalize()
    pre = smi[(smi['date_only'] >= pd.Timestamp('2025-08-10').tz_localize(None)) & (smi['date_only'] < EVENT_START.tz_localize(None))]
    event = smi[(smi['date_only'] >= EVENT_START.tz_localize(None)) & (smi['date_only'] <= EVENT_END.tz_localize(None))]
    post = smi[(smi['date_only'] >= RECOVERY_START.tz_localize(None)) & (smi['date_only'] <= (RECOVERY_START + pd.Timedelta(days=7)).tz_localize(None))]
    print(f"  security_master_ingest daily avg pre-event: {len(pre)/max(1,(pre['date_only'].nunique())):,.0f}")
    print(f"  security_master_ingest daily avg event:    {len(event)/max(1,(event['date_only'].nunique())):,.0f}")
    print(f"  security_master_ingest daily avg post:     {len(post)/max(1,(post['date_only'].nunique())):,.0f}")

    # Steward commentary EMEA share
    emea_share = (com['region'] == 'EMEA').mean()
    print(f"  steward_commentary EMEA share: {emea_share:.2f}")

    # Holdings unresolved rate during event vs pre
    hold['date_only'] = pd.to_datetime(hold['date'], errors='coerce').dt.tz_localize(None).dt.normalize()
    h_pre = hold[(hold['date_only'] >= pd.Timestamp('2025-08-10').tz_localize(None)) & (hold['date_only'] < EVENT_START.tz_localize(None))]
    h_event = hold[(hold['date_only'] >= EVENT_START.tz_localize(None)) & (hold['date_only'] <= EVENT_END.tz_localize(None))]
    print(f"  holdings_report golden_resolved pre-event:  {(h_pre['golden_resolved'].mean() if len(h_pre)>0 else np.nan):.2f}")
    print(f"  holdings_report golden_resolved in-event:   {(h_event['golden_resolved'].mean() if len(h_event)>0 else np.nan):.2f}")

    print("\nGeneration complete - All timestamps are naive (no tz) and floored to ms.")
