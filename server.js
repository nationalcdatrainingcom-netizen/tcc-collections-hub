require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

// Stripe is optional — only used for the Collections/Pay-What-You-Can feature.
// If STRIPE_SECRET_KEY isn't set, the Collections UI still works but payment
// links won't be generated and webhook events won't be recorded.
let stripe = null;
try {
  const Stripe = require('stripe');
  if (process.env.STRIPE_SECRET_KEY) {
    stripe = Stripe(process.env.STRIPE_SECRET_KEY);
    console.log('Stripe initialized');
  } else {
    console.log('Stripe disabled (STRIPE_SECRET_KEY not set)');
  }
} catch (e) {
  console.log('Stripe module not installed — Collections feature will be read-only');
}

const app = express();
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 }, // 25MB — Playground CSVs with signatures can be large
});
const JWT_SECRET = process.env.HUB_JWT_SECRET || 'tcc-hub-jwt-2026';

app.use(cors());

// ── Stripe webhook — must use raw body BEFORE express.json() ────────────────
// Stripe signs the raw request body; if we JSON-parse first we can't verify.
app.post('/api/collections/webhook',
  express.raw({ type: 'application/json' }),
  async (req, res) => {
    if (!stripe) return res.status(503).send('Stripe not configured');
    const sig = req.headers['stripe-signature'];
    const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;

    let event;
    try {
      if (webhookSecret) {
        event = stripe.webhooks.constructEvent(req.body, sig, webhookSecret);
      } else {
        // No secret configured — parse without verification (ok for local dev only)
        event = JSON.parse(req.body.toString());
        console.warn('Stripe webhook received WITHOUT signature verification (set STRIPE_WEBHOOK_SECRET)');
      }
    } catch (err) {
      console.error('Webhook signature verification failed:', err.message);
      return res.status(400).send(`Webhook Error: ${err.message}`);
    }

    try {
      await handleStripeEvent(event);
      res.json({ received: true });
    } catch (e) {
      console.error('Webhook handler failed:', e);
      res.status(500).json({ error: e.message });
    }
  }
);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── SSO Middleware ──────────────────────────────────────────────────────────
// ALLOW_DIRECT_ACCESS (default true) lets Mary hit the app without going
// through the TCC Hub. If a token is present and valid it's used; otherwise
// requests are authorized as the owner. Set ALLOW_DIRECT_ACCESS=false in Render
// once Hub SSO is wired up for billing coordinators.
const ALLOW_DIRECT_ACCESS = (process.env.ALLOW_DIRECT_ACCESS || 'true').toLowerCase() === 'true';
const DIRECT_ACCESS_USER = { email: 'mary@childrenscenterinc.com', name: 'Mary Wardlaw', role: 'owner', direct: true };

function ssoAuth(req, res, next) {
  const token = req.query.token || req.headers['authorization']?.replace('Bearer ', '');
  if (token && token !== 'null' && token !== 'undefined') {
    try {
      req.user = jwt.verify(token, JWT_SECRET);
      return next();
    } catch (e) {
      if (ALLOW_DIRECT_ACCESS) {
        req.user = DIRECT_ACCESS_USER;
        return next();
      }
      return res.status(401).json({ error: 'Invalid token' });
    }
  }
  if (ALLOW_DIRECT_ACCESS) {
    req.user = DIRECT_ACCESS_USER;
    return next();
  }
  return res.status(401).json({ error: 'Unauthorized' });
}

// Public endpoint so frontend can know whether direct access is allowed
app.get('/api/auth/mode', (req, res) => {
  res.json({ allowDirectAccess: ALLOW_DIRECT_ACCESS, user: ALLOW_DIRECT_ACCESS ? DIRECT_ACCESS_USER : null });
});

// ── DB Init ─────────────────────────────────────────────────────────────────
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS children (
      id SERIAL PRIMARY KEY,
      center TEXT NOT NULL,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      subsidy_type TEXT DEFAULT 'private_pay',
      is_gsrp BOOLEAN DEFAULT FALSE,
      is_cdc BOOLEAN DEFAULT FALSE,
      is_school_age BOOLEAN DEFAULT FALSE,
      is_active BOOLEAN DEFAULT TRUE,
      inactive_date DATE,
      enrolled_hours_per_day NUMERIC(4,2) DEFAULT 9.00,
      enrolled_days TEXT DEFAULT 'M,T,W,Th,F',
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(center, first_name, last_name)
    );

    -- Add columns if the table already existed (safe on reruns)
    ALTER TABLE children ADD COLUMN IF NOT EXISTS inactive_date DATE;
    ALTER TABLE children ADD COLUMN IF NOT EXISTS enrolled_hours_per_day NUMERIC(4,2) DEFAULT 9.00;
    ALTER TABLE children ADD COLUMN IF NOT EXISTS enrolled_days TEXT DEFAULT 'M,T,W,Th,F';
    ALTER TABLE children ADD COLUMN IF NOT EXISTS child_id_number TEXT;
    ALTER TABLE children ADD COLUMN IF NOT EXISTS case_number TEXT;
    ALTER TABLE children ADD COLUMN IF NOT EXISTS authorized_hours NUMERIC(6,2);
    ALTER TABLE children ADD COLUMN IF NOT EXISTS cdc_start_date DATE;
    ALTER TABLE children ADD COLUMN IF NOT EXISTS cdc_end_date TEXT DEFAULT 'ONGOING';
    ALTER TABLE children ADD COLUMN IF NOT EXISTS family_contribution NUMERIC(10,2) DEFAULT 0;
    ALTER TABLE children ADD COLUMN IF NOT EXISTS child_care_fees NUMERIC(10,2) DEFAULT 0;

    CREATE TABLE IF NOT EXISTS cdc_periods (
      id SERIAL PRIMARY KEY,
      period_number TEXT NOT NULL,
      center TEXT NOT NULL,
      child_id INTEGER REFERENCES children(id),
      child_name TEXT,
      billed BOOLEAN DEFAULT FALSE,
      amount_paid NUMERIC(10,2),
      applied_to_account BOOLEAN DEFAULT FALSE,
      status TEXT DEFAULT 'pending',
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS attendance_records (
      id SERIAL PRIMARY KEY,
      center TEXT NOT NULL,
      child_id INTEGER REFERENCES children(id),
      child_first TEXT,
      child_last TEXT,
      attend_date DATE NOT NULL,
      checkin_time TIME,
      checkout_time TIME,
      is_absent BOOLEAN DEFAULT FALSE,
      raw_row JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS billing_flags (
      id SERIAL PRIMARY KEY,
      center TEXT NOT NULL,
      child_id INTEGER REFERENCES children(id),
      child_name TEXT,
      flag_type TEXT NOT NULL,
      flag_detail TEXT,
      period_number TEXT,
      attend_date DATE,
      resolved BOOLEAN DEFAULT FALSE,
      resolved_at TIMESTAMPTZ,
      resolved_by TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS no_school_days (
      id SERIAL PRIMARY KEY,
      center TEXT,
      school_date DATE NOT NULL,
      reason TEXT,
      applies_to TEXT DEFAULT 'all',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS gsrp_late_pickups (
      id SERIAL PRIMARY KEY,
      center TEXT NOT NULL,
      child_id INTEGER REFERENCES children(id),
      child_name TEXT,
      pickup_date DATE,
      scheduled_out TIME,
      actual_out TIME,
      minutes_late INTEGER,
      grace_used BOOLEAN DEFAULT FALSE,
      fee_charged NUMERIC(10,2) DEFAULT 0,
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS tuition_rates (
      id SERIAL PRIMARY KEY,
      center TEXT NOT NULL,
      rate_type TEXT NOT NULL,
      label TEXT NOT NULL,
      weekly_amount NUMERIC(10,2),
      hourly_rate NUMERIC(10,2) DEFAULT 8.00,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS billing_periods (
      id SERIAL PRIMARY KEY,
      period_number TEXT UNIQUE NOT NULL,
      start_date DATE,
      end_date DATE,
      reporting_deadline DATE,
      deadline_is_4pm BOOLEAN DEFAULT FALSE,
      payment_date DATE,
      payment_delayed_holiday BOOLEAN DEFAULT FALSE,
      billing_status TEXT DEFAULT 'pending',
      crediting_status TEXT DEFAULT 'pending'
    );

    ALTER TABLE billing_periods ADD COLUMN IF NOT EXISTS deadline_is_4pm BOOLEAN DEFAULT FALSE;
    ALTER TABLE billing_periods ADD COLUMN IF NOT EXISTS payment_delayed_holiday BOOLEAN DEFAULT FALSE;

    -- ── Collections: families who left with a balance, or are very past-due ──
    CREATE TABLE IF NOT EXISTS collections_families (
      id SERIAL PRIMARY KEY,
      family_name TEXT NOT NULL,
      primary_contact_email TEXT,
      primary_contact_phone TEXT,
      children_names TEXT,
      original_balance NUMERIC(10,2) NOT NULL,
      center TEXT,
      left_date DATE,
      status TEXT DEFAULT 'active',
      notes TEXT,
      payinfull_link_url TEXT,
      payinfull_link_id TEXT,
      paywhatyoucan_link_url TEXT,
      paywhatyoucan_link_id TEXT,
      settled_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS collections_payments (
      id SERIAL PRIMARY KEY,
      family_id INTEGER REFERENCES collections_families(id) ON DELETE CASCADE,
      stripe_payment_intent_id TEXT UNIQUE,
      stripe_charge_id TEXT,
      stripe_customer_email TEXT,
      amount NUMERIC(10,2) NOT NULL,
      amount_refunded NUMERIC(10,2) DEFAULT 0,
      currency TEXT DEFAULT 'usd',
      status TEXT NOT NULL,
      failure_reason TEXT,
      paid_at TIMESTAMPTZ,
      link_type TEXT,
      raw_event JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS collections_events (
      id SERIAL PRIMARY KEY,
      family_id INTEGER REFERENCES collections_families(id) ON DELETE CASCADE,
      event_type TEXT NOT NULL,
      stripe_event_id TEXT UNIQUE,
      detail TEXT,
      amount NUMERIC(10,2),
      raw_event JSONB,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Seed billing periods from the official MiLEAP CDC 2026 Payment Schedule.
  // Columns: [period, start, end, reporting_deadline, deadline_is_4pm, payment_date, payment_delayed_holiday]
  // The 4pm flag means the deadline falls on a day before a holiday — submissions close at 4pm, not midnight.
  // The holiday flag means the check/EFT was delayed because of a state holiday.
  // ON CONFLICT DO UPDATE ensures any previously-seeded wrong dates get corrected on next deploy.
  const periods = [
    ['601','2025-12-28','2026-01-10','2026-01-15', false, '2026-01-23', true ],
    ['602','2026-01-11','2026-01-24','2026-01-29', false, '2026-02-05', false],
    ['603','2026-01-25','2026-02-07','2026-02-12', false, '2026-02-20', true ],
    ['604','2026-02-08','2026-02-21','2026-02-26', false, '2026-03-05', false],
    ['605','2026-02-22','2026-03-07','2026-03-12', false, '2026-03-19', false],
    ['606','2026-03-08','2026-03-21','2026-03-26', false, '2026-04-02', false],
    ['607','2026-03-22','2026-04-04','2026-04-09', false, '2026-04-16', false],
    ['608','2026-04-05','2026-04-18','2026-04-23', false, '2026-04-30', false],
    ['609','2026-04-19','2026-05-02','2026-05-07', false, '2026-05-14', false],
    ['610','2026-05-03','2026-05-16','2026-05-21', false, '2026-05-29', true ],
    ['611','2026-05-17','2026-05-30','2026-06-04', false, '2026-06-11', false],
    ['612','2026-05-31','2026-06-13','2026-06-17', true,  '2026-06-25', false],
    ['613','2026-06-14','2026-06-27','2026-07-01', true,  '2026-07-09', false],
    ['614','2026-06-28','2026-07-11','2026-07-16', false, '2026-07-23', false],
    ['615','2026-07-12','2026-07-25','2026-07-30', false, '2026-08-06', false],
    ['616','2026-07-26','2026-08-08','2026-08-13', false, '2026-08-20', false],
    ['617','2026-08-09','2026-08-22','2026-08-27', false, '2026-09-03', false],
    ['618','2026-08-23','2026-09-05','2026-09-10', false, '2026-09-17', false],
    ['619','2026-09-06','2026-09-19','2026-09-24', false, '2026-10-01', false],
    ['620','2026-09-20','2026-10-03','2026-10-08', false, '2026-10-16', true ],
    ['621','2026-10-04','2026-10-17','2026-10-22', false, '2026-10-29', false],
    ['622','2026-10-18','2026-10-31','2026-11-05', false, '2026-11-13', true ],
    ['623','2026-11-01','2026-11-14','2026-11-19', false, '2026-12-01', true ],
    ['624','2026-11-15','2026-11-28','2026-12-03', false, '2026-12-10', false],
    ['625','2026-11-29','2026-12-12','2026-12-17', false, '2026-12-28', true ],
    ['626','2026-12-13','2026-12-26','2026-12-29', false, '2027-01-07', false],
  ];
  for (const [p,s,e,r,r4,c,cDelayed] of periods) {
    await pool.query(
      `INSERT INTO billing_periods
         (period_number, start_date, end_date, reporting_deadline, deadline_is_4pm, payment_date, payment_delayed_holiday)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (period_number) DO UPDATE SET
         start_date=EXCLUDED.start_date,
         end_date=EXCLUDED.end_date,
         reporting_deadline=EXCLUDED.reporting_deadline,
         deadline_is_4pm=EXCLUDED.deadline_is_4pm,
         payment_date=EXCLUDED.payment_date,
         payment_delayed_holiday=EXCLUDED.payment_delayed_holiday`,
      [p,s,e,r,r4,c,cDelayed]
    );
  }
  console.log('DB ready');
}

// ── GSRP Hours Config ────────────────────────────────────────────────────────
const GSRP_CONFIG = {
  niles:       { days: [1,2,3,4],   start: '08:30', end: '15:30' }, // M-Th
  peace:       { days: [1,2,3,4],   start: '08:30', end: '15:30' },
  montessori:  { days: [1,2,3,4,5], start: '08:00', end: '15:00' }, // M-F
};
const CLOSING_TIME = '18:00';
const LATE_FEE_BASE = 10;
const LATE_FEE_PER_MIN = 1;
const GRACE_MINUTES = 10;

function toMinutes(timeStr) {
  if (!timeStr) return null;
  const clean = timeStr.replace(/\s*(AM|PM)/i, '').trim();
  const [h, m] = clean.split(':').map(Number);
  const suffix = timeStr.toUpperCase().includes('PM') ? 'PM' : 'AM';
  let hours = h;
  if (suffix === 'PM' && h !== 12) hours += 12;
  if (suffix === 'AM' && h === 12) hours = 0;
  return hours * 60 + (m || 0);
}

function calcLateFee(minutesLate) {
  if (minutesLate <= 0) return 0;
  return LATE_FEE_BASE + (minutesLate * LATE_FEE_PER_MIN);
}

// ── Routes: Children ─────────────────────────────────────────────────────────
app.get('/api/children', ssoAuth, async (req, res) => {
  const { center } = req.query;
  const q = center
    ? `SELECT * FROM children WHERE center=$1 ORDER BY last_name,first_name`
    : `SELECT * FROM children ORDER BY center,last_name,first_name`;
  const { rows } = await pool.query(q, center ? [center] : []);
  res.json(rows);
});

app.put('/api/children/:id', ssoAuth, async (req, res) => {
  const { subsidy_type, is_gsrp, is_cdc, is_school_age, is_active, notes,
          enrolled_hours_per_day, inactive_date,
          child_id_number, case_number, authorized_hours,
          cdc_start_date, cdc_end_date, family_contribution, child_care_fees } = req.body;
  const { rows } = await pool.query(
    `UPDATE children SET subsidy_type=$1,is_gsrp=$2,is_cdc=$3,is_school_age=$4,
     is_active=$5,notes=$6,
     enrolled_hours_per_day=COALESCE($7, enrolled_hours_per_day),
     inactive_date=$8,
     child_id_number=$9,
     case_number=$10,
     authorized_hours=$11,
     cdc_start_date=$12,
     cdc_end_date=COALESCE($13, cdc_end_date),
     family_contribution=COALESCE($14, family_contribution),
     child_care_fees=COALESCE($15, child_care_fees),
     updated_at=NOW() WHERE id=$16 RETURNING *`,
    [subsidy_type, is_gsrp, is_cdc, is_school_age, is_active, notes,
     enrolled_hours_per_day == null ? null : enrolled_hours_per_day,
     inactive_date || null,
     child_id_number || null,
     case_number || null,
     authorized_hours == null ? null : authorized_hours,
     cdc_start_date || null,
     cdc_end_date || null,
     family_contribution == null ? null : family_contribution,
     child_care_fees == null ? null : child_care_fees,
     req.params.id]
  );
  res.json(rows[0]);
});

app.post('/api/children', ssoAuth, async (req, res) => {
  const { center, first_name, last_name, subsidy_type, is_gsrp, is_cdc, is_school_age, notes } = req.body;
  const { rows } = await pool.query(
    `INSERT INTO children (center,first_name,last_name,subsidy_type,is_gsrp,is_cdc,is_school_age,notes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (center,first_name,last_name) DO UPDATE
     SET subsidy_type=$4,is_gsrp=$5,is_cdc=$6,is_school_age=$7,notes=$8,updated_at=NOW()
     RETURNING *`,
    [center, first_name, last_name, subsidy_type||'private_pay', is_gsrp||false, is_cdc||false, is_school_age||false, notes||null]
  );
  res.json(rows[0]);
});

// ── Routes: Bulk Roster Import (Playground CSV) ───────────────────────────────
// Playground exports typically include columns like:
//   "First Name", "Last Name", "Date of Birth", "Classroom", "Status",
//   "Enrollment Status", "Funding Source", "Subsidy", "Tags", etc.
// This endpoint accepts the raw Playground CSV and maps it into our children
// table. It supports a dry-run preview (no DB writes) and a commit mode.

function normHeader(h) {
  return String(h || '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

function pickField(row, candidates) {
  const normMap = {};
  for (const key of Object.keys(row)) normMap[normHeader(key)] = key;
  for (const cand of candidates) {
    const nk = normHeader(cand);
    if (normMap[nk] !== undefined) {
      const v = row[normMap[nk]];
      if (v !== undefined && v !== null && String(v).trim() !== '') return String(v).trim();
    }
  }
  return '';
}

function inferSubsidy(fundingText, tagsText) {
  const combined = `${fundingText} ${tagsText}`.toLowerCase();
  const hasCdc = /\bcdc\b|child\s*development\s*and\s*care|subsidy/i.test(combined);
  const hasGsrp = /gsrp|great\s*start/i.test(combined);
  const hasTri = /tri[\s-]?share/i.test(combined);
  if (hasCdc && hasGsrp) return { subsidy_type: 'cdc_gsrp', is_cdc: true, is_gsrp: true };
  if (hasCdc) return { subsidy_type: 'cdc', is_cdc: true, is_gsrp: false };
  if (hasGsrp) return { subsidy_type: 'gsrp', is_cdc: false, is_gsrp: true };
  if (hasTri) return { subsidy_type: 'private_pay', is_cdc: false, is_gsrp: false }; // Tri-Share tracked separately
  if (combined.trim()) return { subsidy_type: 'private_pay', is_cdc: false, is_gsrp: false };
  return { subsidy_type: 'unknown', is_cdc: false, is_gsrp: false };
}

function inferSchoolAge(classroomText, dobText) {
  const c = (classroomText || '').toLowerCase();
  if (/school.?age|sa\b|kinder|before\s*&?\s*after|bns/i.test(c)) return true;
  if (dobText) {
    const dob = new Date(dobText);
    if (!isNaN(dob)) {
      const ageYears = (Date.now() - dob.getTime()) / (365.25 * 24 * 3600 * 1000);
      if (ageYears >= 5.5) return true;
    }
  }
  return false;
}

function parseRosterRow(row) {
  const first = pickField(row, ['First Name', 'first_name', 'Child First Name', 'firstname', 'Given Name']);
  const last  = pickField(row, ['Last Name', 'last_name', 'Child Last Name', 'lastname', 'Family Name', 'Surname']);
  if (!first || !last) return null;

  const dob       = pickField(row, ['Date of Birth', 'DOB', 'Birth Date', 'birthdate']);
  const classroom = pickField(row, ['Classroom', 'Room', 'Class', 'Program', 'Group']);
  const status    = pickField(row, ['Status', 'Enrollment Status', 'Active Status']);
  const funding   = pickField(row, ['Funding Source', 'Subsidy', 'Subsidy Type', 'Funding', 'Payment Source']);
  const tags      = pickField(row, ['Tags', 'Labels', 'Notes', 'Categories']);

  const subsidy = inferSubsidy(funding, tags);
  const is_school_age = inferSchoolAge(classroom, dob);
  const is_active = !/withdrawn|inactive|disenrolled|terminated/i.test(status);

  return {
    first_name: first,
    last_name: last,
    dob: dob || null,
    classroom: classroom || null,
    raw_status: status || null,
    funding: funding || null,
    tags: tags || null,
    ...subsidy,
    is_school_age,
    is_active,
    notes: [classroom && `Classroom: ${classroom}`, funding && `Funding: ${funding}`].filter(Boolean).join(' · '),
  };
}

// Preview — parses and returns what would be imported, no DB writes
app.post('/api/upload/roster/preview', ssoAuth, upload.single('file'), async (req, res) => {
  const { center } = req.body;
  if (!req.file || !center) return res.status(400).json({ error: 'Missing center or file' });

  let records;
  try {
    records = parse(req.file.buffer.toString(), { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) { return res.status(400).json({ error: 'CSV parse error: ' + e.message }); }

  if (!records.length) return res.json({ rows: [], headers: [], errors: ['CSV appears empty'] });

  const headers = Object.keys(records[0]);
  const parsed = [];
  const errors = [];

  for (let i = 0; i < records.length; i++) {
    const r = parseRosterRow(records[i]);
    if (!r) { errors.push(`Row ${i+2}: missing first or last name`); continue; }

    // Check whether this would be a new row or an update
    const existing = await pool.query(
      `SELECT id,subsidy_type,is_gsrp,is_cdc,is_school_age,is_active FROM children
       WHERE center=$1 AND LOWER(first_name)=LOWER($2) AND LOWER(last_name)=LOWER($3)`,
      [center, r.first_name, r.last_name]
    );
    parsed.push({
      ...r,
      action: existing.rows.length ? 'update' : 'new',
      existing: existing.rows[0] || null,
    });
  }

  res.json({ rows: parsed, headers, errors, totalRows: records.length });
});

// Commit — writes to DB
app.post('/api/upload/roster/commit', ssoAuth, upload.single('file'), async (req, res) => {
  const { center, mode } = req.body; // mode: 'merge' (default) or 'replace'
  if (!req.file || !center) return res.status(400).json({ error: 'Missing center or file' });

  let records;
  try {
    records = parse(req.file.buffer.toString(), { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) { return res.status(400).json({ error: 'CSV parse error: ' + e.message }); }

  let created = 0, updated = 0, skipped = 0;
  const flags = [];
  const importedNames = new Set();

  for (const row of records) {
    const r = parseRosterRow(row);
    if (!r) { skipped++; continue; }
    importedNames.add(`${r.first_name.toLowerCase()}|${r.last_name.toLowerCase()}`);

    const existing = await pool.query(
      `SELECT id FROM children WHERE center=$1 AND LOWER(first_name)=LOWER($2) AND LOWER(last_name)=LOWER($3)`,
      [center, r.first_name, r.last_name]
    );

    if (existing.rows.length === 0) {
      const { rows } = await pool.query(
        `INSERT INTO children (center,first_name,last_name,subsidy_type,is_gsrp,is_cdc,is_school_age,is_active,notes)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
        [center, r.first_name, r.last_name, r.subsidy_type, r.is_gsrp, r.is_cdc, r.is_school_age, r.is_active, r.notes]
      );
      created++;
      if (r.subsidy_type === 'unknown') {
        await createFlag(center, rows[0].id, `${r.first_name} ${r.last_name}`, 'NEW_CHILD_NO_STATUS',
          `Imported from Playground roster with unknown funding source — please set`, null);
        flags.push({ childName: `${r.first_name} ${r.last_name}`, type: 'NEW_CHILD_NO_STATUS' });
      }
    } else {
      // Merge mode: update inferred fields but preserve manual overrides if subsidy was already set
      await pool.query(
        `UPDATE children SET
           subsidy_type = CASE WHEN subsidy_type IN ('unknown','') OR subsidy_type IS NULL THEN $1 ELSE subsidy_type END,
           is_gsrp = CASE WHEN subsidy_type IN ('unknown','') OR subsidy_type IS NULL THEN $2 ELSE is_gsrp END,
           is_cdc  = CASE WHEN subsidy_type IN ('unknown','') OR subsidy_type IS NULL THEN $3 ELSE is_cdc END,
           is_school_age = $4,
           is_active = $5,
           notes = CASE WHEN notes IS NULL OR notes = '' THEN $6 ELSE notes END,
           updated_at = NOW()
         WHERE id=$7`,
        [r.subsidy_type, r.is_gsrp, r.is_cdc, r.is_school_age, r.is_active, r.notes, existing.rows[0].id]
      );
      updated++;
    }
  }

  // Replace mode: deactivate children not present in the new roster
  let deactivated = 0;
  if (mode === 'replace') {
    const all = await pool.query(`SELECT id,first_name,last_name FROM children WHERE center=$1 AND is_active=true`, [center]);
    for (const c of all.rows) {
      const key = `${c.first_name.toLowerCase()}|${c.last_name.toLowerCase()}`;
      if (!importedNames.has(key)) {
        await pool.query(`UPDATE children SET is_active=false,updated_at=NOW() WHERE id=$1`, [c.id]);
        deactivated++;
      }
    }
  }

  res.json({ created, updated, skipped, deactivated, flags });
});


app.post('/api/upload/cdc-statement', ssoAuth, upload.single('file'), async (req, res) => {
  const { center, period_number } = req.body;
  if (!req.file || !center || !period_number) return res.status(400).json({ error: 'Missing required fields' });

  let records;
  try {
    records = parse(req.file.buffer.toString(), {
      columns: true, skip_empty_lines: true, trim: true
    });
  } catch (e) {
    return res.status(400).json({ error: 'Could not parse CSV: ' + e.message });
  }

  const inserted = [];
  const flags = [];

  for (const row of records) {
    const childName = row['Child Name'] || row['child_name'] || row['name'] || '';
    const amountPaid = parseFloat(row['Amount Paid'] || row['amount_paid'] || 0) || null;
    const status = row['Status'] || row['Notes'] || row['Applied to Account'] || '';
    const isNoAuth = /no auth/i.test(status);
    const isNotFound = /not found/i.test(status) || /duplicate/i.test(status);

    if (!childName) continue;

    // Try to match to existing child
    const nameParts = childName.trim().split(' ');
    const firstName = nameParts[0];
    const lastName = nameParts.slice(1).join(' ');
    const childRow = await pool.query(
      `SELECT id FROM children WHERE center=$1 AND LOWER(first_name)=LOWER($2) AND LOWER(last_name)=LOWER($3)`,
      [center, firstName, lastName]
    );
    const childId = childRow.rows[0]?.id || null;

    await pool.query(
      `INSERT INTO cdc_periods (period_number,center,child_id,child_name,billed,amount_paid,status,notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT DO NOTHING`,
      [period_number, center, childId, childName, true, amountPaid,
       isNoAuth ? 'no_auth' : isNotFound ? 'not_found' : amountPaid ? 'paid' : 'unknown',
       status || null]
    );

    // Create flags for problematic records
    if (isNoAuth) {
      await createFlag(center, childId, childName, 'NO_AUTHORIZATION',
        `No authorization on CDC statement for period ${period_number}`, period_number);
      flags.push({ childName, type: 'NO_AUTHORIZATION' });
    }
    if (isNotFound) {
      await createFlag(center, childId, childName, 'NOT_FOUND_IN_DHS',
        `Child not found in DHS for period ${period_number}`, period_number);
      flags.push({ childName, type: 'NOT_FOUND_IN_DHS' });
    }
    inserted.push(childName);
  }

  // Check for CDC children NOT on this statement
  const cdcChildren = await pool.query(
    `SELECT id,first_name,last_name FROM children WHERE center=$1 AND is_cdc=true AND is_active=true`,
    [center]
  );
  for (const child of cdcChildren.rows) {
    const fullName = `${child.first_name} ${child.last_name}`;
    const onStatement = await pool.query(
      `SELECT id FROM cdc_periods WHERE period_number=$1 AND center=$2 AND child_id=$3`,
      [period_number, center, child.id]
    );
    if (onStatement.rows.length === 0) {
      await createFlag(center, child.id, fullName, 'MISSING_FROM_STATEMENT',
        `CDC child not present on statement for period ${period_number} — family should be billed directly`,
        period_number);
      flags.push({ childName: fullName, type: 'MISSING_FROM_STATEMENT' });
    }
  }

  res.json({ inserted: inserted.length, flags });
});

// ── Routes: Upload Attendance CSV ─────────────────────────────────────────────
// ── Helper: robust CSV parsing for Playground attendance exports ─────────────
// Playground's CSV has quirks:
//   1. BOM (\uFEFF) at the start of the header row — breaks column lookups
//   2. Duplicate column names: "Signer" appears twice (after Check-in AND after
//      Check-out), and "Signature" appears twice. csv-parse's default columns:true
//      silently drops duplicates, so the second value overwrites the first.
//   3. Huge signature URL columns (thousands of chars) we don't care about
// This parser strips BOM, handles duplicate headers by suffixing _2, _3, etc.,
// and also accepts the older lowercase "last_name" / "first_name" format.
function parsePlaygroundAttendanceCSV(buffer) {
  let text = buffer.toString();
  if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1); // strip BOM

  // Use array-mode parse so we can process headers manually
  const rows = parse(text, { skip_empty_lines: true, trim: true });
  if (!rows.length) return [];

  // Build unique column names (append _2, _3 for duplicates)
  const rawHeaders = rows[0].map(h => String(h || '').trim());
  const seen = {};
  const headers = rawHeaders.map(h => {
    seen[h] = (seen[h] || 0) + 1;
    return seen[h] === 1 ? h : `${h}_${seen[h]}`;
  });

  // Convert remaining rows into objects keyed by unique headers
  return rows.slice(1).map(row => {
    const obj = {};
    for (let i = 0; i < headers.length; i++) obj[headers[i]] = row[i] || '';
    return obj;
  });
}

// Pull a value using any of the given header-name variants
function pickAttendanceField(row, names) {
  for (const n of names) if (row[n] !== undefined && row[n] !== '') return row[n];
  return '';
}


app.post('/api/upload/attendance', ssoAuth, upload.single('file'), async (req, res) => {
  const startTime = Date.now();
  const { center } = req.body;
  if (!req.file || !center) return res.status(400).json({ error: 'Missing center or file' });

  try {
    let records;
    try {
      records = parsePlaygroundAttendanceCSV(req.file.buffer);
    } catch (e) { return res.status(400).json({ error: 'CSV parse error: ' + e.message }); }

    const centerKey = center.toLowerCase().replace(/\s+/g,'');
    const gsrp = GSRP_CONFIG[centerKey] || GSRP_CONFIG['niles'];
    const flags = [];

    // ── PASS 1: extract rows + collect unique children ────────────────────────
    const cleanRows = [];
    const uniqueChildren = new Map();
    for (const row of records) {
      const lastName = pickAttendanceField(row, ['Last name','Last Name','last_name','LastName']);
      const firstName = pickAttendanceField(row, ['First name','First Name','first_name','FirstName']);
      const dateStr = pickAttendanceField(row, ['Date','date']);
      const checkin = pickAttendanceField(row, ['Check-in','Check In','checkin','CheckIn']);
      const checkout = pickAttendanceField(row, ['Check-out','Check Out','checkout','CheckOut']);
      if (!lastName || !dateStr) continue;

      const dateObj = new Date(dateStr);
      if (isNaN(dateObj)) continue;
      const isAbsent = /absent/i.test(checkin) || checkin === '-' || !checkin;
      const key = `${firstName.toLowerCase()}|${lastName.toLowerCase()}`;
      uniqueChildren.set(key, { first: firstName, last: lastName });

      // Strip signature URLs — keep raw_row small
      const cleanRow = {};
      for (const [k, v] of Object.entries(row)) {
        if (/^Signature/i.test(k)) continue;
        cleanRow[k] = v;
      }

      cleanRows.push({
        firstName, lastName, dateObj, checkin, checkout, isAbsent, cleanRow, key,
        dayOfWeek: dateObj.getDay(),
        attendDateISO: dateObj.toISOString().split('T')[0],
      });
    }

    // ── PASS 2: bulk resolve + create children ────────────────────────────────
    const childEntries = [...uniqueChildren.values()];
    const childIdMap = new Map();
    const childMetaMap = new Map(); // key → {is_gsrp, is_cdc, is_school_age}
    let autoCreated = 0;

    if (childEntries.length) {
      const firstNames = childEntries.map(c => c.first);
      const lastNames = childEntries.map(c => c.last);
      const existingQ = await pool.query(
        `SELECT id, first_name, last_name, is_gsrp, is_cdc, is_school_age FROM children
         WHERE center=$1 AND (LOWER(first_name), LOWER(last_name)) = ANY(
           SELECT LOWER(unnest($2::text[])), LOWER(unnest($3::text[]))
         )`,
        [center, firstNames, lastNames]
      );
      for (const r of existingQ.rows) {
        const k = `${r.first_name.toLowerCase()}|${r.last_name.toLowerCase()}`;
        childIdMap.set(k, r.id);
        childMetaMap.set(k, { is_gsrp: r.is_gsrp, is_cdc: r.is_cdc, is_school_age: r.is_school_age });
      }
      const missing = childEntries.filter(c => !childIdMap.has(`${c.first.toLowerCase()}|${c.last.toLowerCase()}`));
      if (missing.length) {
        const placeholders = missing.map((_, i) => `($1, $${i*2+2}, $${i*2+3}, 'unknown', 'Auto-created from attendance upload — set subsidy type')`).join(',');
        const params = [center];
        for (const m of missing) params.push(m.first, m.last);
        const insertedQ = await pool.query(
          `INSERT INTO children (center, first_name, last_name, subsidy_type, notes)
           VALUES ${placeholders}
           ON CONFLICT (center, first_name, last_name) DO UPDATE SET notes=EXCLUDED.notes
           RETURNING id, first_name, last_name`,
          params
        );
        for (const r of insertedQ.rows) {
          const k = `${r.first_name.toLowerCase()}|${r.last_name.toLowerCase()}`;
          childIdMap.set(k, r.id);
          childMetaMap.set(k, { is_gsrp: false, is_cdc: false, is_school_age: false });
        }
        autoCreated = missing.length;
        await Promise.all(insertedQ.rows.map(r => createFlag(
          center, r.id, `${r.first_name} ${r.last_name}`, 'NEW_CHILD_NO_STATUS',
          `Child added from attendance upload with unknown subsidy status — please set status`, null
        )));
        for (const r of insertedQ.rows) {
          flags.push({ childName: `${r.first_name} ${r.last_name}`, type: 'NEW_CHILD_NO_STATUS' });
        }
      }
    }

    // ── PASS 3: bulk insert attendance records ────────────────────────────────
    const CHUNK_SIZE = 200;
    let processed = 0;
    for (let s = 0; s < cleanRows.length; s += CHUNK_SIZE) {
      const chunk = cleanRows.slice(s, s + CHUNK_SIZE);
      const valuesSql = [];
      const params = [];
      let p = 0;
      for (const r of chunk) {
        const childId = childIdMap.get(r.key);
        if (!childId) continue;
        params.push(
          center, childId, r.firstName, r.lastName, r.attendDateISO,
          r.isAbsent ? null : parseTime12(r.checkin),
          r.isAbsent ? null : parseTime12(r.checkout),
          r.isAbsent, JSON.stringify(r.cleanRow)
        );
        valuesSql.push(`($${p+1},$${p+2},$${p+3},$${p+4},$${p+5},$${p+6},$${p+7},$${p+8},$${p+9})`);
        p += 9;
        processed++;
      }
      if (valuesSql.length) {
        await pool.query(
          `INSERT INTO attendance_records
             (center, child_id, child_first, child_last, attend_date, checkin_time, checkout_time, is_absent, raw_row)
           VALUES ${valuesSql.join(',')}`,
          params
        );
      }
    }

    // ── PASS 4: check late pickups for GSRP kids (parallelized) ───────────────
    const latePickupPromises = [];
    for (const r of cleanRows) {
      const meta = childMetaMap.get(r.key);
      if (!meta?.is_gsrp || r.isAbsent || !r.checkout) continue;
      if (!gsrp.days.includes(r.dayOfWeek)) continue;
      const checkoutMinutes = toMinutes(r.checkout);
      if (checkoutMinutes && checkoutMinutes > 18*60) {
        const minsLate = checkoutMinutes - 18*60;
        const childId = childIdMap.get(r.key);
        latePickupPromises.push(createFlag(
          center, childId, `${r.firstName} ${r.lastName}`, 'AFTER_CLOSING_PICKUP',
          `Picked up ${minsLate} min after 6pm on ${r.attendDateISO}. Fee: $${calcLateFee(minsLate)}`,
          null, r.attendDateISO
        ));
        flags.push({ childName: `${r.firstName} ${r.lastName}`, type: 'AFTER_CLOSING_PICKUP', minsLate });
      }
    }
    await Promise.all(latePickupPromises);

    const ms = Date.now() - startTime;
    console.log(`[upload/attendance] ${center}: ${processed} records, ${autoCreated} new kids, ${ms}ms`);
    res.json({ processed, autoCreated, elapsed_ms: ms, flags });
  } catch (e) {
    console.error(`[upload/attendance] ERROR for ${center}:`, e);
    res.status(500).json({ error: 'Upload failed: ' + e.message });
  }
});

function timeStrToMin(t) {
  const [h,m] = t.split(':').map(Number);
  return h*60+(m||0);
}

function parseTime12(str) {
  if (!str || str === '-' || /absent/i.test(str)) return null;
  try {
    const clean = str.trim().toUpperCase();
    const isPM = clean.includes('PM');
    const isAM = clean.includes('AM');
    const timePart = clean.replace(/\s*(AM|PM)/, '').trim();
    let [h, m] = timePart.split(':').map(Number);
    if (isPM && h !== 12) h += 12;
    if (isAM && h === 12) h = 0;
    return `${String(h).padStart(2,'0')}:${String(m||0).padStart(2,'0')}`;
  } catch { return null; }
}

// ── Routes: Billing Engine ────────────────────────────────────────────────────
app.get('/api/billing/calculate', ssoAuth, async (req, res) => {
  const { center, start_date, end_date } = req.query;
  if (!center || !start_date || !end_date) return res.status(400).json({ error: 'Missing params' });

  const centerKey = center.toLowerCase().replace(/[^a-z]/g,'');
  const gsrp = GSRP_CONFIG[centerKey] || GSRP_CONFIG['niles'];

  const { rows: records } = await pool.query(
    `SELECT ar.*,c.is_gsrp,c.is_cdc,c.is_school_age,c.subsidy_type
     FROM attendance_records ar
     JOIN children c ON ar.child_id=c.id
     WHERE ar.center=$1 AND ar.attend_date BETWEEN $2 AND $3 AND ar.is_absent=false
     ORDER BY ar.child_last,ar.child_first,ar.attend_date`,
    [center, start_date, end_date]
  );

  const { rows: noSchoolDays } = await pool.query(
    `SELECT school_date FROM no_school_days WHERE (center=$1 OR center IS NULL OR applies_to='all')
     AND school_date BETWEEN $2 AND $3`,
    [center, start_date, end_date]
  );
  const noSchoolSet = new Set(noSchoolDays.map(r => r.school_date.toISOString().split('T')[0]));

  const results = {};

  for (const rec of records) {
    const key = `${rec.child_last}_${rec.child_first}`;
    if (!results[key]) {
      results[key] = {
        childName: `${rec.child_first} ${rec.child_last}`,
        isGsrp: rec.is_gsrp,
        isCdc: rec.is_cdc,
        isSchoolAge: rec.is_school_age,
        subsidyType: rec.subsidy_type,
        cdcBillableBlocks: [],
        gsrpBlocks: [],
        privateBillable: [],
        noSchoolDayCharges: [],
        latePickupFees: [],
      };
    }

    const entry = results[key];
    const dateStr = rec.attend_date.toISOString().split('T')[0];
    const dateObj = new Date(rec.attend_date);
    const dayOfWeek = dateObj.getUTCDay();
    const isFriday = dayOfWeek === 5;
    const isNoSchool = noSchoolSet.has(dateStr);
    const gsrpDay = gsrp.days.includes(dayOfWeek);

    const checkinMin = rec.checkin_time ? timeStrToMin(rec.checkin_time) : null;
    const checkoutMin = rec.checkout_time ? timeStrToMin(rec.checkout_time) : null;
    const gsrpStart = timeStrToMin(gsrp.start);
    const gsrpEnd = timeStrToMin(gsrp.end);

    if (rec.is_gsrp && gsrpDay && !isFriday && !isNoSchool) {
      // Split into CDC-billable (before GSRP) + GSRP block + CDC-billable (after GSRP)
      if (checkinMin !== null && checkinMin < gsrpStart) {
        const block = { date: dateStr, in: minToTime(checkinMin), out: minToTime(gsrpStart), type: 'before_gsrp', billTo: rec.is_cdc ? 'CDC' : 'private' };
        rec.is_cdc ? entry.cdcBillableBlocks.push(block) : entry.privateBillable.push(block);
      }
      entry.gsrpBlocks.push({ date: dateStr, in: gsrp.start, out: gsrp.end, billTo: 'GSRP' });
      if (checkoutMin !== null && checkoutMin > gsrpEnd) {
        const block = { date: dateStr, in: minToTime(gsrpEnd), out: minToTime(checkoutMin), type: 'after_gsrp', billTo: rec.is_cdc ? 'CDC' : 'private' };
        rec.is_cdc ? entry.cdcBillableBlocks.push(block) : entry.privateBillable.push(block);
      }
    } else if (rec.is_gsrp && isFriday) {
      // Full day CDC billable on Fridays (Niles/Peace)
      if (centerKey !== 'montessori') {
        const block = { date: dateStr, in: minToTime(checkinMin||0), out: minToTime(checkoutMin||18*60), type: 'friday_full', billTo: rec.is_cdc ? 'CDC' : 'private' };
        rec.is_cdc ? entry.cdcBillableBlocks.push(block) : entry.privateBillable.push(block);
      }
    } else {
      // Not GSRP or no-school day — full time billable
      if (checkinMin !== null && checkoutMin !== null) {
        const block = { date: dateStr, in: minToTime(checkinMin), out: minToTime(checkoutMin), type: 'full_day', billTo: rec.is_cdc ? 'CDC' : 'private' };
        if (rec.is_cdc) entry.cdcBillableBlocks.push(block);
        else entry.privateBillable.push(block);

        // No-school day charge for school-age or GSRP-only families
        if (isNoSchool && rec.is_school_age) {
          const hours = (checkoutMin - checkinMin) / 60;
          const hourlyCharge = hours * 8;
          entry.noSchoolDayCharges.push({ date: dateStr, hours: Math.round(hours*10)/10, hourlyCharge });
        }
      }
    }

    // Late after closing (6pm)
    if (checkoutMin && checkoutMin > 18*60) {
      const minsLate = checkoutMin - 18*60;
      const fee = calcLateFee(minsLate);
      entry.latePickupFees.push({ date: dateStr, checkoutTime: minToTime(checkoutMin), minsLate, fee });
    }
  }

  res.json(Object.values(results));
});

function minToTime(m) {
  const h = Math.floor(m/60);
  const min = m%60;
  return `${String(h).padStart(2,'0')}:${String(min).padStart(2,'0')}`;
}

// ── Routes: Flags ─────────────────────────────────────────────────────────────
app.get('/api/flags', ssoAuth, async (req, res) => {
  const { center, resolved } = req.query;
  const params = [];
  let q = `SELECT f.*,c.first_name,c.last_name FROM billing_flags f
           LEFT JOIN children c ON f.child_id=c.id WHERE 1=1`;
  if (center) { params.push(center); q += ` AND f.center=$${params.length}`; }
  if (resolved !== undefined) { params.push(resolved === 'true'); q += ` AND f.resolved=$${params.length}`; }
  q += ` ORDER BY f.created_at DESC`;
  const { rows } = await pool.query(q, params);
  res.json(rows);
});

app.put('/api/flags/:id/resolve', ssoAuth, async (req, res) => {
  const { resolved_by } = req.body;
  const { rows } = await pool.query(
    `UPDATE billing_flags SET resolved=true,resolved_at=NOW(),resolved_by=$1 WHERE id=$2 RETURNING *`,
    [resolved_by||'admin', req.params.id]
  );
  res.json(rows[0]);
});

async function createFlag(center, childId, childName, flagType, detail, period, date) {
  await pool.query(
    `INSERT INTO billing_flags (center,child_id,child_name,flag_type,flag_detail,period_number,attend_date)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [center, childId||null, childName, flagType, detail, period||null, date||null]
  );
}

// ── Routes: CDC Periods ───────────────────────────────────────────────────────
app.get('/api/cdc-periods', ssoAuth, async (req, res) => {
  const { center, period_number } = req.query;
  const params = [];
  let q = `SELECT cp.*,c.subsidy_type,c.is_gsrp FROM cdc_periods cp
           LEFT JOIN children c ON cp.child_id=c.id WHERE 1=1`;
  if (center) { params.push(center); q += ` AND cp.center=$${params.length}`; }
  if (period_number) { params.push(period_number); q += ` AND cp.period_number=$${params.length}`; }
  q += ` ORDER BY cp.child_name`;
  const { rows } = await pool.query(q, params);
  res.json(rows);
});

app.put('/api/cdc-periods/:id', ssoAuth, async (req, res) => {
  const { amount_paid, applied_to_account, status, notes } = req.body;
  const { rows } = await pool.query(
    `UPDATE cdc_periods SET amount_paid=$1,applied_to_account=$2,status=$3,notes=$4 WHERE id=$5 RETURNING *`,
    [amount_paid, applied_to_account, status, notes, req.params.id]
  );
  res.json(rows[0]);
});

// ── Routes: No School Days ────────────────────────────────────────────────────
app.get('/api/no-school-days', ssoAuth, async (req, res) => {
  const { rows } = await pool.query(`SELECT * FROM no_school_days ORDER BY school_date`);
  res.json(rows);
});

app.post('/api/no-school-days', ssoAuth, async (req, res) => {
  const { school_date, reason, center, applies_to } = req.body;
  const { rows } = await pool.query(
    `INSERT INTO no_school_days (school_date,reason,center,applies_to) VALUES ($1,$2,$3,$4) RETURNING *`,
    [school_date, reason, center||null, applies_to||'all']
  );
  res.json(rows[0]);
});

app.delete('/api/no-school-days/:id', ssoAuth, async (req, res) => {
  await pool.query(`DELETE FROM no_school_days WHERE id=$1`, [req.params.id]);
  res.json({ success: true });
});

// ── Routes: Billing Periods ───────────────────────────────────────────────────
app.get('/api/billing-periods', ssoAuth, async (req, res) => {
  const { rows } = await pool.query(`SELECT * FROM billing_periods ORDER BY start_date DESC`);
  res.json(rows);
});

app.put('/api/billing-periods/:id', ssoAuth, async (req, res) => {
  const { billing_status, crediting_status } = req.body;
  const { rows } = await pool.query(
    `UPDATE billing_periods SET billing_status=$1,crediting_status=$2 WHERE id=$3 RETURNING *`,
    [billing_status, crediting_status, req.params.id]
  );
  res.json(rows[0]);
});

// ── Routes: Dashboard Stats ───────────────────────────────────────────────────
app.get('/api/dashboard', ssoAuth, async (req, res) => {
  const [flags, cdcPeriods, children, periods] = await Promise.all([
    pool.query(`SELECT flag_type,center,COUNT(*) as count FROM billing_flags WHERE resolved=false GROUP BY flag_type,center`),
    pool.query(`SELECT COUNT(*) FILTER (WHERE status='no_auth') as no_auth,
                       COUNT(*) FILTER (WHERE status='missing_from_statement' OR status='not_found') as missing,
                       COUNT(*) FILTER (WHERE applied_to_account=false AND amount_paid>0) as not_applied
                FROM cdc_periods`),
    pool.query(`SELECT COUNT(*) FILTER (WHERE subsidy_type='unknown') as unknown_status,
                       COUNT(*) FILTER (WHERE is_active=true) as active
                FROM children`),
    pool.query(`SELECT * FROM billing_periods WHERE end_date <= NOW() AND billing_status='pending' ORDER BY end_date LIMIT 3`),
  ]);
  res.json({
    activeFlags: flags.rows,
    cdcSummary: cdcPeriods.rows[0],
    childrenSummary: children.rows[0],
    overduePeriods: periods.rows,
  });
});

// ── Tuition Rates ─────────────────────────────────────────────────────────────
app.get('/api/tuition-rates', ssoAuth, async (req, res) => {
  const { rows } = await pool.query(`SELECT * FROM tuition_rates ORDER BY center,rate_type`);
  res.json(rows);
});

app.post('/api/tuition-rates', ssoAuth, async (req, res) => {
  const { center, rate_type, label, weekly_amount, hourly_rate } = req.body;
  const { rows } = await pool.query(
    `INSERT INTO tuition_rates (center,rate_type,label,weekly_amount,hourly_rate)
     VALUES ($1,$2,$3,$4,$5) RETURNING *`,
    [center, rate_type, label, weekly_amount, hourly_rate||8.00]
  );
  res.json(rows[0]);
});

// ═════════════════════════════════════════════════════════════════════════════
// ── CDC Filing Wizard ────────────────────────────────────────────────────────
// ═════════════════════════════════════════════════════════════════════════════
// Rules applied per child per day in the period:
//   1. Active status check — if inactive_date is set and day > inactive_date, EXCLUDE that day.
//   2. Absence streak (consecutive school-days Mon–Fri only):
//        days 1–10  → submit enrolled_hours_per_day as NORMAL attendance (no absence flag)
//        days 11–20 → submit enrolled_hours_per_day flagged as HOLIDAY/ABSENT
//        days 21+   → EXCLUDE this child from filing from that point forward in the period
//   3. Present day — use actual check-in/out. Multiple in/out blocks per day are summed
//      (any child can have multiple blocks — school-age splits for school, GSRP/Strong
//      Beginnings kids leave and come back on GSRP days, etc.).
//   4. GSRP carve-out — on GSRP-scheduled days for GSRP-enrolled kids, subtract the
//      overlap with the GSRP window (Niles/Peace M–Th 8:30–3:30; Montessori M–F 8:00–3:00).
//      Only remaining NON-GSRP time is submitted to CDC.
//   5. No-school / no-GSRP day — if the date is in the no_school_days table, submit ALL
//      hours the child was present (no GSRP carve-out that day).
//   6. Friday special (Niles/Peace) — GSRP kids on Fridays bill full-day to CDC (no carve-out).

const GSRP_WINDOWS = {
  niles:      { days: [1,2,3,4],   start: 8*60+30, end: 15*60+30 }, // M-Th 8:30-3:30
  peace:      { days: [1,2,3,4],   start: 8*60+30, end: 15*60+30 },
  montessori: { days: [1,2,3,4,5], start: 8*60,    end: 15*60 },    // M-F 8:00-3:00
};

function centerKey(center) {
  const c = String(center || '').toLowerCase();
  if (c.includes('niles')) return 'niles';
  if (c.includes('peace')) return 'peace';
  if (c.includes('mont'))  return 'montessori';
  return 'niles';
}

function parseTime12(t) {
  if (!t || t === '-' || /absent/i.test(t)) return null;
  const s = String(t).trim();
  const m = s.match(/(\d{1,2}):(\d{2})\s*(AM|PM)?/i);
  if (!m) return null;
  let h = parseInt(m[1]);
  const min = parseInt(m[2]);
  const suf = (m[3] || '').toUpperCase();
  if (suf === 'PM' && h !== 12) h += 12;
  if (suf === 'AM' && h === 12) h = 0;
  return h*60 + min;
}

function overlapMinutes(aStart, aEnd, bStart, bEnd) {
  const s = Math.max(aStart, bStart);
  const e = Math.min(aEnd, bEnd);
  return Math.max(0, e - s);
}

function eachDateInRange(startStr, endStr) {
  const out = [];
  const d = new Date(startStr + 'T00:00:00Z');
  const end = new Date(endStr + 'T00:00:00Z');
  while (d <= end) {
    out.push(d.toISOString().split('T')[0]);
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return out;
}

function dayOfWeekUTC(dateStr) {
  return new Date(dateStr + 'T00:00:00Z').getUTCDay(); // 0=Sun
}

function isSchoolDay(dateStr) {
  const dow = dayOfWeekUTC(dateStr);
  return dow >= 1 && dow <= 5;
}

/**
 * Main rule engine.
 * @param {Array} children   — [{id, first_name, last_name, is_gsrp, is_cdc, is_school_age, is_active, inactive_date, enrolled_hours_per_day}]
 * @param {Array} attendance — [{child_id, attend_date, checkin_time, checkout_time, is_absent}]
 * @param {Set}  noSchoolSet — Set of date strings where there is no school/GSRP
 * @param {string} center    — center name
 * @param {string} periodStart — YYYY-MM-DD
 * @param {string} periodEnd   — YYYY-MM-DD
 */
function computeCDCFiling(children, attendance, noSchoolSet, center, periodStart, periodEnd) {
  const ck = centerKey(center);
  const gsrp = GSRP_WINDOWS[ck];
  const dates = eachDateInRange(periodStart, periodEnd);

  // Group attendance by child_id -> date -> [blocks]
  const attByChild = {};
  for (const a of attendance) {
    const d = typeof a.attend_date === 'string'
      ? a.attend_date.split('T')[0]
      : new Date(a.attend_date).toISOString().split('T')[0];
    if (!attByChild[a.child_id]) attByChild[a.child_id] = {};
    if (!attByChild[a.child_id][d]) attByChild[a.child_id][d] = [];
    attByChild[a.child_id][d].push(a);
  }

  const results = [];

  for (const child of children) {
    if (!child.is_cdc) continue; // Only CDC kids get filed

    const childAtt = attByChild[child.id] || {};
    const enrolled = parseFloat(child.enrolled_hours_per_day) || 9.0;
    const inactiveDate = child.inactive_date
      ? (typeof child.inactive_date === 'string'
          ? child.inactive_date.split('T')[0]
          : new Date(child.inactive_date).toISOString().split('T')[0])
      : null;

    let consecutiveAbsent = 0;
    let excludedFromHere = false;
    const dayRows = [];

    for (const dateStr of dates) {
      const dow = dayOfWeekUTC(dateStr);
      const isWeekend = dow === 0 || dow === 6;
      const isNoSchool = noSchoolSet.has(dateStr);

      // Inactive cutoff — any day past the inactive date is excluded (not filed at all)
      if (inactiveDate && dateStr > inactiveDate) {
        dayRows.push({ date: dateStr, status: 'INACTIVE', hours: 0, excluded: true });
        continue;
      }

      // If already past 20 consecutive school-day absences, exclude
      if (excludedFromHere) {
        dayRows.push({ date: dateStr, status: 'EXCLUDED_21_DAY', hours: 0, excluded: true });
        continue;
      }

      // Weekends — not billable, don't count toward absence streak
      if (isWeekend) {
        dayRows.push({ date: dateStr, status: 'WEEKEND', hours: 0, excluded: true });
        continue;
      }

      const blocks = childAtt[dateStr] || [];
      const presentBlocks = blocks.filter(b => !b.is_absent && b.checkin_time && b.checkout_time);
      const isAbsentToday = presentBlocks.length === 0;

      if (isAbsentToday) {
        // School-day absence — increment streak
        consecutiveAbsent++;
        if (consecutiveAbsent <= 10) {
          // Days 1-10: submit enrolled hours as NORMAL (no absence flag per Mary's clarification)
          dayRows.push({
            date: dateStr,
            status: 'ABSENT_NORMAL',
            hours: enrolled,
            absent_day_number: consecutiveAbsent,
            excluded: false,
            is_holiday_absent: false,
          });
        } else if (consecutiveAbsent <= 20) {
          // Days 11-20: submit enrolled hours flagged as HOLIDAY/ABSENT
          dayRows.push({
            date: dateStr,
            status: 'ABSENT_HOLIDAY',
            hours: enrolled,
            absent_day_number: consecutiveAbsent,
            excluded: false,
            is_holiday_absent: true,
          });
        } else {
          // Day 21+ : exclude this child from filing going forward in this period
          excludedFromHere = true;
          dayRows.push({ date: dateStr, status: 'EXCLUDED_21_DAY', hours: 0, excluded: true });
        }
        continue;
      }

      // Child was present today — reset absence streak
      consecutiveAbsent = 0;

      // Sum all blocks; then subtract GSRP overlap if applicable
      let totalMins = 0;
      const blockDetails = [];
      // Sort blocks by check-in time so "primary" block is first
      const sortedBlocks = [...presentBlocks].sort((a, b) => {
        const aIn = typeof a.checkin_time === 'string' ? parseTime12(a.checkin_time) : timeStrToMin(a.checkin_time);
        const bIn = typeof b.checkin_time === 'string' ? parseTime12(b.checkin_time) : timeStrToMin(b.checkin_time);
        return (aIn||0) - (bIn||0);
      });
      for (const b of sortedBlocks) {
        const inM  = typeof b.checkin_time  === 'string' ? parseTime12(b.checkin_time)  : timeStrToMin(b.checkin_time);
        const outM = typeof b.checkout_time === 'string' ? parseTime12(b.checkout_time) : timeStrToMin(b.checkout_time);
        if (inM == null || outM == null || outM <= inM) continue;
        totalMins += (outM - inM);
        blockDetails.push({
          in: inM,
          out: outM,
          in_str: minToTimeAMPM(inM),
          out_str: minToTimeAMPM(outM),
        });
      }

      // GSRP carve-out (only when: child is GSRP-enrolled, day is in GSRP schedule, NOT a no-school day, NOT Friday for Niles/Peace)
      const isGsrpScheduledDay = gsrp.days.includes(dow) && !isNoSchool;
      const isFridayNilesOrPeace = (dow === 5) && (ck === 'niles' || ck === 'peace');
      const applyCarveOut = child.is_gsrp && isGsrpScheduledDay && !isFridayNilesOrPeace;

      let gsrpOverlapMins = 0;
      if (applyCarveOut) {
        for (const blk of blockDetails) {
          gsrpOverlapMins += overlapMinutes(blk.in, blk.out, gsrp.start, gsrp.end);
        }
      }

      const billableMins = Math.max(0, totalMins - gsrpOverlapMins);
      const hours = Math.round((billableMins / 60) * 100) / 100;

      dayRows.push({
        date: dateStr,
        status: isNoSchool ? 'NO_SCHOOL_DAY' : (applyCarveOut ? 'GSRP_CARVE' : 'PRESENT'),
        hours,
        total_mins: totalMins,
        gsrp_mins: gsrpOverlapMins,
        blocks: blockDetails,
        excluded: false,
        is_holiday_absent: false,
      });
    }

    const totalHours = Math.round(dayRows.filter(r => !r.excluded).reduce((s,r) => s + (r.hours||0), 0) * 100) / 100;
    const normalHours = Math.round(dayRows.filter(r => !r.excluded && !r.is_holiday_absent).reduce((s,r) => s + (r.hours||0), 0) * 100) / 100;
    const holidayAbsentHours = Math.round(dayRows.filter(r => r.is_holiday_absent).reduce((s,r) => s + (r.hours||0), 0) * 100) / 100;
    const excludedAfter = dayRows.find(r => r.status === 'EXCLUDED_21_DAY')?.date || null;
    const inactiveAfter = dayRows.find(r => r.status === 'INACTIVE')?.date || null;

    results.push({
      child_id: child.id,
      first_name: child.first_name,
      last_name: child.last_name,
      is_gsrp: !!child.is_gsrp,
      is_school_age: !!child.is_school_age,
      enrolled_hours_per_day: enrolled,
      child_id_number: child.child_id_number || '',
      case_number: child.case_number || '',
      authorized_hours: child.authorized_hours || '',
      cdc_start_date: child.cdc_start_date || '',
      cdc_end_date: child.cdc_end_date || 'ONGOING',
      family_contribution: child.family_contribution == null ? 0 : parseFloat(child.family_contribution),
      child_care_fees: child.child_care_fees == null ? 0 : parseFloat(child.child_care_fees),
      total_hours: totalHours,
      normal_hours: normalHours,
      holiday_absent_hours: holidayAbsentHours,
      excluded_after: excludedAfter,
      inactive_after: inactiveAfter,
      days: dayRows,
    });
  }

  return results;
}

function timeStrToMin(t) {
  if (!t) return null;
  if (typeof t === 'string') return parseTime12(t);
  // Postgres TIME returns like "13:45:00"
  const s = String(t);
  const m = s.match(/(\d{1,2}):(\d{2})/);
  if (!m) return null;
  return parseInt(m[1])*60 + parseInt(m[2]);
}

function minToTimeAMPM(mins) {
  if (mins == null) return '';
  let h = Math.floor(mins / 60);
  const m = mins % 60;
  const suffix = h >= 12 ? 'PM' : 'AM';
  if (h === 0) h = 12;
  else if (h > 12) h -= 12;
  return { hhmm: `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}`, ampm: suffix };
}

// ── Wizard Endpoint: Upload one center's attendance for a period ─────────────
// Separate from the regular /api/upload/attendance because this one is scoped
// to a specific CDC period and is part of the wizard flow.
app.post('/api/cdc-filing/upload-attendance', ssoAuth, upload.single('file'), async (req, res) => {
  const startTime = Date.now();
  const { center, period_number } = req.body;
  if (!req.file || !center || !period_number) return res.status(400).json({ error: 'Missing center, period_number, or file' });

  try {
    const periodQ = await pool.query(`SELECT start_date,end_date FROM billing_periods WHERE period_number=$1`, [period_number]);
    if (!periodQ.rows[0]) return res.status(400).json({ error: `Unknown period: ${period_number}` });
    const { start_date, end_date } = periodQ.rows[0];
    const periodStartISO = start_date.toISOString().split('T')[0];
    const periodEndISO = end_date.toISOString().split('T')[0];

    let records;
    try {
      records = parsePlaygroundAttendanceCSV(req.file.buffer);
    } catch (e) { return res.status(400).json({ error: 'CSV parse error: ' + e.message }); }

    // ── PASS 1: extract clean rows, collect unique children ────────────────────
    const cleanRows = [];
    const uniqueChildren = new Map(); // "first|last" → {first, last}

    for (const row of records) {
      const lastName = pickAttendanceField(row, ['Last name','Last Name','last_name','LastName']);
      const firstName = pickAttendanceField(row, ['First name','First Name','first_name','FirstName']);
      const dateStr = pickAttendanceField(row, ['Date','date']);
      const checkin = pickAttendanceField(row, ['Check-in','Check In','checkin','CheckIn']);
      const checkout = pickAttendanceField(row, ['Check-out','Check Out','checkout','CheckOut']);
      if (!lastName || !dateStr) continue;

      const dateObj = new Date(dateStr);
      if (isNaN(dateObj)) continue;
      const attendDateISO = dateObj.toISOString().split('T')[0];
      if (attendDateISO < periodStartISO || attendDateISO > periodEndISO) continue;

      const isAbsent = /absent/i.test(checkin) || checkin === '-' || !checkin;
      const key = `${firstName.toLowerCase()}|${lastName.toLowerCase()}`;
      uniqueChildren.set(key, { first: firstName, last: lastName });

      // Strip the giant signature URLs before storing raw_row to keep DB small
      const cleanRow = {};
      for (const [k, v] of Object.entries(row)) {
        if (/^Signature/i.test(k)) continue; // skip signature URL columns
        cleanRow[k] = v;
      }

      cleanRows.push({
        firstName, lastName, attendDateISO, checkin, checkout, isAbsent, cleanRow, key,
      });
    }

    // ── PASS 2: bulk upsert children — one query for all ────────────────────────
    // Use a VALUES clause with all unique children, then ON CONFLICT to get IDs back
    const childEntries = [...uniqueChildren.values()];
    const childIdMap = new Map(); // key → id
    let autoCreated = 0;

    if (childEntries.length) {
      // Check which children exist first (single query)
      const firstNames = childEntries.map(c => c.first);
      const lastNames = childEntries.map(c => c.last);
      const existingQ = await pool.query(
        `SELECT id, first_name, last_name FROM children
         WHERE center=$1 AND (LOWER(first_name), LOWER(last_name)) = ANY(
           SELECT LOWER(unnest($2::text[])), LOWER(unnest($3::text[]))
         )`,
        [center, firstNames, lastNames]
      );
      for (const r of existingQ.rows) {
        childIdMap.set(`${r.first_name.toLowerCase()}|${r.last_name.toLowerCase()}`, r.id);
      }

      // Insert missing children in one multi-values statement
      const missing = childEntries.filter(c => !childIdMap.has(`${c.first.toLowerCase()}|${c.last.toLowerCase()}`));
      if (missing.length) {
        const placeholders = missing.map((_, i) => `($1, $${i*2+2}, $${i*2+3}, 'unknown', 'Auto-created from CDC filing wizard attendance upload')`).join(',');
        const params = [center];
        for (const m of missing) params.push(m.first, m.last);
        const insertedQ = await pool.query(
          `INSERT INTO children (center, first_name, last_name, subsidy_type, notes)
           VALUES ${placeholders}
           ON CONFLICT (center, first_name, last_name) DO UPDATE SET notes=EXCLUDED.notes
           RETURNING id, first_name, last_name`,
          params
        );
        for (const r of insertedQ.rows) {
          childIdMap.set(`${r.first_name.toLowerCase()}|${r.last_name.toLowerCase()}`, r.id);
        }
        autoCreated = missing.length;
      }
    }

    // ── PASS 3: create flags for auto-created children (one bulk insert) ──────
    const flagsList = [];
    const missingWithIds = childEntries.filter(c => {
      const key = `${c.first.toLowerCase()}|${c.last.toLowerCase()}`;
      return childIdMap.has(key);
    });
    // Only flag children we just auto-created — distinguish by checking if they already had flags
    // Simpler: just create flags for children that got inserted as 'unknown' — they'll all have that note
    if (autoCreated > 0) {
      const unknownKidsQ = await pool.query(
        `SELECT id, first_name, last_name FROM children
         WHERE center=$1 AND subsidy_type='unknown' AND id = ANY($2::int[])`,
        [center, [...childIdMap.values()]]
      );
      if (unknownKidsQ.rows.length) {
        const flagPlaceholders = unknownKidsQ.rows.map((_, i) => `($1, $${i*2+2}, $${i*2+3}, 'NEW_CHILD_NO_STATUS', $${childEntries.length*2+2}, $${childEntries.length*2+3})`).join(',');
        const flagParams = [center];
        for (const kid of unknownKidsQ.rows) {
          flagParams.push(kid.id, `${kid.first_name} ${kid.last_name}`);
        }
        flagParams.push(`Child added from CDC filing attendance upload — please set subsidy status`, period_number);

        // Simpler approach: loop but in parallel with Promise.all — way faster than serial
        await Promise.all(unknownKidsQ.rows.map(kid => pool.query(
          `INSERT INTO billing_flags (center, child_id, child_name, flag_type, flag_detail, period_number)
           VALUES ($1, $2, $3, 'NEW_CHILD_NO_STATUS', $4, $5)`,
          [center, kid.id, `${kid.first_name} ${kid.last_name}`,
           `Child added from CDC filing attendance upload — please set subsidy status`,
           period_number]
        )));

        for (const kid of unknownKidsQ.rows) {
          flagsList.push({ childName: `${kid.first_name} ${kid.last_name}`, type: 'NEW_CHILD_NO_STATUS' });
        }
      }
    }

    // ── PASS 4: bulk insert all attendance records in chunks ────────────────────
    // One big INSERT with multi-values is ~100x faster than 500 sequential INSERTs
    const CHUNK_SIZE = 200;
    let processed = 0, rowsWithBlocks = 0;

    for (let chunkStart = 0; chunkStart < cleanRows.length; chunkStart += CHUNK_SIZE) {
      const chunk = cleanRows.slice(chunkStart, chunkStart + CHUNK_SIZE);
      const valuesSql = [];
      const params = [];
      let p = 0;
      for (const r of chunk) {
        const childId = childIdMap.get(r.key);
        if (!childId) continue;
        params.push(
          center,                           // $p+1
          childId,                          // $p+2
          r.firstName,                      // $p+3
          r.lastName,                       // $p+4
          r.attendDateISO,                  // $p+5
          r.isAbsent ? null : parseTime12(r.checkin),  // $p+6
          r.isAbsent ? null : parseTime12(r.checkout), // $p+7
          r.isAbsent,                       // $p+8
          JSON.stringify(r.cleanRow)        // $p+9
        );
        valuesSql.push(`($${p+1},$${p+2},$${p+3},$${p+4},$${p+5},$${p+6},$${p+7},$${p+8},$${p+9})`);
        p += 9;
        processed++;
        if (!r.isAbsent) rowsWithBlocks++;
      }
      if (valuesSql.length) {
        await pool.query(
          `INSERT INTO attendance_records
             (center, child_id, child_first, child_last, attend_date, checkin_time, checkout_time, is_absent, raw_row)
           VALUES ${valuesSql.join(',')}`,
          params
        );
      }
    }

    const ms = Date.now() - startTime;
    console.log(`[cdc-filing upload] ${center} period ${period_number}: ${processed} records, ${autoCreated} new kids, ${ms}ms`);

    res.json({
      center,
      period_number,
      period_start: start_date,
      period_end: end_date,
      processed,
      autoCreated,
      rowsWithBlocks,
      elapsed_ms: ms,
      flags: flagsList,
    });
  } catch (e) {
    console.error(`[cdc-filing upload] ERROR for ${center} period ${period_number}:`, e);
    res.status(500).json({ error: 'Upload failed: ' + e.message, stack: e.stack?.split('\n').slice(0,3).join('\n') });
  }
});

// ── Wizard Endpoint: Generate the filing ─────────────────────────────────────
app.post('/api/cdc-filing/generate', ssoAuth, async (req, res) => {
  const { period_number, centers } = req.body;
  if (!period_number || !centers || !centers.length) return res.status(400).json({ error: 'Missing period_number or centers' });

  const periodQ = await pool.query(`SELECT * FROM billing_periods WHERE period_number=$1`, [period_number]);
  if (!periodQ.rows[0]) return res.status(400).json({ error: 'Unknown period' });
  const period = periodQ.rows[0];
  const periodStart = period.start_date.toISOString().split('T')[0];
  const periodEnd = period.end_date.toISOString().split('T')[0];

  const noSchoolQ = await pool.query(`SELECT school_date,center FROM no_school_days WHERE school_date BETWEEN $1 AND $2`,
    [periodStart, periodEnd]);

  const byCenter = {};
  for (const center of centers) {
    const noSchoolSet = new Set(
      noSchoolQ.rows
        .filter(r => !r.center || r.center === center || r.center === centerKey(center))
        .map(r => r.school_date.toISOString().split('T')[0])
    );

    const childrenQ = await pool.query(
      `SELECT id,first_name,last_name,is_gsrp,is_cdc,is_school_age,is_active,inactive_date,enrolled_hours_per_day,
              child_id_number,case_number,authorized_hours,cdc_start_date,cdc_end_date,family_contribution,child_care_fees
       FROM children
       WHERE center=$1 AND is_cdc=true
       ORDER BY last_name,first_name`,
      [center]
    );

    const attendanceQ = await pool.query(
      `SELECT child_id,attend_date,checkin_time,checkout_time,is_absent
       FROM attendance_records
       WHERE center=$1 AND attend_date BETWEEN $2 AND $3`,
      [center, periodStart, periodEnd]
    );

    byCenter[center] = {
      center,
      period_start: periodStart,
      period_end: periodEnd,
      children: computeCDCFiling(childrenQ.rows, attendanceQ.rows, noSchoolSet, center, periodStart, periodEnd),
    };
  }

  res.json({
    period_number,
    period_start: periodStart,
    period_end: periodEnd,
    reporting_deadline: period.reporting_deadline,
    centers: byCenter,
  });
});

// ── Wizard Endpoint: Download the filing as CSV ──────────────────────────────
// Format TBD — Mary will upload a DHS sample to match. For now, provide two
// defaults (per-child totals and per-child per-day) so she can validate the math.
app.get('/api/cdc-filing/download', ssoAuth, async (req, res) => {
  const { period_number, center, format } = req.query; // format: 'totals' | 'daily'
  if (!period_number || !center) return res.status(400).json({ error: 'Missing period_number or center' });

  const periodQ = await pool.query(`SELECT * FROM billing_periods WHERE period_number=$1`, [period_number]);
  if (!periodQ.rows[0]) return res.status(400).json({ error: 'Unknown period' });
  const period = periodQ.rows[0];
  const periodStart = period.start_date.toISOString().split('T')[0];
  const periodEnd = period.end_date.toISOString().split('T')[0];

  const noSchoolQ = await pool.query(`SELECT school_date FROM no_school_days WHERE school_date BETWEEN $1 AND $2`, [periodStart, periodEnd]);
  const noSchoolSet = new Set(noSchoolQ.rows.map(r => r.school_date.toISOString().split('T')[0]));

  const childrenQ = await pool.query(
    `SELECT id,first_name,last_name,is_gsrp,is_cdc,is_school_age,is_active,inactive_date,enrolled_hours_per_day,
            child_id_number,case_number,authorized_hours,cdc_start_date,cdc_end_date,family_contribution,child_care_fees
     FROM children WHERE center=$1 AND is_cdc=true ORDER BY last_name,first_name`, [center]);
  const attendanceQ = await pool.query(
    `SELECT child_id,attend_date,checkin_time,checkout_time,is_absent
     FROM attendance_records WHERE center=$1 AND attend_date BETWEEN $2 AND $3`, [center, periodStart, periodEnd]);

  const results = computeCDCFiling(childrenQ.rows, attendanceQ.rows, noSchoolSet, center, periodStart, periodEnd);

  let csv;
  if (format === 'daily') {
    const lines = ['Last Name,First Name,Date,Hours,Status,Holiday/Absent Flag'];
    for (const r of results) {
      for (const d of r.days) {
        if (d.excluded) continue;
        lines.push([
          r.last_name, r.first_name, d.date,
          (d.hours||0).toFixed(2),
          d.status,
          d.is_holiday_absent ? 'YES' : ''
        ].map(v => `"${String(v).replace(/"/g,'""')}"`).join(','));
      }
    }
    csv = lines.join('\n');
  } else {
    // Default: per-child totals
    const lines = ['Last Name,First Name,Period Start,Period End,Total Hours,Normal Hours,Holiday/Absent Hours,Excluded After,Inactive After'];
    for (const r of results) {
      lines.push([
        r.last_name, r.first_name, periodStart, periodEnd,
        r.total_hours.toFixed(2),
        r.normal_hours.toFixed(2),
        r.holiday_absent_hours.toFixed(2),
        r.excluded_after || '',
        r.inactive_after || ''
      ].map(v => `"${String(v).replace(/"/g,'""')}"`).join(','));
    }
    csv = lines.join('\n');
  }

  const filename = `CDC_Filing_${center}_Period${period_number}_${format||'totals'}.csv`;
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(csv);
});

// ── Wizard Endpoint: DHS-Matching Printable Filing Form ──────────────────────
// Returns a full HTML page that mirrors the DHS MiLogin filing form layout.
// Supports both browser print (opens in new tab, user prints) and PDF download.
app.get('/api/cdc-filing/print', ssoAuth, async (req, res) => {
  const { period_number, center } = req.query;
  if (!period_number || !center) return res.status(400).send('Missing period_number or center');

  const periodQ = await pool.query(`SELECT * FROM billing_periods WHERE period_number=$1`, [period_number]);
  if (!periodQ.rows[0]) return res.status(400).send('Unknown period');
  const period = periodQ.rows[0];
  const periodStart = period.start_date.toISOString().split('T')[0];
  const periodEnd = period.end_date.toISOString().split('T')[0];

  const noSchoolQ = await pool.query(`SELECT school_date FROM no_school_days WHERE school_date BETWEEN $1 AND $2`, [periodStart, periodEnd]);
  const noSchoolSet = new Set(noSchoolQ.rows.map(r => r.school_date.toISOString().split('T')[0]));

  const childrenQ = await pool.query(
    `SELECT id,first_name,last_name,is_gsrp,is_cdc,is_school_age,is_active,inactive_date,enrolled_hours_per_day,
            child_id_number,case_number,authorized_hours,cdc_start_date,cdc_end_date,family_contribution,child_care_fees
     FROM children WHERE center=$1 AND is_cdc=true ORDER BY last_name,first_name`, [center]);
  const attendanceQ = await pool.query(
    `SELECT child_id,attend_date,checkin_time,checkout_time,is_absent
     FROM attendance_records WHERE center=$1 AND attend_date BETWEEN $2 AND $3`, [center, periodStart, periodEnd]);

  const results = computeCDCFiling(childrenQ.rows, attendanceQ.rows, noSchoolSet, center, periodStart, periodEnd);

  // Generate list of every date in the period with day-of-week label
  const dates = eachDateInRange(periodStart, periodEnd);
  const dowLabels = ['SUN','MON','TUE','WED','THU','FRI','SAT'];

  const fmtMMDDYYYY = (iso) => {
    const d = new Date(iso + 'T00:00:00Z');
    const mm = String(d.getUTCMonth()+1).padStart(2,'0');
    const dd = String(d.getUTCDate()).padStart(2,'0');
    return `${mm}/${dd}/${d.getUTCFullYear()}`;
  };
  const fmtDDMONYYYY = (iso) => {
    if (!iso) return '';
    const d = new Date(iso + (String(iso).includes('T')?'':'T00:00:00Z'));
    if (isNaN(d)) return String(iso);
    const months = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'];
    return `${String(d.getUTCDate()).padStart(2,'0')}-${months[d.getUTCMonth()]}-${d.getUTCFullYear()}`;
  };

  // Compute per-child totals
  function formatTotalHHMM(hours) {
    if (!hours) return '0:00';
    const h = Math.floor(hours);
    const m = Math.round((hours - h) * 60);
    return `${h}:${String(m).padStart(2,'0')}`;
  }

  // Render one child's block
  function renderChildBlock(child, bgShade) {
    // Map day rows by date for fast lookup
    const dayByDate = {};
    for (const d of child.days) dayByDate[d.date] = d;

    const totalCareHours = child.normal_hours || 0;
    const totalAbsenceHours = child.holiday_absent_hours || 0;

    // Warning banner for children who need special attention
    let warningBanner = '';
    if (child.excluded_after) {
      warningBanner = `<div class="warn-banner">⚠ EXCLUDED from filing after ${fmtMMDDYYYY(child.excluded_after)} (21+ consecutive absence days). Do NOT submit this child.</div>`;
    } else if (child.inactive_after) {
      warningBanner = `<div class="warn-banner">⚠ Child is INACTIVE as of ${fmtMMDDYYYY(child.inactive_after)}. Do not fill in data past this date.</div>`;
    } else if (child.holiday_absent_hours > 0) {
      warningBanner = `<div class="warn-banner warn-absent">⚠ 10+ consecutive school-day absences — check the ABSENT box on highlighted days below.</div>`;
    }

    const headerRow = `
      <div class="child-header">
        <span><u>Child's Name:</u> <strong>${child.last_name}, ${child.first_name}</strong></span>
        <span><u>Child's ID Number:</u> <strong>${child.child_id_number||'_______________'}</strong></span>
        <span><u>Case Number:</u> <strong>${child.case_number||'_______________'}</strong></span>
        <span class="fees">Child Care Fees: <input type="text" value="${child.child_care_fees||0}" style="width:40px"></span>
      </div>
      <div class="child-subheader">
        <span><u>Authorized hours:</u> <strong>${child.authorized_hours||'___'}</strong></span>
        <span><u>Start Date:</u> <strong>${child.cdc_start_date ? fmtDDMONYYYY(child.cdc_start_date) : '___________'}</strong></span>
        <span><u>End Date:</u> <strong style="color:#1a7a3a">${child.cdc_end_date||'ONGOING'}</strong></span>
        <span><u>Family Contribution:</u> <strong>$${child.family_contribution||0}</strong></span>
      </div>`;

    // Header row with day-of-week + date
    const dowRow = dates.map(d => {
      const dow = new Date(d + 'T00:00:00Z').getUTCDay();
      const dd = String(new Date(d+'T00:00:00Z').getUTCDate()).padStart(2,'0');
      return `<td class="dow-cell"><div class="dow">${dowLabels[dow]}</div><div class="dow-date">${dd}</div></td>`;
    }).join('');

    // IN row + OUT row + ABSENT row
    const inCells = dates.map(d => {
      const dow = new Date(d + 'T00:00:00Z').getUTCDay();
      const isWeekend = dow === 0 || dow === 6;
      if (isWeekend) return `<td class="wknd-cell"></td>`;
      const day = dayByDate[d];
      if (!day || day.excluded) {
        if (day && day.status === 'INACTIVE') return `<td class="inactive-cell">INACTIVE</td>`;
        if (day && day.status === 'EXCLUDED_21_DAY') return `<td class="excluded-cell">EXCL</td>`;
        return `<td class="empty-cell"><div class="time-in"></div><div class="ampm">AM</div></td>`;
      }
      if (day.is_holiday_absent) {
        return `<td class="absent-cell"><div class="time-in"></div><div class="ampm">AM</div></td>`;
      }
      const blocks = day.blocks || [];
      if (!blocks.length) {
        return `<td class="empty-cell"><div class="time-in"></div><div class="ampm">AM</div></td>`;
      }
      const primary = blocks[0];
      const hasSecondary = blocks.length > 1;
      return `<td class="data-cell${hasSecondary?' has-multi':''}">
        <div class="time-in">${primary.in_str.hhmm}</div>
        <div class="ampm">${primary.in_str.ampm}</div>
      </td>`;
    }).join('');

    const outCells = dates.map(d => {
      const dow = new Date(d + 'T00:00:00Z').getUTCDay();
      const isWeekend = dow === 0 || dow === 6;
      if (isWeekend) return `<td class="wknd-cell"></td>`;
      const day = dayByDate[d];
      if (!day || day.excluded) {
        if (day && day.status === 'INACTIVE') return `<td class="inactive-cell"></td>`;
        if (day && day.status === 'EXCLUDED_21_DAY') return `<td class="excluded-cell"></td>`;
        return `<td class="empty-cell"><div class="time-out"></div><div class="ampm">PM</div></td>`;
      }
      if (day.is_holiday_absent) {
        return `<td class="absent-cell"><div class="time-out"></div><div class="ampm">PM</div></td>`;
      }
      const blocks = day.blocks || [];
      if (!blocks.length) {
        return `<td class="empty-cell"><div class="time-out"></div><div class="ampm">PM</div></td>`;
      }
      const primary = blocks[0];
      const hasSecondary = blocks.length > 1;
      return `<td class="data-cell${hasSecondary?' has-multi':''}">
        <div class="time-out">${primary.out_str.hhmm}</div>
        <div class="ampm">${primary.out_str.ampm}</div>
      </td>`;
    }).join('');

    // Secondary block row (only shown if any child has multi-block days)
    const hasAnyMulti = dates.some(d => {
      const day = dayByDate[d];
      return day && !day.excluded && (day.blocks||[]).length > 1;
    });

    let secondaryRow = '';
    if (hasAnyMulti) {
      const sec2In = dates.map(d => {
        const dow = new Date(d + 'T00:00:00Z').getUTCDay();
        if (dow === 0 || dow === 6) return `<td class="wknd-cell"></td>`;
        const day = dayByDate[d];
        if (!day || day.excluded || !(day.blocks||[]).length || day.blocks.length < 2) return `<td class="empty-cell-sm"></td>`;
        const b = day.blocks[1];
        return `<td class="data-cell-multi"><div class="time-in">${b.in_str.hhmm}</div><div class="ampm">${b.in_str.ampm}</div></td>`;
      }).join('');
      const sec2Out = dates.map(d => {
        const dow = new Date(d + 'T00:00:00Z').getUTCDay();
        if (dow === 0 || dow === 6) return `<td class="wknd-cell"></td>`;
        const day = dayByDate[d];
        if (!day || day.excluded || !(day.blocks||[]).length || day.blocks.length < 2) return `<td class="empty-cell-sm"></td>`;
        const b = day.blocks[1];
        return `<td class="data-cell-multi"><div class="time-out">${b.out_str.hhmm}</div><div class="ampm">${b.out_str.ampm}</div></td>`;
      }).join('');
      secondaryRow = `
        <tr class="multi-row"><td class="row-label">IN #2:</td>${sec2In}</tr>
        <tr class="multi-row"><td class="row-label">OUT #2:</td>${sec2Out}</tr>`;
    }

    const absentCells = dates.map(d => {
      const dow = new Date(d + 'T00:00:00Z').getUTCDay();
      if (dow === 0 || dow === 6) return `<td class="wknd-cell"></td>`;
      const day = dayByDate[d];
      const checkAbsent = day && day.is_holiday_absent;
      return `<td class="absent-check-cell${checkAbsent?' mark-absent':''}">
        <input type="checkbox" ${checkAbsent?'checked':''}> ABSENT
      </td>`;
    }).join('');

    return `
      <div class="child-block ${bgShade}">
        ${warningBanner}
        ${headerRow}
        <table class="dhs-grid">
          <tr class="dow-row"><td class="row-label corner"></td>${dowRow}</tr>
          <tr><td class="row-label">IN:</td>${inCells}</tr>
          <tr><td class="row-label">OUT:</td>${outCells}</tr>
          ${secondaryRow}
          <tr class="absent-row"><td class="row-label"></td>${absentCells}</tr>
        </table>
        <div class="totals-row">
          <span><strong>TOTAL: (hh:mm)</strong> <span class="total-val">${formatTotalHHMM(child.total_hours)}</span></span>
          <span>Total Care Hours: <strong>${(totalCareHours).toFixed(2)}</strong></span>
          <span>Total Absence Hours: <strong>${(totalAbsenceHours).toFixed(2)}</strong></span>
        </div>
      </div>`;
  }

  // Filter out children who shouldn't be filed (all excluded, no data)
  const filingChildren = results.filter(c => !(c.days.every(d => d.excluded)));

  const html = `<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>CDC Filing — ${center} — Period ${period_number}</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Helvetica', 'Arial', sans-serif; font-size: 9px; background: #fff; color: #000; padding: 6mm; }

  .page-header {
    background: #2e5fa8;
    color: #fff;
    padding: 6px 14px;
    display: flex;
    justify-content: space-between;
    font-size: 11px;
    font-weight: 600;
    margin-bottom: 4px;
  }

  .toolbar {
    display: flex; gap: 10px; padding: 8px 0;
    border-bottom: 1px solid #ccc; margin-bottom: 10px;
  }
  .toolbar button {
    padding: 6px 14px; cursor: pointer; border: 1px solid #2e5fa8;
    background: #2e5fa8; color: #fff; font-size: 12px; border-radius: 4px;
  }
  .toolbar button.ghost { background: #fff; color: #2e5fa8; }

  .child-block {
    margin-bottom: 3px;
    padding: 5px 8px;
    page-break-inside: avoid;
    break-inside: avoid;
  }
  .child-block.shade-a { background: #d9e1ec; }
  .child-block.shade-b { background: #ffffff; }

  .warn-banner {
    background: #fff4cf; border: 1px solid #d6a100;
    padding: 3px 8px; font-size: 9px; font-weight: 600;
    margin-bottom: 4px; color: #7a5c00;
  }
  .warn-banner.warn-absent { background: #fdecea; border-color: #c0392b; color: #802317; }

  .child-header, .child-subheader {
    display: flex; gap: 14px; flex-wrap: wrap; align-items: center;
    font-size: 9.5px; padding: 1px 0;
  }
  .child-header .fees { margin-left: auto; }
  .child-header input { border: 1px solid #333; padding: 1px 3px; font-size: 9px; }

  .dhs-grid {
    width: 100%;
    border-collapse: collapse;
    margin-top: 3px;
    table-layout: fixed;
  }
  .dhs-grid td {
    border: 1px solid #666;
    text-align: center;
    vertical-align: middle;
    padding: 1px;
    height: 22px;
    font-size: 8.5px;
  }
  .row-label {
    background: #f0f0f0; font-weight: 600; font-size: 9px;
    text-align: left; padding: 2px 4px; width: 45px;
  }
  .corner { background: transparent; border: none; }

  .dow-row td { background: transparent; border: none; font-weight: 600; padding: 2px 1px; }
  .dow { font-size: 8.5px; color: #333; }
  .dow-date { font-size: 10px; font-weight: 600; }

  .wknd-cell { background: #e8e8e8; color: #999; }
  .empty-cell { background: #fff; }
  .empty-cell-sm { background: #fff; height: 16px; }

  .data-cell { background: #fff; }
  .data-cell .time-in, .data-cell .time-out {
    font-size: 10px; font-weight: 600; line-height: 1.1;
  }
  .data-cell .ampm { font-size: 7.5px; color: #555; }

  .data-cell.has-multi { background: #fff2d4; } /* marker that secondary block exists */

  .data-cell-multi { background: #fff6b8; } /* Secondary block row — GOLD highlight */
  .data-cell-multi .time-in, .data-cell-multi .time-out { font-size: 9.5px; font-weight: 600; }
  .data-cell-multi .ampm { font-size: 7px; }

  .multi-row .row-label { background: #fff6b8; font-style: italic; }

  .absent-cell { background: #fdecea !important; }
  .absent-check-cell {
    font-size: 7px; background: transparent; border: none !important;
    padding: 1px 0;
  }
  .absent-check-cell input { transform: scale(0.85); vertical-align: middle; }
  .absent-check-cell.mark-absent {
    background: #fdecea !important; font-weight: 600; color: #802317;
  }
  .absent-row td.wknd-cell { background: #e8e8e8; }

  .inactive-cell, .excluded-cell {
    background: #333; color: #fff; font-size: 7.5px; font-weight: 600;
  }

  .totals-row {
    display: flex; gap: 20px; padding: 2px 4px; font-size: 9px;
    border-top: 1px dashed #999; margin-top: 2px;
  }
  .total-val { font-family: monospace; background: #fff; border: 1px solid #999; padding: 0 4px; }

  .summary-bar {
    background: #1a2744; color: #fff; padding: 6px 14px;
    margin-bottom: 10px; font-size: 11px;
    display: flex; justify-content: space-between;
  }

  .legend {
    display: flex; gap: 14px; font-size: 8.5px; padding: 6px 4px;
    border-top: 1px solid #ccc; margin-top: 10px;
  }
  .legend span { display: flex; align-items: center; gap: 4px; }
  .legend .swatch {
    display: inline-block; width: 14px; height: 10px; border: 1px solid #666;
  }

  @media print {
    .toolbar { display: none; }
    body { padding: 4mm; font-size: 8.5px; }
    @page { size: landscape; margin: 8mm; }
    .child-block { margin-bottom: 2px; padding: 3px 6px; }
    .dhs-grid td { height: 20px; }
  }
</style>
</head>
<body>

<div class="toolbar">
  <button onclick="window.print()">🖨 Print</button>
  <button class="ghost" onclick="window.close()">Close</button>
  <span style="margin-left:auto;font-size:12px;color:#555;align-self:center">
    ${filingChildren.length} CDC children · Period ${period_number} · ${center}
  </span>
</div>

<div class="page-header">
  <span>Start Date: ${fmtMMDDYYYY(periodStart)}</span>
  <span>End Date: ${fmtMMDDYYYY(periodEnd)}</span>
  <span>Pay Period Number: ${period_number}</span>
</div>

<div class="summary-bar">
  <span><strong>${center}</strong> — The Children's Center</span>
  <span>${filingChildren.length} children to file</span>
  <span>Generated ${new Date().toLocaleString('en-US', { month:'short', day:'numeric', year:'numeric', hour:'numeric', minute:'2-digit' })}</span>
</div>

${filingChildren.length === 0
  ? `<div style="padding:40px;text-align:center;color:#888">No CDC children to file for ${center} in this period.</div>`
  : filingChildren.map((c, i) => renderChildBlock(c, i % 2 === 0 ? 'shade-a' : 'shade-b')).join('')
}

<div class="legend">
  <span><span class="swatch" style="background:#fff6b8"></span> Secondary in/out time (child left &amp; returned)</span>
  <span><span class="swatch" style="background:#fdecea"></span> 10+ day absence — check ABSENT box</span>
  <span><span class="swatch" style="background:#333"></span> Inactive / Excluded (don't fill in)</span>
  <span><span class="swatch" style="background:#e8e8e8"></span> Weekend</span>
</div>

</body></html>`;

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(html);
});

// ── Wizard Endpoint: DHS Filing Form as PDF ──────────────────────────────────
// Uses puppeteer to render the HTML form to PDF for download
app.get('/api/cdc-filing/pdf', ssoAuth, async (req, res) => {
  const { period_number, center } = req.query;
  if (!period_number || !center) return res.status(400).json({ error: 'Missing period_number or center' });

  let puppeteer;
  try { puppeteer = require('puppeteer'); }
  catch (e) {
    return res.status(501).json({
      error: 'PDF generation not available on this instance.',
      hint: 'Add "puppeteer": "^22.0.0" to package.json dependencies and redeploy. Until then, use Print from the browser.'
    });
  }

  try {
    // Build the URL for the print-friendly HTML endpoint on ourselves
    const token = req.query.token || req.headers['authorization']?.replace('Bearer ', '') || '';
    const host = req.headers.host;
    const proto = req.headers['x-forwarded-proto'] || 'http';
    const url = `${proto}://${host}/api/cdc-filing/print?period_number=${period_number}&center=${encodeURIComponent(center)}${token?`&token=${token}`:''}`;

    const browser = await puppeteer.launch({ args: ['--no-sandbox','--disable-setuid-sandbox'] });
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: 'networkidle0', timeout: 30000 });
    const pdfBuffer = await page.pdf({
      landscape: true,
      format: 'Letter',
      margin: { top: '8mm', bottom: '8mm', left: '8mm', right: '8mm' },
      printBackground: true,
    });
    await browser.close();

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="CDC_Filing_${center}_Period${period_number}.pdf"`);
    res.send(pdfBuffer);
  } catch (e) {
    res.status(500).json({ error: 'PDF render failed: ' + e.message });
  }
});


app.delete('/api/cdc-filing/attendance', ssoAuth, async (req, res) => {
  const { period_number, center } = req.query;
  if (!period_number || !center) return res.status(400).json({ error: 'Missing period_number or center' });

  const periodQ = await pool.query(`SELECT start_date,end_date FROM billing_periods WHERE period_number=$1`, [period_number]);
  if (!periodQ.rows[0]) return res.status(400).json({ error: 'Unknown period' });
  const { start_date, end_date } = periodQ.rows[0];

  const { rowCount } = await pool.query(
    `DELETE FROM attendance_records WHERE center=$1 AND attend_date BETWEEN $2 AND $3`,
    [center, start_date, end_date]
  );
  res.json({ deleted: rowCount });
});

// ── Wizard Endpoint: Which centers already have attendance for this period? ──
app.get('/api/cdc-filing/status', ssoAuth, async (req, res) => {
  const { period_number } = req.query;
  if (!period_number) return res.status(400).json({ error: 'Missing period_number' });

  const periodQ = await pool.query(`SELECT start_date,end_date FROM billing_periods WHERE period_number=$1`, [period_number]);
  if (!periodQ.rows[0]) return res.status(400).json({ error: 'Unknown period' });
  const { start_date, end_date } = periodQ.rows[0];

  const q = await pool.query(
    `SELECT center, COUNT(*) AS records, COUNT(DISTINCT child_id) AS children
     FROM attendance_records WHERE attend_date BETWEEN $1 AND $2 GROUP BY center`,
    [start_date, end_date]
  );
  const status = { Niles: null, Peace: null, Montessori: null };
  for (const row of q.rows) {
    if (status[row.center] !== undefined) status[row.center] = { records: +row.records, children: +row.children };
  }
  res.json({ period_number, period_start: start_date, period_end: end_date, status });
});



// ═════════════════════════════════════════════════════════════════════════════
// ── Collections (Past-Due / Departed Families) ──────────────────────────────
// ═════════════════════════════════════════════════════════════════════════════
// Families who left TCC with a balance, or who are very far behind. Each family
// gets two Stripe Payment Links:
//   1. "Pay in Full 50% off" — one-time, amount = remaining_balance / 2
//   2. "Pay What You Can" — flexible, $25 minimum
// Webhook events from Stripe update balances, payments, and event log in real time.

const COLLECTIONS_MIN_PAYMENT_CENTS = 2500; // $25 minimum for pay-what-you-can

async function handleStripeEvent(event) {
  const type = event.type;
  const obj = event.data.object;

  async function findFamilyIdFromEvent() {
    const metadata = obj.metadata || {};
    if (metadata.family_id) return parseInt(metadata.family_id);
    if (obj.payment_intent) {
      try {
        const pi = await stripe.paymentIntents.retrieve(obj.payment_intent);
        if (pi.metadata?.family_id) return parseInt(pi.metadata.family_id);
      } catch { /* ignore */ }
    }
    const email = obj.customer_email || obj.receipt_email;
    if (email) {
      const { rows } = await pool.query(
        `SELECT id FROM collections_families WHERE LOWER(primary_contact_email)=LOWER($1) LIMIT 1`,
        [email]
      );
      if (rows[0]) return rows[0].id;
    }
    return null;
  }

  const familyId = await findFamilyIdFromEvent();

  await pool.query(
    `INSERT INTO collections_events (family_id, event_type, stripe_event_id, detail, amount, raw_event)
     VALUES ($1,$2,$3,$4,$5,$6)
     ON CONFLICT (stripe_event_id) DO NOTHING`,
    [familyId, type, event.id, describeEvent(event), eventAmount(event), JSON.stringify(event)]
  );

  switch (type) {
    case 'payment_intent.succeeded':
    case 'checkout.session.completed': {
      const pi = type === 'checkout.session.completed' && obj.payment_intent
        ? await stripe.paymentIntents.retrieve(obj.payment_intent)
        : obj;
      const amountCents = pi.amount_received || pi.amount || 0;
      const amount = amountCents / 100;
      const linkType = pi.metadata?.link_type || obj.metadata?.link_type || null;
      await pool.query(
        `INSERT INTO collections_payments
         (family_id, stripe_payment_intent_id, stripe_charge_id, stripe_customer_email,
          amount, currency, status, paid_at, link_type, raw_event)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT (stripe_payment_intent_id) DO UPDATE
           SET status='succeeded', amount=EXCLUDED.amount, paid_at=EXCLUDED.paid_at`,
        [familyId, pi.id, pi.latest_charge || null, pi.receipt_email || obj.customer_email || null,
         amount, pi.currency || 'usd', 'succeeded', new Date(pi.created * 1000), linkType,
         JSON.stringify(event)]
      );
      if (linkType === 'pay_in_full' && familyId) {
        await pool.query(
          `UPDATE collections_families SET status='settled', settled_at=NOW(), updated_at=NOW() WHERE id=$1`,
          [familyId]
        );
      }
      break;
    }
    case 'payment_intent.payment_failed': {
      const pi = obj;
      await pool.query(
        `INSERT INTO collections_payments
         (family_id, stripe_payment_intent_id, stripe_customer_email, amount, currency,
          status, failure_reason, link_type, raw_event)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT (stripe_payment_intent_id) DO UPDATE
           SET status='failed', failure_reason=EXCLUDED.failure_reason`,
        [familyId, pi.id, pi.receipt_email || null, (pi.amount || 0) / 100, pi.currency || 'usd',
         'failed', pi.last_payment_error?.message || 'Unknown failure',
         pi.metadata?.link_type || null, JSON.stringify(event)]
      );
      break;
    }
    case 'charge.refunded': {
      const ch = obj;
      await pool.query(
        `UPDATE collections_payments
         SET amount_refunded=$1, status=CASE WHEN $1 >= amount*100 THEN 'refunded' ELSE status END
         WHERE stripe_charge_id=$2`,
        [(ch.amount_refunded || 0) / 100, ch.id]
      );
      break;
    }
  }
}

function describeEvent(event) {
  const obj = event.data.object;
  switch (event.type) {
    case 'payment_intent.succeeded':
    case 'checkout.session.completed':
      return `Payment of $${((obj.amount_received||obj.amount||0)/100).toFixed(2)} received`;
    case 'payment_intent.payment_failed':
      return `Payment of $${((obj.amount||0)/100).toFixed(2)} FAILED: ${obj.last_payment_error?.message || 'unknown'}`;
    case 'charge.refunded':
      return `Refund of $${((obj.amount_refunded||0)/100).toFixed(2)}`;
    default:
      return event.type;
  }
}

function eventAmount(event) {
  const obj = event.data.object;
  const cents = obj.amount_received || obj.amount || obj.amount_refunded || 0;
  return cents / 100;
}

async function familyWithBalance(familyId) {
  const { rows: fRows } = await pool.query(`SELECT * FROM collections_families WHERE id=$1`, [familyId]);
  if (!fRows[0]) return null;
  const family = fRows[0];
  const { rows: pRows } = await pool.query(
    `SELECT COALESCE(SUM(amount - amount_refunded), 0) AS total_paid
     FROM collections_payments WHERE family_id=$1 AND status='succeeded'`,
    [familyId]
  );
  const totalPaid = parseFloat(pRows[0].total_paid) || 0;
  const original = parseFloat(family.original_balance) || 0;
  const remaining = Math.max(0, original - totalPaid);
  return {
    ...family,
    total_paid: totalPaid,
    remaining_balance: remaining,
    discount_pay_in_full: remaining / 2,
  };
}

app.get('/api/collections/families', ssoAuth, async (req, res) => {
  const { status } = req.query;
  const params = [];
  let q = `SELECT f.*,
             COALESCE((SELECT SUM(amount - amount_refunded) FROM collections_payments
                       WHERE family_id=f.id AND status='succeeded'), 0) AS total_paid,
             COALESCE((SELECT COUNT(*) FROM collections_payments
                       WHERE family_id=f.id AND status='failed'), 0) AS failed_count
           FROM collections_families f WHERE 1=1`;
  if (status) { params.push(status); q += ` AND f.status=$${params.length}`; }
  q += ` ORDER BY f.status ASC, f.family_name ASC`;
  const { rows } = await pool.query(q, params);
  res.json(rows.map(r => ({
    ...r,
    total_paid: parseFloat(r.total_paid),
    remaining_balance: Math.max(0, parseFloat(r.original_balance) - parseFloat(r.total_paid)),
    discount_pay_in_full: Math.max(0, parseFloat(r.original_balance) - parseFloat(r.total_paid)) / 2,
    failed_count: parseInt(r.failed_count),
  })));
});

app.get('/api/collections/families/:id', ssoAuth, async (req, res) => {
  const family = await familyWithBalance(req.params.id);
  if (!family) return res.status(404).json({ error: 'Not found' });
  const { rows: payments } = await pool.query(
    `SELECT * FROM collections_payments WHERE family_id=$1 ORDER BY created_at DESC`, [req.params.id]);
  const { rows: events } = await pool.query(
    `SELECT * FROM collections_events WHERE family_id=$1 ORDER BY created_at DESC LIMIT 50`, [req.params.id]);
  res.json({ family, payments, events });
});

app.post('/api/collections/families', ssoAuth, async (req, res) => {
  const { family_name, primary_contact_email, primary_contact_phone, children_names,
          original_balance, center, left_date, notes } = req.body;
  if (!family_name || !original_balance) return res.status(400).json({ error: 'family_name and original_balance required' });
  const { rows } = await pool.query(
    `INSERT INTO collections_families
     (family_name, primary_contact_email, primary_contact_phone, children_names,
      original_balance, center, left_date, notes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
    [family_name, primary_contact_email||null, primary_contact_phone||null, children_names||null,
     original_balance, center||null, left_date||null, notes||null]
  );
  res.json(rows[0]);
});

app.put('/api/collections/families/:id', ssoAuth, async (req, res) => {
  const { family_name, primary_contact_email, primary_contact_phone, children_names,
          original_balance, center, left_date, status, notes } = req.body;
  const { rows } = await pool.query(
    `UPDATE collections_families SET
       family_name=COALESCE($1, family_name),
       primary_contact_email=$2, primary_contact_phone=$3, children_names=$4,
       original_balance=COALESCE($5, original_balance),
       center=$6, left_date=$7,
       status=COALESCE($8, status), notes=$9, updated_at=NOW()
     WHERE id=$10 RETURNING *`,
    [family_name||null, primary_contact_email||null, primary_contact_phone||null, children_names||null,
     original_balance==null ? null : original_balance, center||null, left_date||null,
     status||null, notes||null, req.params.id]
  );
  res.json(rows[0]);
});

app.delete('/api/collections/families/:id', ssoAuth, async (req, res) => {
  await pool.query(`DELETE FROM collections_families WHERE id=$1`, [req.params.id]);
  res.json({ success: true });
});

app.post('/api/collections/families/import', ssoAuth, upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  let records;
  try {
    records = parse(req.file.buffer.toString(), { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) { return res.status(400).json({ error: 'CSV parse error: ' + e.message }); }

  let created = 0, skipped = 0;
  const errors = [];
  for (let i = 0; i < records.length; i++) {
    const row = records[i];
    const name = row['Family Name'] || row['family_name'] || row['Name'] || '';
    const balance = parseFloat(row['Balance'] || row['Original Balance'] || row['original_balance'] || row['Amount Owed'] || 0);
    if (!name || !balance) { skipped++; continue; }
    try {
      await pool.query(
        `INSERT INTO collections_families
         (family_name, primary_contact_email, primary_contact_phone, children_names,
          original_balance, center, left_date, notes)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
        [name,
         row['Email'] || row['email'] || null,
         row['Phone'] || row['phone'] || null,
         row['Children'] || row['children'] || row['Children Names'] || null,
         balance,
         row['Center'] || row['center'] || null,
         row['Left Date'] || row['left_date'] || null,
         row['Notes'] || row['notes'] || null]
      );
      created++;
    } catch (e) {
      errors.push(`Row ${i+2}: ${e.message}`);
    }
  }
  res.json({ created, skipped, errors });
});

app.post('/api/collections/families/:id/create-links', ssoAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Stripe not configured. Set STRIPE_SECRET_KEY in Render environment variables.' });

  const family = await familyWithBalance(req.params.id);
  if (!family) return res.status(404).json({ error: 'Family not found' });

  const remainingCents = Math.round(family.remaining_balance * 100);
  if (remainingCents <= 0) return res.status(400).json({ error: 'No remaining balance' });

  const discountCents = Math.round(remainingCents / 2);

  try {
    const payInFullPrice = await stripe.prices.create({
      currency: 'usd',
      unit_amount: discountCents,
      product_data: { name: `Pay in Full — 50% off — ${family.family_name}` },
    });
    const payInFullLink = await stripe.paymentLinks.create({
      line_items: [{ price: payInFullPrice.id, quantity: 1 }],
      metadata: { family_id: String(family.id), link_type: 'pay_in_full' },
      payment_intent_data: {
        metadata: { family_id: String(family.id), link_type: 'pay_in_full' },
      },
      after_completion: { type: 'hosted_confirmation', hosted_confirmation: {
        custom_message: `Thank you! Your balance of $${family.remaining_balance.toFixed(2)} has been paid in full (with 50% discount applied). You will receive a receipt by email.`,
      }},
    });

    const pwycPrice = await stripe.prices.create({
      currency: 'usd',
      custom_unit_amount: {
        enabled: true,
        minimum: COLLECTIONS_MIN_PAYMENT_CENTS,
        preset: Math.min(remainingCents, 10000),
      },
      product_data: { name: `Payment Toward Balance — ${family.family_name}` },
    });
    const pwycLink = await stripe.paymentLinks.create({
      line_items: [{ price: pwycPrice.id, quantity: 1 }],
      metadata: { family_id: String(family.id), link_type: 'pay_what_you_can' },
      payment_intent_data: {
        metadata: { family_id: String(family.id), link_type: 'pay_what_you_can' },
      },
      after_completion: { type: 'hosted_confirmation', hosted_confirmation: {
        custom_message: `Thank you for your payment! Every contribution helps settle your balance with The Children's Center.`,
      }},
    });

    const { rows } = await pool.query(
      `UPDATE collections_families SET
         payinfull_link_url=$1, payinfull_link_id=$2,
         paywhatyoucan_link_url=$3, paywhatyoucan_link_id=$4,
         updated_at=NOW()
       WHERE id=$5 RETURNING *`,
      [payInFullLink.url, payInFullLink.id, pwycLink.url, pwycLink.id, family.id]
    );

    res.json({
      pay_in_full: { url: payInFullLink.url, amount: discountCents / 100 },
      pay_what_you_can: { url: pwycLink.url, minimum: COLLECTIONS_MIN_PAYMENT_CENTS / 100 },
      family: rows[0],
    });
  } catch (e) {
    console.error('Stripe link creation failed:', e);
    res.status(500).json({ error: 'Stripe error: ' + e.message });
  }
});

app.post('/api/collections/families/:id/deactivate-links', ssoAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Stripe not configured' });
  const { rows } = await pool.query(`SELECT payinfull_link_id, paywhatyoucan_link_id FROM collections_families WHERE id=$1`, [req.params.id]);
  if (!rows[0]) return res.status(404).json({ error: 'Not found' });
  const deactivated = [];
  for (const linkId of [rows[0].payinfull_link_id, rows[0].paywhatyoucan_link_id]) {
    if (!linkId) continue;
    try {
      await stripe.paymentLinks.update(linkId, { active: false });
      deactivated.push(linkId);
    } catch (e) { /* skip */ }
  }
  await pool.query(
    `UPDATE collections_families SET payinfull_link_url=NULL, payinfull_link_id=NULL,
       paywhatyoucan_link_url=NULL, paywhatyoucan_link_id=NULL, updated_at=NOW() WHERE id=$1`,
    [req.params.id]
  );
  res.json({ deactivated });
});

app.get('/api/collections/dashboard', ssoAuth, async (req, res) => {
  const [familiesQ, paymentsQ, failedQ, recentQ] = await Promise.all([
    pool.query(`SELECT
      COUNT(*) FILTER (WHERE status='active') as active_count,
      COUNT(*) FILTER (WHERE status='settled') as settled_count,
      COALESCE(SUM(original_balance) FILTER (WHERE status='active'), 0) as total_owed
     FROM collections_families`),
    pool.query(`SELECT
      COALESCE(SUM(amount - amount_refunded), 0) as collected_total,
      COALESCE(SUM(amount - amount_refunded) FILTER (WHERE paid_at >= date_trunc('month', NOW())), 0) as collected_this_month,
      COUNT(*) FILTER (WHERE paid_at >= date_trunc('month', NOW())) as payments_this_month
     FROM collections_payments WHERE status='succeeded'`),
    pool.query(`SELECT p.*, f.family_name FROM collections_payments p
                LEFT JOIN collections_families f ON p.family_id=f.id
                WHERE p.status='failed' AND p.created_at > NOW() - INTERVAL '30 days'
                ORDER BY p.created_at DESC LIMIT 20`),
    pool.query(`SELECT e.*, f.family_name FROM collections_events e
                LEFT JOIN collections_families f ON e.family_id=f.id
                ORDER BY e.created_at DESC LIMIT 20`),
  ]);

  const totalOwed = parseFloat(familiesQ.rows[0].total_owed) || 0;
  const collectedTotal = parseFloat(paymentsQ.rows[0].collected_total) || 0;

  res.json({
    stripe_configured: !!stripe,
    summary: {
      active_families: parseInt(familiesQ.rows[0].active_count),
      settled_families: parseInt(familiesQ.rows[0].settled_count),
      total_owed: totalOwed,
      collected_total: collectedTotal,
      collected_this_month: parseFloat(paymentsQ.rows[0].collected_this_month) || 0,
      payments_this_month: parseInt(paymentsQ.rows[0].payments_this_month),
      outstanding: Math.max(0, totalOwed - collectedTotal),
    },
    failed_payments: failedQ.rows,
    recent_events: recentQ.rows,
  });
});

// ── Serve frontend ───────────────────────────────────────────────────────────
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

initDB().then(() => {
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log(`TCC Billing Hub running on port ${PORT}`));
}).catch(e => { console.error('DB init failed:', e); process.exit(1); });
