require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
const upload = multer({ storage: multer.memoryStorage() });
const JWT_SECRET = process.env.HUB_JWT_SECRET || 'tcc-hub-jwt-2026';

app.use(cors());
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
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(center, first_name, last_name)
    );

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
      payment_date DATE,
      billing_status TEXT DEFAULT 'pending',
      crediting_status TEXT DEFAULT 'pending'
    );
  `);

  // Seed billing periods from DHS spreadsheet
  const periods = [
    ['601','2025-12-28','2026-01-10','2026-01-15','2026-01-22'],
    ['602','2026-01-11','2026-01-24','2026-01-29','2026-02-05'],
    ['603','2026-01-25','2026-02-07','2026-02-12','2026-02-19'],
    ['604','2026-02-08','2026-02-21','2026-02-26','2026-03-05'],
    ['605','2026-02-22','2026-03-07','2026-03-12','2026-03-19'],
    ['606','2026-03-08','2026-03-21','2026-03-26','2026-04-02'],
    ['607','2026-03-22','2026-04-04','2026-04-09','2026-04-16'],
    ['608','2026-04-05','2026-04-18','2026-04-23','2026-04-30'],
    ['609','2026-04-19','2026-05-02','2026-05-07','2026-05-14'],
    ['610','2026-05-03','2026-05-16','2026-05-21','2026-05-28'],
    ['611','2026-05-17','2026-05-30','2026-06-04','2026-06-11'],
    ['612','2026-05-31','2026-06-13','2026-06-18','2026-06-25'],
    ['613','2026-06-14','2026-06-27','2026-07-02','2026-07-09'],
    ['614','2026-06-28','2026-07-11','2026-07-16','2026-07-23'],
  ];
  for (const [p,s,e,r,c] of periods) {
    await pool.query(
      `INSERT INTO billing_periods (period_number,start_date,end_date,reporting_deadline,payment_date)
       VALUES ($1,$2,$3,$4,$5) ON CONFLICT (period_number) DO NOTHING`,
      [p,s,e,r,c]
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
  const { subsidy_type, is_gsrp, is_cdc, is_school_age, is_active, notes } = req.body;
  const { rows } = await pool.query(
    `UPDATE children SET subsidy_type=$1,is_gsrp=$2,is_cdc=$3,is_school_age=$4,
     is_active=$5,notes=$6,updated_at=NOW() WHERE id=$7 RETURNING *`,
    [subsidy_type, is_gsrp, is_cdc, is_school_age, is_active, notes, req.params.id]
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
app.post('/api/upload/attendance', ssoAuth, upload.single('file'), async (req, res) => {
  const { center } = req.body;
  if (!req.file || !center) return res.status(400).json({ error: 'Missing center or file' });

  let records;
  try {
    records = parse(req.file.buffer.toString(), {
      columns: true, skip_empty_lines: true, trim: true
    });
  } catch (e) { return res.status(400).json({ error: 'CSV parse error: ' + e.message }); }

  const centerKey = center.toLowerCase().replace(/\s+/g,'').replace('peace','peace').replace('niles','niles').replace('montessori','montessori');
  const gsrp = GSRP_CONFIG[centerKey] || GSRP_CONFIG['niles'];
  const inserted = [];
  const flags = [];

  for (const row of records) {
    const lastName = row['Last name'] || row['last_name'] || '';
    const firstName = row['First name'] || row['first_name'] || '';
    const dateStr = row['Date'] || row['date'] || '';
    const checkin = row['Check-in'] || row['checkin'] || '';
    const checkout = row['Check-out'] || row['checkout'] || '';
    if (!lastName || !dateStr) continue;

    const isAbsent = /absent/i.test(checkin) || checkin === '-' || !checkin;
    const dateObj = new Date(dateStr);
    const dayOfWeek = dateObj.getDay(); // 0=Sun,1=Mon...

    // Find or auto-create child
    let childRow = await pool.query(
      `SELECT id,is_gsrp,is_cdc,is_school_age FROM children
       WHERE center=$1 AND LOWER(first_name)=LOWER($2) AND LOWER(last_name)=LOWER($3)`,
      [center, firstName, lastName]
    );
    let childId = childRow.rows[0]?.id || null;
    if (!childId) {
      const inserted = await pool.query(
        `INSERT INTO children (center,first_name,last_name,subsidy_type,notes)
         VALUES ($1,$2,$3,'unknown','Auto-created from attendance upload — set subsidy type')
         ON CONFLICT (center,first_name,last_name) DO UPDATE SET notes=EXCLUDED.notes RETURNING id`,
        [center, firstName, lastName]
      );
      childId = inserted.rows[0].id;
      await createFlag(center, childId, `${firstName} ${lastName}`, 'NEW_CHILD_NO_STATUS',
        `Child added from attendance upload with unknown subsidy status — please set status`, null);
      flags.push({ childName: `${firstName} ${lastName}`, type: 'NEW_CHILD_NO_STATUS' });
    }

    await pool.query(
      `INSERT INTO attendance_records (center,child_id,child_first,child_last,attend_date,checkin_time,checkout_time,is_absent,raw_row)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [center, childId, firstName, lastName, dateObj.toISOString().split('T')[0],
       isAbsent ? null : parseTime12(checkin),
       isAbsent ? null : parseTime12(checkout),
       isAbsent, JSON.stringify(row)]
    );

    // GSRP late pickup check
    const child = childRow.rows[0];
    if (child?.is_gsrp && !isAbsent && checkout) {
      const gsrpEndMin = toMinutes(gsrp.end + ' AM') || (gsrp.end.includes(':') ? parseInt(gsrp.end)*60+parseInt(gsrp.end.split(':')[1]) : 0);
      const checkoutMin = toMinutes(checkout);
      const closingMin = toMinutes(CLOSING_TIME + ' PM') || 18*60;

      if (checkoutMin && gsrp.days.includes(dayOfWeek)) {
        const gsrpEndMinutes = timeStrToMin(gsrp.end);
        const checkoutMinutes = toMinutes(checkout);
        if (checkoutMinutes > gsrpEndMinutes) {
          // This is tracked as wraparound, not a late fee — only flag if after closing
        }
        // Check after-closing late pickup (after 6pm)
        if (checkoutMinutes > 18*60) {
          const minsLate = checkoutMinutes - 18*60;
          await createFlag(center, childId, `${firstName} ${lastName}`, 'AFTER_CLOSING_PICKUP',
            `Picked up ${minsLate} min after 6pm on ${dateStr}. Fee: $${calcLateFee(minsLate)}`,
            null, dateObj.toISOString().split('T')[0]);
          flags.push({ childName: `${firstName} ${lastName}`, type: 'AFTER_CLOSING_PICKUP', minsLate });
        }
      }
    }
    inserted.push(`${firstName} ${lastName}`);
  }

  res.json({ processed: inserted.length, flags });
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

// ── Serve frontend ────────────────────────────────────────────────────────────
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

initDB().then(() => {
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log(`TCC Billing Hub running on port ${PORT}`));
}).catch(e => { console.error('DB init failed:', e); process.exit(1); });
