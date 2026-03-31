require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// File upload config
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

// Database
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// ============================================================
// DATABASE INITIALIZATION
// ============================================================
async function initDB() {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS col_accounts (
        id SERIAL PRIMARY KEY,
        family_name VARCHAR(255) NOT NULL,
        parent_first VARCHAR(255),
        parent_last VARCHAR(255),
        email VARCHAR(255),
        phone VARCHAR(50),
        center VARCHAR(100),
        track VARCHAR(20) DEFAULT 'current' CHECK (track IN ('current', 'former')),
        status VARCHAR(30) DEFAULT 'active' CHECK (status IN ('active', 'past_due', 'hold', 'arrangement', 'paid', 'closed')),
        balance NUMERIC(10,2) DEFAULT 0,
        original_balance NUMERIC(10,2) DEFAULT 0,
        last_payment_date DATE,
        days_past_due INTEGER DEFAULT 0,
        stage VARCHAR(30) DEFAULT 'none',
        notes TEXT,
        source VARCHAR(50),
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS col_children (
        id SERIAL PRIMARY KEY,
        account_id INTEGER REFERENCES col_accounts(id) ON DELETE CASCADE,
        child_name VARCHAR(255) NOT NULL,
        classroom VARCHAR(100),
        enrollment_status VARCHAR(30) DEFAULT 'enrolled',
        attendance_hold BOOLEAN DEFAULT false,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS col_payments (
        id SERIAL PRIMARY KEY,
        account_id INTEGER REFERENCES col_accounts(id) ON DELETE CASCADE,
        amount NUMERIC(10,2) NOT NULL,
        payment_date DATE DEFAULT CURRENT_DATE,
        method VARCHAR(50),
        stripe_id VARCHAR(255),
        notes TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS col_communications (
        id SERIAL PRIMARY KEY,
        account_id INTEGER REFERENCES col_accounts(id) ON DELETE CASCADE,
        type VARCHAR(30) NOT NULL,
        channel VARCHAR(30) NOT NULL,
        template_id INTEGER,
        subject VARCHAR(255),
        body TEXT,
        status VARCHAR(30) DEFAULT 'sent',
        sent_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS col_templates (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        channel VARCHAR(30) NOT NULL,
        track VARCHAR(20),
        stage VARCHAR(50),
        subject VARCHAR(255),
        body TEXT NOT NULL,
        active BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS col_payment_plans (
        id SERIAL PRIMARY KEY,
        account_id INTEGER REFERENCES col_accounts(id) ON DELETE CASCADE,
        monthly_amount NUMERIC(10,2),
        weekly_amount NUMERIC(10,2),
        frequency VARCHAR(20) DEFAULT 'monthly',
        start_date DATE,
        next_due DATE,
        stripe_sub_id VARCHAR(255),
        status VARCHAR(30) DEFAULT 'active',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS col_activity_log (
        id SERIAL PRIMARY KEY,
        user_name VARCHAR(100),
        action VARCHAR(100) NOT NULL,
        account_id INTEGER REFERENCES col_accounts(id) ON DELETE SET NULL,
        details TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_col_accounts_track ON col_accounts(track);
      CREATE INDEX IF NOT EXISTS idx_col_accounts_status ON col_accounts(status);
      CREATE INDEX IF NOT EXISTS idx_col_accounts_center ON col_accounts(center);
      CREATE INDEX IF NOT EXISTS idx_col_children_account ON col_children(account_id);
      CREATE INDEX IF NOT EXISTS idx_col_payments_account ON col_payments(account_id);
      CREATE INDEX IF NOT EXISTS idx_col_communications_account ON col_communications(account_id);
      CREATE INDEX IF NOT EXISTS idx_col_activity_log_account ON col_activity_log(account_id);
    `);
    console.log('Database tables initialized');
  } finally {
    client.release();
  }
}

// ============================================================
// NICKNAME / NAME MATCHING (same logic as Payroll Hub)
// ============================================================
const NICKNAMES = {
  'william': ['will', 'bill', 'billy', 'willy', 'liam'],
  'robert': ['rob', 'bob', 'bobby', 'robbie'],
  'richard': ['rick', 'dick', 'rich', 'ricky'],
  'james': ['jim', 'jimmy', 'jamie'],
  'john': ['johnny', 'jack', 'jon'],
  'michael': ['mike', 'mikey', 'mickey'],
  'david': ['dave', 'davey'],
  'thomas': ['tom', 'tommy'],
  'joseph': ['joe', 'joey'],
  'charles': ['charlie', 'chuck', 'chas'],
  'daniel': ['dan', 'danny'],
  'matthew': ['matt', 'matty'],
  'anthony': ['tony'],
  'christopher': ['chris'],
  'andrew': ['andy', 'drew'],
  'steven': ['steve', 'stephen'],
  'edward': ['ed', 'eddie', 'ted', 'teddy'],
  'timothy': ['tim', 'timmy'],
  'joshua': ['josh'],
  'kenneth': ['ken', 'kenny'],
  'benjamin': ['ben', 'benny'],
  'nicholas': ['nick', 'nicky'],
  'samuel': ['sam', 'sammy'],
  'alexander': ['alex'],
  'jonathan': ['jon', 'jonny'],
  'patrick': ['pat', 'patty'],
  'raymond': ['ray'],
  'gregory': ['greg'],
  'lawrence': ['larry'],
  'katherine': ['kate', 'kathy', 'katie', 'kat', 'katy', 'cathy'],
  'elizabeth': ['liz', 'lizzy', 'beth', 'betty', 'eliza', 'liza'],
  'jennifer': ['jen', 'jenny'],
  'margaret': ['maggie', 'meg', 'peggy', 'marge'],
  'patricia': ['pat', 'patty', 'trish', 'tricia'],
  'barbara': ['barb', 'barbie'],
  'jessica': ['jess', 'jessie'],
  'christina': ['chris', 'chrissy', 'tina', 'christine'],
  'stephanie': ['steph'],
  'rebecca': ['becky', 'becca'],
  'deborah': ['deb', 'debbie'],
  'dorothy': ['dot', 'dotty'],
  'amanda': ['mandy'],
  'melissa': ['missy'],
  'kimberly': ['kim', 'kimmy'],
  'victoria': ['vicky', 'tori'],
  'catherine': ['cathy', 'cat', 'kate', 'katie'],
  'gabrielle': ['gabby', 'gabi'],
  'kirsten': ['kirsty'],
  'sharon': ['shari'],
  'cynthia': ['cindy'],
  'alexandra': ['alex', 'lexi'],
  'samantha': ['sam'],
  'natalie': ['nat'],
  'valerie': ['val'],
  'abigail': ['abby'],
  'allison': ['ally', 'allie'],
  'jacqueline': ['jackie'],
  'madeleine': ['maddie', 'maddy'],
  'caroline': ['carrie'],
  'cassandra': ['cassie', 'cass'],
  'theodore': ['theo', 'ted', 'teddy'],
};

function normalizeForMatch(name) {
  if (!name) return '';
  return name.toLowerCase().replace(/[^a-z]/g, '').trim();
}

function namesMatch(name1, name2) {
  const n1 = normalizeForMatch(name1);
  const n2 = normalizeForMatch(name2);
  if (n1 === n2) return true;

  // Check nickname mappings
  for (const [formal, nicks] of Object.entries(NICKNAMES)) {
    const allForms = [formal, ...nicks];
    if (allForms.includes(n1) && allForms.includes(n2)) return true;
  }
  return false;
}

// ============================================================
// CSV IMPORT HELPERS
// ============================================================
function detectColumns(headers) {
  const lower = headers.map(h => h.toLowerCase().trim());
  const mapping = {};

  // Family/parent name
  const familyIdx = lower.findIndex(h => h.includes('family') || h.includes('household') || h.includes('account'));
  const lastIdx = lower.findIndex(h => h.includes('last name') || h === 'last' || h.includes('parent last'));
  const firstIdx = lower.findIndex(h => h.includes('first name') || h === 'first' || h.includes('parent first'));
  const nameIdx = lower.findIndex(h => h === 'name' || h === 'parent name' || h === 'parent');

  if (familyIdx >= 0) mapping.family_name = familyIdx;
  if (lastIdx >= 0) mapping.parent_last = lastIdx;
  if (firstIdx >= 0) mapping.parent_first = firstIdx;
  if (nameIdx >= 0 && !mapping.family_name) mapping.family_name = nameIdx;

  // Email
  const emailIdx = lower.findIndex(h => h.includes('email'));
  if (emailIdx >= 0) mapping.email = emailIdx;

  // Phone
  const phoneIdx = lower.findIndex(h => h.includes('phone') || h.includes('mobile') || h.includes('cell'));
  if (phoneIdx >= 0) mapping.phone = phoneIdx;

  // Balance
  const balIdx = lower.findIndex(h => h.includes('balance') || h.includes('amount') || h.includes('owed') || h.includes('due'));
  if (balIdx >= 0) mapping.balance = balIdx;

  // Child name
  const childIdx = lower.findIndex(h => h.includes('child') || h.includes('student') || h.includes('enrolled'));
  if (childIdx >= 0) mapping.child_name = childIdx;

  // Center/location
  const centerIdx = lower.findIndex(h => h.includes('center') || h.includes('location') || h.includes('site') || h.includes('facility'));
  if (centerIdx >= 0) mapping.center = centerIdx;

  return mapping;
}

function parseBalance(val) {
  if (!val) return 0;
  const cleaned = String(val).replace(/[$,\s]/g, '').replace(/\((.+)\)/, '-$1');
  const num = parseFloat(cleaned);
  return isNaN(num) ? 0 : Math.round(num * 100) / 100;
}

function cleanPhone(val) {
  if (!val) return null;
  return String(val).replace(/[^\d+]/g, '');
}

// ============================================================
// API ROUTES
// ============================================================

// --- Dashboard Stats ---
app.get('/api/stats', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        COUNT(*) as total_accounts,
        COUNT(*) FILTER (WHERE track = 'current') as current_accounts,
        COUNT(*) FILTER (WHERE track = 'former') as former_accounts,
        COUNT(*) FILTER (WHERE status = 'past_due') as past_due_accounts,
        COUNT(*) FILTER (WHERE status = 'hold') as hold_accounts,
        COUNT(*) FILTER (WHERE status = 'arrangement') as arrangement_accounts,
        COUNT(*) FILTER (WHERE status = 'paid') as paid_accounts,
        COALESCE(SUM(balance), 0) as total_outstanding,
        COALESCE(SUM(balance) FILTER (WHERE track = 'current'), 0) as current_outstanding,
        COALESCE(SUM(balance) FILTER (WHERE track = 'former'), 0) as former_outstanding,
        COALESCE(SUM(balance) FILTER (WHERE center ILIKE '%peace%'), 0) as peace_outstanding,
        COALESCE(SUM(balance) FILTER (WHERE center ILIKE '%niles%'), 0) as niles_outstanding,
        COALESCE(SUM(balance) FILTER (WHERE center ILIKE '%montessori%' OR center ILIKE '%mcc%'), 0) as montessori_outstanding
      FROM col_accounts
      WHERE status != 'closed'
    `);

    const payments = await pool.query(`
      SELECT
        COALESCE(SUM(amount) FILTER (WHERE payment_date >= CURRENT_DATE - INTERVAL '7 days'), 0) as collected_week,
        COALESCE(SUM(amount) FILTER (WHERE payment_date >= DATE_TRUNC('month', CURRENT_DATE)), 0) as collected_month,
        COALESCE(SUM(amount), 0) as collected_total
      FROM col_payments
    `);

    res.json({ ...result.rows[0], ...payments.rows[0] });
  } catch (err) {
    console.error('Stats error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- List Accounts ---
app.get('/api/accounts', async (req, res) => {
  try {
    const { track, status, center, search, sort, order, limit, offset } = req.query;
    let where = ['1=1'];
    let params = [];
    let paramIdx = 1;

    if (track) { where.push(`track = $${paramIdx++}`); params.push(track); }
    if (status) { where.push(`status = $${paramIdx++}`); params.push(status); }
    if (center) { where.push(`center ILIKE $${paramIdx++}`); params.push(`%${center}%`); }
    if (search) {
      where.push(`(family_name ILIKE $${paramIdx} OR parent_first ILIKE $${paramIdx} OR parent_last ILIKE $${paramIdx} OR email ILIKE $${paramIdx})`);
      params.push(`%${search}%`);
      paramIdx++;
    }

    const sortCol = ['family_name', 'balance', 'status', 'track', 'center', 'days_past_due', 'updated_at', 'created_at'].includes(sort) ? sort : 'balance';
    const sortOrder = order === 'asc' ? 'ASC' : 'DESC';
    const lim = Math.min(parseInt(limit) || 50, 200);
    const off = parseInt(offset) || 0;

    const countResult = await pool.query(`SELECT COUNT(*) FROM col_accounts WHERE ${where.join(' AND ')}`, params);

    const result = await pool.query(
      `SELECT a.*,
        (SELECT json_agg(json_build_object('id', c.id, 'child_name', c.child_name, 'classroom', c.classroom, 'attendance_hold', c.attendance_hold))
         FROM col_children c WHERE c.account_id = a.id) as children
       FROM col_accounts a
       WHERE ${where.join(' AND ')}
       ORDER BY ${sortCol} ${sortOrder}
       LIMIT ${lim} OFFSET ${off}`,
      params
    );

    res.json({
      accounts: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit: lim,
      offset: off
    });
  } catch (err) {
    console.error('Accounts error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Single Account Detail ---
app.get('/api/accounts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const account = await pool.query('SELECT * FROM col_accounts WHERE id = $1', [id]);
    if (account.rows.length === 0) return res.status(404).json({ error: 'Account not found' });

    const children = await pool.query('SELECT * FROM col_children WHERE account_id = $1 ORDER BY child_name', [id]);
    const payments = await pool.query('SELECT * FROM col_payments WHERE account_id = $1 ORDER BY payment_date DESC', [id]);
    const communications = await pool.query('SELECT * FROM col_communications WHERE account_id = $1 ORDER BY sent_at DESC', [id]);
    const plans = await pool.query('SELECT * FROM col_payment_plans WHERE account_id = $1 ORDER BY created_at DESC', [id]);
    const activity = await pool.query('SELECT * FROM col_activity_log WHERE account_id = $1 ORDER BY created_at DESC LIMIT 50', [id]);

    res.json({
      ...account.rows[0],
      children: children.rows,
      payments: payments.rows,
      communications: communications.rows,
      payment_plans: plans.rows,
      activity: activity.rows
    });
  } catch (err) {
    console.error('Account detail error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Update Account ---
app.put('/api/accounts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { family_name, parent_first, parent_last, email, phone, center, track, status, balance, notes } = req.body;

    const result = await pool.query(
      `UPDATE col_accounts SET
        family_name = COALESCE($1, family_name),
        parent_first = COALESCE($2, parent_first),
        parent_last = COALESCE($3, parent_last),
        email = COALESCE($4, email),
        phone = COALESCE($5, phone),
        center = COALESCE($6, center),
        track = COALESCE($7, track),
        status = COALESCE($8, status),
        balance = COALESCE($9, balance),
        notes = COALESCE($10, notes),
        updated_at = NOW()
       WHERE id = $11 RETURNING *`,
      [family_name, parent_first, parent_last, email, phone, center, track, status, balance, notes, id]
    );

    if (result.rows.length === 0) return res.status(404).json({ error: 'Account not found' });

    await pool.query(
      'INSERT INTO col_activity_log (user_name, action, account_id, details) VALUES ($1, $2, $3, $4)',
      [req.body.user || 'Mary', 'account_updated', id, JSON.stringify(req.body)]
    );

    res.json(result.rows[0]);
  } catch (err) {
    console.error('Update error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Add Manual Payment ---
app.post('/api/accounts/:id/payments', async (req, res) => {
  try {
    const { id } = req.params;
    const { amount, method, notes } = req.body;

    const payment = await pool.query(
      'INSERT INTO col_payments (account_id, amount, method, notes) VALUES ($1, $2, $3, $4) RETURNING *',
      [id, amount, method || 'manual', notes]
    );

    // Update balance
    await pool.query(
      `UPDATE col_accounts SET
        balance = GREATEST(0, balance - $1),
        last_payment_date = CURRENT_DATE,
        status = CASE WHEN balance - $1 <= 0 THEN 'paid' ELSE status END,
        updated_at = NOW()
       WHERE id = $2`,
      [amount, id]
    );

    await pool.query(
      'INSERT INTO col_activity_log (user_name, action, account_id, details) VALUES ($1, $2, $3, $4)',
      [req.body.user || 'Mary', 'payment_recorded', id, `$${amount} via ${method || 'manual'}`]
    );

    res.json(payment.rows[0]);
  } catch (err) {
    console.error('Payment error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Add Note ---
app.post('/api/accounts/:id/notes', async (req, res) => {
  try {
    const { id } = req.params;
    const { note, user } = req.body;

    const current = await pool.query('SELECT notes FROM col_accounts WHERE id = $1', [id]);
    const existingNotes = current.rows[0]?.notes || '';
    const timestamp = new Date().toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    const newNotes = `[${timestamp} - ${user || 'Mary'}] ${note}\n${existingNotes}`;

    await pool.query('UPDATE col_accounts SET notes = $1, updated_at = NOW() WHERE id = $2', [newNotes, id]);

    await pool.query(
      'INSERT INTO col_activity_log (user_name, action, account_id, details) VALUES ($1, $2, $3, $4)',
      [user || 'Mary', 'note_added', id, note]
    );

    res.json({ success: true });
  } catch (err) {
    console.error('Note error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- CSV Import ---
app.post('/api/import', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const content = req.file.buffer.toString('utf-8');
    const records = parse(content, { columns: false, skip_empty_lines: true, relax_column_count: true });

    if (records.length < 2) return res.status(400).json({ error: 'File has no data rows' });

    const headers = records[0];
    const mapping = detectColumns(headers);
    const source = req.body.source || 'csv_import';
    const defaultCenter = req.body.center || '';
    const defaultTrack = req.body.track || 'current';

    // Preview mode - return mapped data without importing
    if (req.body.preview === 'true') {
      const preview = records.slice(1, 11).map(row => ({
        family_name: mapping.family_name !== undefined ? row[mapping.family_name] : '',
        parent_first: mapping.parent_first !== undefined ? row[mapping.parent_first] : '',
        parent_last: mapping.parent_last !== undefined ? row[mapping.parent_last] : '',
        email: mapping.email !== undefined ? row[mapping.email] : '',
        phone: mapping.phone !== undefined ? row[mapping.phone] : '',
        balance: mapping.balance !== undefined ? parseBalance(row[mapping.balance]) : 0,
        child_name: mapping.child_name !== undefined ? row[mapping.child_name] : '',
        center: mapping.center !== undefined ? row[mapping.center] : defaultCenter,
      }));
      return res.json({
        headers,
        mapping,
        preview,
        total_rows: records.length - 1,
        source
      });
    }

    // Actual import
    const client = await pool.connect();
    let imported = 0, skipped = 0, updated = 0;
    const errors = [];

    try {
      await client.query('BEGIN');

      for (let i = 1; i < records.length; i++) {
        const row = records[i];
        try {
          let familyName = mapping.family_name !== undefined ? row[mapping.family_name]?.trim() : '';
          const parentFirst = mapping.parent_first !== undefined ? row[mapping.parent_first]?.trim() : '';
          const parentLast = mapping.parent_last !== undefined ? row[mapping.parent_last]?.trim() : '';

          if (!familyName && parentLast) familyName = parentFirst ? `${parentLast}, ${parentFirst}` : parentLast;
          if (!familyName) { skipped++; continue; }

          const email = mapping.email !== undefined ? row[mapping.email]?.trim() : null;
          const phone = mapping.phone !== undefined ? cleanPhone(row[mapping.phone]) : null;
          const balance = mapping.balance !== undefined ? parseBalance(row[mapping.balance]) : 0;
          const childName = mapping.child_name !== undefined ? row[mapping.child_name]?.trim() : null;
          const center = mapping.center !== undefined ? row[mapping.center]?.trim() : defaultCenter;

          if (balance <= 0) { skipped++; continue; }

          // Check for existing account (name match)
          const existing = await client.query(
            `SELECT id, family_name FROM col_accounts WHERE
              LOWER(REPLACE(family_name, ' ', '')) = LOWER(REPLACE($1, ' ', ''))
              OR (parent_last IS NOT NULL AND LOWER(parent_last) = LOWER($2))`,
            [familyName, parentLast || familyName.split(',')[0]?.trim()]
          );

          let accountId;
          if (existing.rows.length > 0) {
            // Update existing
            accountId = existing.rows[0].id;
            await client.query(
              `UPDATE col_accounts SET
                balance = $1,
                email = COALESCE(NULLIF($2, ''), email),
                phone = COALESCE(NULLIF($3, ''), phone),
                center = COALESCE(NULLIF($4, ''), center),
                source = $5,
                updated_at = NOW()
               WHERE id = $6`,
              [balance, email, phone, center, source, accountId]
            );
            updated++;
          } else {
            // Insert new
            const ins = await client.query(
              `INSERT INTO col_accounts (family_name, parent_first, parent_last, email, phone, center, track, status, balance, original_balance, source)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10) RETURNING id`,
              [familyName, parentFirst, parentLast, email, phone, center, defaultTrack, balance > 0 ? 'past_due' : 'active', balance, source]
            );
            accountId = ins.rows[0].id;
            imported++;
          }

          // Add child if present
          if (childName && accountId) {
            const existingChild = await client.query(
              'SELECT id FROM col_children WHERE account_id = $1 AND LOWER(child_name) = LOWER($2)',
              [accountId, childName]
            );
            if (existingChild.rows.length === 0) {
              await client.query(
                'INSERT INTO col_children (account_id, child_name) VALUES ($1, $2)',
                [accountId, childName]
              );
            }
          }
        } catch (rowErr) {
          errors.push(`Row ${i + 1}: ${rowErr.message}`);
        }
      }

      await client.query('COMMIT');

      await pool.query(
        'INSERT INTO col_activity_log (user_name, action, details) VALUES ($1, $2, $3)',
        ['System', 'csv_import', `Imported ${imported} new, updated ${updated}, skipped ${skipped} from ${source}. ${errors.length} errors.`]
      );

      res.json({ imported, updated, skipped, errors: errors.slice(0, 20), total: records.length - 1 });
    } catch (txErr) {
      await client.query('ROLLBACK');
      throw txErr;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Import error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Column Mapping Override ---
app.post('/api/import/mapped', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const content = req.file.buffer.toString('utf-8');
    const records = parse(content, { columns: false, skip_empty_lines: true, relax_column_count: true });

    const customMapping = JSON.parse(req.body.mapping || '{}');
    const source = req.body.source || 'csv_import';
    const defaultCenter = req.body.center || '';
    const defaultTrack = req.body.track || 'current';

    const client = await pool.connect();
    let imported = 0, skipped = 0, updated = 0;
    const errors = [];

    try {
      await client.query('BEGIN');

      for (let i = 1; i < records.length; i++) {
        const row = records[i];
        try {
          let familyName = customMapping.family_name !== undefined ? row[customMapping.family_name]?.trim() : '';
          const parentFirst = customMapping.parent_first !== undefined ? row[customMapping.parent_first]?.trim() : '';
          const parentLast = customMapping.parent_last !== undefined ? row[customMapping.parent_last]?.trim() : '';

          if (!familyName && parentLast) familyName = parentFirst ? `${parentLast}, ${parentFirst}` : parentLast;
          if (!familyName) { skipped++; continue; }

          const email = customMapping.email !== undefined ? row[customMapping.email]?.trim() : null;
          const phone = customMapping.phone !== undefined ? cleanPhone(row[customMapping.phone]) : null;
          const balance = customMapping.balance !== undefined ? parseBalance(row[customMapping.balance]) : 0;
          const childName = customMapping.child_name !== undefined ? row[customMapping.child_name]?.trim() : null;
          const center = customMapping.center !== undefined ? row[customMapping.center]?.trim() : defaultCenter;

          if (balance <= 0) { skipped++; continue; }

          const existing = await client.query(
            `SELECT id FROM col_accounts WHERE
              LOWER(REPLACE(family_name, ' ', '')) = LOWER(REPLACE($1, ' ', ''))`,
            [familyName]
          );

          let accountId;
          if (existing.rows.length > 0) {
            accountId = existing.rows[0].id;
            await client.query(
              `UPDATE col_accounts SET balance = $1, email = COALESCE(NULLIF($2,''), email), phone = COALESCE(NULLIF($3,''), phone), center = COALESCE(NULLIF($4,''), center), source = $5, updated_at = NOW() WHERE id = $6`,
              [balance, email, phone, center, source, accountId]
            );
            updated++;
          } else {
            const ins = await client.query(
              `INSERT INTO col_accounts (family_name, parent_first, parent_last, email, phone, center, track, status, balance, original_balance, source)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$10) RETURNING id`,
              [familyName, parentFirst, parentLast, email, phone, center, defaultTrack, 'past_due', balance, source]
            );
            accountId = ins.rows[0].id;
            imported++;
          }

          if (childName && accountId) {
            const ec = await client.query('SELECT id FROM col_children WHERE account_id=$1 AND LOWER(child_name)=LOWER($2)', [accountId, childName]);
            if (ec.rows.length === 0) await client.query('INSERT INTO col_children (account_id, child_name) VALUES ($1,$2)', [accountId, childName]);
          }
        } catch (rowErr) {
          errors.push(`Row ${i+1}: ${rowErr.message}`);
        }
      }

      await client.query('COMMIT');
      res.json({ imported, updated, skipped, errors: errors.slice(0, 20), total: records.length - 1 });
    } catch (txErr) {
      await client.query('ROLLBACK');
      throw txErr;
    } finally {
      client.release();
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Add Account Manually ---
app.post('/api/accounts', async (req, res) => {
  try {
    const { family_name, parent_first, parent_last, email, phone, center, track, balance, notes } = req.body;

    const result = await pool.query(
      `INSERT INTO col_accounts (family_name, parent_first, parent_last, email, phone, center, track, status, balance, original_balance, notes, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9,$10,'manual') RETURNING *`,
      [family_name, parent_first, parent_last, email, phone, center, track || 'current', balance > 0 ? 'past_due' : 'active', balance || 0, notes]
    );

    await pool.query(
      'INSERT INTO col_activity_log (user_name, action, account_id, details) VALUES ($1,$2,$3,$4)',
      [req.body.user || 'Mary', 'account_created', result.rows[0].id, `Manual entry: ${family_name}`]
    );

    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Add Child to Account ---
app.post('/api/accounts/:id/children', async (req, res) => {
  try {
    const { child_name, classroom } = req.body;
    const result = await pool.query(
      'INSERT INTO col_children (account_id, child_name, classroom) VALUES ($1,$2,$3) RETURNING *',
      [req.params.id, child_name, classroom]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Activity Log ---
app.get('/api/activity', async (req, res) => {
  try {
    const { limit } = req.query;
    const result = await pool.query(
      `SELECT l.*, a.family_name FROM col_activity_log l
       LEFT JOIN col_accounts a ON l.account_id = a.id
       ORDER BY l.created_at DESC LIMIT $1`,
      [Math.min(parseInt(limit) || 50, 200)]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Delete Account ---
app.delete('/api/accounts/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM col_accounts WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// FRONTEND
// ============================================================
app.get('/', (req, res) => {
  res.send(FRONTEND_HTML);
});

const FRONTEND_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TCC Collections Hub</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f0f2f5; color: #333; }

/* Header */
.header { background: linear-gradient(135deg, #1B4F72, #2E86C1); color: white; padding: 16px 24px; display: flex; align-items: center; justify-content: space-between; box-shadow: 0 2px 8px rgba(0,0,0,0.15); }
.header h1 { font-size: 22px; font-weight: 700; }
.header h1 span { color: #F39C12; }
.header-actions { display: flex; gap: 10px; }
.header-btn { background: rgba(255,255,255,0.15); border: 1px solid rgba(255,255,255,0.3); color: white; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; transition: all 0.2s; }
.header-btn:hover { background: rgba(255,255,255,0.25); }
.header-btn.primary { background: #F39C12; border-color: #F39C12; font-weight: 600; }
.header-btn.primary:hover { background: #E67E22; }

/* Stats Cards */
.stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; padding: 20px 24px; }
.stat-card { background: white; border-radius: 10px; padding: 18px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
.stat-card .label { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
.stat-card .value { font-size: 28px; font-weight: 700; color: #1B4F72; }
.stat-card .value.money { color: #E74C3C; }
.stat-card .value.green { color: #27AE60; }
.stat-card .sub { font-size: 12px; color: #999; margin-top: 4px; }

/* Main content */
.main { padding: 0 24px 24px; }

/* Tabs */
.tabs { display: flex; gap: 4px; margin-bottom: 16px; background: white; border-radius: 10px; padding: 4px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
.tab { padding: 10px 20px; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 500; color: #666; transition: all 0.2s; border: none; background: none; }
.tab.active { background: #1B4F72; color: white; }
.tab:hover:not(.active) { background: #f0f2f5; }
.tab .count { background: rgba(0,0,0,0.1); padding: 2px 8px; border-radius: 10px; font-size: 11px; margin-left: 6px; }
.tab.active .count { background: rgba(255,255,255,0.2); }

/* Filters */
.filters { display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }
.filter-select, .search-input { padding: 8px 12px; border: 1px solid #ddd; border-radius: 6px; font-size: 13px; background: white; }
.search-input { flex: 1; min-width: 200px; }
.search-input:focus, .filter-select:focus { outline: none; border-color: #2E86C1; box-shadow: 0 0 0 2px rgba(46,134,193,0.15); }

/* Account Table */
.table-wrap { background: white; border-radius: 10px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); overflow: hidden; }
table { width: 100%; border-collapse: collapse; }
th { background: #f8f9fa; padding: 12px 16px; text-align: left; font-size: 12px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 2px solid #eee; cursor: pointer; white-space: nowrap; }
th:hover { background: #eee; }
td { padding: 12px 16px; border-bottom: 1px solid #f0f0f0; font-size: 14px; }
tr:hover { background: #f8fafc; cursor: pointer; }
tr.hold-row { background: #FFF3E0; }
tr.paid-row { background: #E8F5E9; }

/* Status badges */
.badge { display: inline-block; padding: 3px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; text-transform: uppercase; }
.badge-active { background: #E3F2FD; color: #1565C0; }
.badge-past_due { background: #FFF3E0; color: #E65100; }
.badge-hold { background: #FCE4EC; color: #C62828; }
.badge-arrangement { background: #F3E5F5; color: #6A1B9A; }
.badge-paid { background: #E8F5E9; color: #2E7D32; }
.badge-closed { background: #ECEFF1; color: #546E7A; }
.badge-current { background: #E3F2FD; color: #1565C0; }
.badge-former { background: #FFF8E1; color: #F57F17; }

/* Modal */
.modal-overlay { display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.5); z-index: 1000; justify-content: center; align-items: flex-start; padding: 40px 20px; overflow-y: auto; }
.modal-overlay.active { display: flex; }
.modal { background: white; border-radius: 12px; width: 100%; max-width: 700px; box-shadow: 0 20px 60px rgba(0,0,0,0.3); }
.modal-header { display: flex; justify-content: space-between; align-items: center; padding: 20px 24px; border-bottom: 1px solid #eee; }
.modal-header h2 { font-size: 18px; color: #1B4F72; }
.modal-close { background: none; border: none; font-size: 24px; cursor: pointer; color: #999; padding: 0 4px; }
.modal-body { padding: 24px; max-height: 70vh; overflow-y: auto; }
.modal-footer { padding: 16px 24px; border-top: 1px solid #eee; display: flex; justify-content: flex-end; gap: 10px; }

/* Forms */
.form-group { margin-bottom: 16px; }
.form-group label { display: block; font-size: 13px; font-weight: 600; color: #555; margin-bottom: 6px; }
.form-group input, .form-group select, .form-group textarea { width: 100%; padding: 10px 12px; border: 1px solid #ddd; border-radius: 6px; font-size: 14px; }
.form-group textarea { height: 80px; resize: vertical; }
.form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }

/* Buttons */
.btn { padding: 10px 20px; border-radius: 6px; font-size: 14px; font-weight: 600; cursor: pointer; border: none; transition: all 0.2s; }
.btn-primary { background: #1B4F72; color: white; }
.btn-primary:hover { background: #154360; }
.btn-gold { background: #F39C12; color: white; }
.btn-gold:hover { background: #E67E22; }
.btn-outline { background: white; color: #1B4F72; border: 1px solid #1B4F72; }
.btn-outline:hover { background: #f0f7fc; }
.btn-danger { background: #E74C3C; color: white; }
.btn-danger:hover { background: #C0392B; }
.btn-sm { padding: 6px 12px; font-size: 12px; }
.btn-green { background: #27AE60; color: white; }
.btn-green:hover { background: #229954; }

/* Detail view */
.detail-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 20px; }
.detail-name { font-size: 24px; font-weight: 700; color: #1B4F72; }
.detail-balance { font-size: 32px; font-weight: 700; color: #E74C3C; }
.detail-section { margin-bottom: 24px; }
.detail-section h3 { font-size: 15px; font-weight: 700; color: #1B4F72; margin-bottom: 10px; border-bottom: 2px solid #F39C12; padding-bottom: 6px; }
.detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
.detail-item { display: flex; flex-direction: column; }
.detail-item .label { font-size: 11px; color: #888; text-transform: uppercase; }
.detail-item .value { font-size: 14px; color: #333; font-weight: 500; }

/* Children list */
.child-tag { display: inline-block; background: #E3F2FD; color: #1565C0; padding: 4px 12px; border-radius: 14px; font-size: 12px; margin: 2px 4px 2px 0; }
.child-tag.held { background: #FCE4EC; color: #C62828; }

/* Activity items */
.activity-item { padding: 8px 0; border-bottom: 1px solid #f0f0f0; font-size: 13px; }
.activity-item .time { color: #999; font-size: 11px; }
.activity-item .action { font-weight: 500; }

/* Import section */
.import-zone { border: 2px dashed #ccc; border-radius: 10px; padding: 40px; text-align: center; background: #fafafa; margin-bottom: 20px; }
.import-zone.dragover { border-color: #2E86C1; background: #f0f7fc; }
.import-zone input[type="file"] { display: none; }
.import-zone label { cursor: pointer; color: #2E86C1; font-weight: 600; }

/* Pagination */
.pagination { display: flex; justify-content: space-between; align-items: center; padding: 16px; border-top: 1px solid #eee; }
.pagination .info { font-size: 13px; color: #666; }
.pagination .btns { display: flex; gap: 6px; }

/* Preview table */
.preview-table { width: 100%; border-collapse: collapse; font-size: 12px; margin: 12px 0; }
.preview-table th { background: #1B4F72; color: white; padding: 6px 8px; }
.preview-table td { padding: 6px 8px; border: 1px solid #eee; }

/* Toast */
.toast { position: fixed; bottom: 20px; right: 20px; background: #1B4F72; color: white; padding: 14px 24px; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.2); z-index: 2000; transform: translateY(100px); opacity: 0; transition: all 0.3s; font-size: 14px; }
.toast.show { transform: translateY(0); opacity: 1; }
.toast.error { background: #E74C3C; }
.toast.success { background: #27AE60; }

/* Responsive */
@media (max-width: 768px) {
  .stats-grid { grid-template-columns: repeat(2, 1fr); }
  .form-row { grid-template-columns: 1fr; }
  .header { flex-direction: column; gap: 10px; }
  .tabs { overflow-x: auto; }
  .filters { flex-direction: column; }
  .detail-grid { grid-template-columns: 1fr; }
}
</style>
</head>
<body>

<div class="header">
  <h1>TCC <span>Collections Hub</span></h1>
  <div class="header-actions">
    <button class="header-btn" onclick="showImportModal()">📤 Import CSV</button>
    <button class="header-btn" onclick="showAddModal()">➕ Add Account</button>
    <button class="header-btn" onclick="loadActivity()">📋 Activity Log</button>
  </div>
</div>

<div class="stats-grid" id="statsGrid"></div>

<div class="main">
  <div class="tabs" id="tabBar">
    <button class="tab active" data-track="" onclick="setTrack(this, '')">All Accounts</button>
    <button class="tab" data-track="current" onclick="setTrack(this, 'current')">Current Families</button>
    <button class="tab" data-track="former" onclick="setTrack(this, 'former')">Former Families</button>
  </div>

  <div class="filters">
    <input type="text" class="search-input" id="searchInput" placeholder="Search by name, email..." oninput="debounceSearch()">
    <select class="filter-select" id="statusFilter" onchange="loadAccounts()">
      <option value="">All Statuses</option>
      <option value="active">Active</option>
      <option value="past_due">Past Due</option>
      <option value="hold">On Hold</option>
      <option value="arrangement">Arrangement</option>
      <option value="paid">Paid</option>
      <option value="closed">Closed</option>
    </select>
    <select class="filter-select" id="centerFilter" onchange="loadAccounts()">
      <option value="">All Centers</option>
      <option value="peace">Peace Boulevard</option>
      <option value="niles">Niles</option>
      <option value="montessori">Montessori</option>
    </select>
    <select class="filter-select" id="sortSelect" onchange="loadAccounts()">
      <option value="balance">Highest Balance</option>
      <option value="days_past_due">Days Past Due</option>
      <option value="family_name">Name (A-Z)</option>
      <option value="updated_at">Recently Updated</option>
    </select>
  </div>

  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Family</th>
          <th>Center</th>
          <th>Track</th>
          <th>Status</th>
          <th>Balance</th>
          <th>Email</th>
          <th>Phone</th>
        </tr>
      </thead>
      <tbody id="accountsBody"></tbody>
    </table>
    <div class="pagination">
      <div class="info" id="pageInfo">Showing 0 accounts</div>
      <div class="btns">
        <button class="btn btn-sm btn-outline" id="prevBtn" onclick="prevPage()" disabled>← Previous</button>
        <button class="btn btn-sm btn-outline" id="nextBtn" onclick="nextPage()" disabled>Next →</button>
      </div>
    </div>
  </div>
</div>

<!-- Account Detail Modal -->
<div class="modal-overlay" id="detailModal">
  <div class="modal" style="max-width: 800px;">
    <div class="modal-header">
      <h2 id="detailTitle">Account Detail</h2>
      <button class="modal-close" onclick="closeModal('detailModal')">&times;</button>
    </div>
    <div class="modal-body" id="detailBody"></div>
  </div>
</div>

<!-- Add Account Modal -->
<div class="modal-overlay" id="addModal">
  <div class="modal">
    <div class="modal-header">
      <h2>Add Account</h2>
      <button class="modal-close" onclick="closeModal('addModal')">&times;</button>
    </div>
    <div class="modal-body">
      <div class="form-row">
        <div class="form-group">
          <label>Family Name *</label>
          <input type="text" id="addFamilyName" placeholder="e.g. Smith, Johnson">
        </div>
        <div class="form-group">
          <label>Balance Owed</label>
          <input type="number" id="addBalance" step="0.01" placeholder="0.00">
        </div>
      </div>
      <div class="form-row">
        <div class="form-group">
          <label>Parent First Name</label>
          <input type="text" id="addFirstName">
        </div>
        <div class="form-group">
          <label>Parent Last Name</label>
          <input type="text" id="addLastName">
        </div>
      </div>
      <div class="form-row">
        <div class="form-group">
          <label>Email</label>
          <input type="email" id="addEmail">
        </div>
        <div class="form-group">
          <label>Phone</label>
          <input type="tel" id="addPhone">
        </div>
      </div>
      <div class="form-row">
        <div class="form-group">
          <label>Center</label>
          <select id="addCenter">
            <option value="">Select Center</option>
            <option value="Peace Boulevard">Peace Boulevard</option>
            <option value="Niles">Niles</option>
            <option value="Montessori">Montessori</option>
          </select>
        </div>
        <div class="form-group">
          <label>Track</label>
          <select id="addTrack">
            <option value="current">Current Family</option>
            <option value="former">Former Family</option>
          </select>
        </div>
      </div>
      <div class="form-group">
        <label>Notes</label>
        <textarea id="addNotes" placeholder="Optional notes about this account..."></textarea>
      </div>
    </div>
    <div class="modal-footer">
      <button class="btn btn-outline" onclick="closeModal('addModal')">Cancel</button>
      <button class="btn btn-primary" onclick="saveNewAccount()">Add Account</button>
    </div>
  </div>
</div>

<!-- Import Modal -->
<div class="modal-overlay" id="importModal">
  <div class="modal" style="max-width: 800px;">
    <div class="modal-header">
      <h2>Import CSV</h2>
      <button class="modal-close" onclick="closeModal('importModal')">&times;</button>
    </div>
    <div class="modal-body" id="importBody">
      <div class="form-row">
        <div class="form-group">
          <label>Source</label>
          <select id="importSource">
            <option value="playground">Playground</option>
            <option value="kangarootime_v1">Kangarootime V1</option>
            <option value="kangarootime_v2">Kangarootime V2</option>
            <option value="other">Other</option>
          </select>
        </div>
        <div class="form-group">
          <label>Default Center (if not in CSV)</label>
          <select id="importCenter">
            <option value="">None</option>
            <option value="Peace Boulevard">Peace Boulevard</option>
            <option value="Niles">Niles</option>
            <option value="Montessori">Montessori</option>
          </select>
        </div>
      </div>
      <div class="form-group">
        <label>Default Track</label>
        <select id="importTrack">
          <option value="current">Current Families</option>
          <option value="former">Former Families</option>
        </select>
      </div>
      <div class="import-zone" id="importZone">
        <p style="font-size: 16px; margin-bottom: 10px;">📄 Drop your CSV file here or <label for="importFile">browse</label></p>
        <p style="font-size: 12px; color: #999;">Supports Playground and Kangarootime exports</p>
        <input type="file" id="importFile" accept=".csv,.txt" onchange="handleImportFile(this.files[0])">
      </div>
      <div id="importPreview" style="display:none;"></div>
    </div>
  </div>
</div>

<!-- Activity Log Modal -->
<div class="modal-overlay" id="activityModal">
  <div class="modal" style="max-width: 700px;">
    <div class="modal-header">
      <h2>Activity Log</h2>
      <button class="modal-close" onclick="closeModal('activityModal')">&times;</button>
    </div>
    <div class="modal-body" id="activityBody"></div>
  </div>
</div>

<!-- Edit Account Modal -->
<div class="modal-overlay" id="editModal">
  <div class="modal">
    <div class="modal-header">
      <h2>Edit Account</h2>
      <button class="modal-close" onclick="closeModal('editModal')">&times;</button>
    </div>
    <div class="modal-body" id="editBody"></div>
    <div class="modal-footer">
      <button class="btn btn-outline" onclick="closeModal('editModal')">Cancel</button>
      <button class="btn btn-primary" onclick="saveEditAccount()">Save Changes</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
// State
let currentTrack = '';
let currentOffset = 0;
let currentTotal = 0;
const PAGE_SIZE = 50;
let searchTimer = null;
let editingAccountId = null;

// Init
document.addEventListener('DOMContentLoaded', () => {
  loadStats();
  loadAccounts();

  // Drag and drop
  const zone = document.getElementById('importZone');
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('dragover'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('dragover'));
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.classList.remove('dragover');
    if (e.dataTransfer.files.length) handleImportFile(e.dataTransfer.files[0]);
  });
});

function toast(msg, type = '') {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast show ' + type;
  setTimeout(() => t.className = 'toast', 3000);
}

function fmt(n) {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(n || 0);
}

function closeModal(id) { document.getElementById(id).classList.remove('active'); }
function openModal(id) { document.getElementById(id).classList.add('active'); }

// Stats
async function loadStats() {
  try {
    const res = await fetch('/api/stats');
    const s = res.ok ? await res.json() : {};
    document.getElementById('statsGrid').innerHTML = \`
      <div class="stat-card">
        <div class="label">Total Outstanding</div>
        <div class="value money">\${fmt(s.total_outstanding)}</div>
        <div class="sub">\${s.total_accounts || 0} accounts</div>
      </div>
      <div class="stat-card">
        <div class="label">Current Families</div>
        <div class="value money">\${fmt(s.current_outstanding)}</div>
        <div class="sub">\${s.current_accounts || 0} accounts</div>
      </div>
      <div class="stat-card">
        <div class="label">Former Families</div>
        <div class="value money">\${fmt(s.former_outstanding)}</div>
        <div class="sub">\${s.former_accounts || 0} accounts</div>
      </div>
      <div class="stat-card">
        <div class="label">Collected This Month</div>
        <div class="value green">\${fmt(s.collected_month)}</div>
        <div class="sub">This week: \${fmt(s.collected_week)}</div>
      </div>
      <div class="stat-card">
        <div class="label">Peace Blvd</div>
        <div class="value money">\${fmt(s.peace_outstanding)}</div>
      </div>
      <div class="stat-card">
        <div class="label">Niles</div>
        <div class="value money">\${fmt(s.niles_outstanding)}</div>
      </div>
      <div class="stat-card">
        <div class="label">Montessori</div>
        <div class="value money">\${fmt(s.montessori_outstanding)}</div>
      </div>
      <div class="stat-card">
        <div class="label">Accounts Status</div>
        <div class="value" style="font-size:14px; line-height: 1.8;">
          <span class="badge badge-past_due">\${s.past_due_accounts || 0} Past Due</span>
          <span class="badge badge-hold">\${s.hold_accounts || 0} On Hold</span>
          <span class="badge badge-arrangement">\${s.arrangement_accounts || 0} Arrangements</span>
        </div>
      </div>
    \`;
  } catch(e) { console.error(e); }
}

// Tabs
function setTrack(btn, track) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  currentTrack = track;
  currentOffset = 0;
  loadAccounts();
}

// Search
function debounceSearch() {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => { currentOffset = 0; loadAccounts(); }, 300);
}

// Load Accounts
async function loadAccounts() {
  const search = document.getElementById('searchInput').value;
  const status = document.getElementById('statusFilter').value;
  const center = document.getElementById('centerFilter').value;
  const sort = document.getElementById('sortSelect').value;

  const params = new URLSearchParams({
    limit: PAGE_SIZE,
    offset: currentOffset,
    sort,
    order: sort === 'family_name' ? 'asc' : 'desc'
  });
  if (currentTrack) params.set('track', currentTrack);
  if (search) params.set('search', search);
  if (status) params.set('status', status);
  if (center) params.set('center', center);

  try {
    const res = await fetch('/api/accounts?' + params);
    const data = await res.json();
    currentTotal = data.total;

    const tbody = document.getElementById('accountsBody');
    if (data.accounts.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding:40px; color:#999;">No accounts found. Import a CSV or add accounts manually.</td></tr>';
    } else {
      tbody.innerHTML = data.accounts.map(a => {
        const rowClass = a.status === 'hold' ? 'hold-row' : a.status === 'paid' ? 'paid-row' : '';
        const children = a.children ? JSON.parse(typeof a.children === 'string' ? a.children : JSON.stringify(a.children)) : [];
        const childNames = children ? children.filter(Boolean).map(c => c.child_name).join(', ') : '';
        return \`<tr class="\${rowClass}" onclick="showDetail(\${a.id})">
          <td>
            <div style="font-weight:600;">\${esc(a.family_name)}</div>
            \${childNames ? '<div style="font-size:12px; color:#888;">'+esc(childNames)+'</div>' : ''}
          </td>
          <td>\${esc(a.center || '—')}</td>
          <td><span class="badge badge-\${a.track}">\${a.track}</span></td>
          <td><span class="badge badge-\${a.status}">\${a.status.replace('_',' ')}</span></td>
          <td style="font-weight:600; color:\${a.balance > 0 ? '#E74C3C' : '#27AE60'};">\${fmt(a.balance)}</td>
          <td style="font-size:12px;">\${esc(a.email || '—')}</td>
          <td style="font-size:12px;">\${esc(a.phone || '—')}</td>
        </tr>\`;
      }).join('');
    }

    const start = currentOffset + 1;
    const end = Math.min(currentOffset + PAGE_SIZE, currentTotal);
    document.getElementById('pageInfo').textContent = currentTotal > 0
      ? \`Showing \${start}–\${end} of \${currentTotal} accounts\`
      : 'No accounts found';
    document.getElementById('prevBtn').disabled = currentOffset === 0;
    document.getElementById('nextBtn').disabled = currentOffset + PAGE_SIZE >= currentTotal;
  } catch(e) { console.error(e); }
}

function prevPage() { currentOffset = Math.max(0, currentOffset - PAGE_SIZE); loadAccounts(); }
function nextPage() { currentOffset += PAGE_SIZE; loadAccounts(); }

// Account Detail
async function showDetail(id) {
  try {
    const res = await fetch('/api/accounts/' + id);
    const a = await res.json();

    document.getElementById('detailTitle').textContent = a.family_name;
    document.getElementById('detailBody').innerHTML = \`
      <div class="detail-header">
        <div>
          <div class="detail-name">\${esc(a.family_name)}</div>
          <div style="margin-top:6px;">
            <span class="badge badge-\${a.track}">\${a.track}</span>
            <span class="badge badge-\${a.status}">\${a.status.replace('_',' ')}</span>
          </div>
        </div>
        <div style="text-align:right;">
          <div class="detail-balance">\${fmt(a.balance)}</div>
          <div style="font-size:12px; color:#999;">Original: \${fmt(a.original_balance)}</div>
        </div>
      </div>

      <div style="display:flex; gap:8px; margin-bottom:20px; flex-wrap:wrap;">
        <button class="btn btn-sm btn-gold" onclick="showEditModal(\${a.id})">✏️ Edit</button>
        <button class="btn btn-sm btn-green" onclick="showPaymentForm(\${a.id})">💰 Record Payment</button>
        <button class="btn btn-sm btn-outline" onclick="showNoteForm(\${a.id})">📝 Add Note</button>
        <button class="btn btn-sm btn-outline" onclick="quickStatus(\${a.id}, 'hold')">⏸️ Hold</button>
        <button class="btn btn-sm btn-outline" onclick="quickStatus(\${a.id}, 'arrangement')">🤝 Arrangement</button>
        <button class="btn btn-sm btn-outline" onclick="quickStatus(\${a.id}, 'active')">✅ Active</button>
      </div>

      <div class="detail-section">
        <h3>Contact Information</h3>
        <div class="detail-grid">
          <div class="detail-item"><span class="label">Email</span><span class="value">\${esc(a.email || 'Not provided')}</span></div>
          <div class="detail-item"><span class="label">Phone</span><span class="value">\${esc(a.phone || 'Not provided')}</span></div>
          <div class="detail-item"><span class="label">Center</span><span class="value">\${esc(a.center || 'Not set')}</span></div>
          <div class="detail-item"><span class="label">Source</span><span class="value">\${esc(a.source || '—')}</span></div>
        </div>
      </div>

      <div class="detail-section">
        <h3>Children</h3>
        <div>
          \${a.children && a.children.length > 0
            ? a.children.map(c => \`<span class="child-tag \${c.attendance_hold ? 'held' : ''}">\${esc(c.child_name)}\${c.attendance_hold ? ' (HOLD)' : ''}</span>\`).join('')
            : '<span style="color:#999;">No children linked</span>'
          }
          <button class="btn btn-sm btn-outline" style="margin-left:8px;" onclick="showAddChildForm(\${a.id})">+ Add Child</button>
        </div>
      </div>

      <div class="detail-section">
        <h3>Payment History</h3>
        \${a.payments && a.payments.length > 0
          ? '<table style="width:100%;font-size:13px;"><tr><th style="text-align:left;padding:6px;">Date</th><th style="text-align:left;padding:6px;">Amount</th><th style="text-align:left;padding:6px;">Method</th><th style="text-align:left;padding:6px;">Notes</th></tr>' +
            a.payments.map(p => \`<tr><td style="padding:6px;">\${new Date(p.payment_date).toLocaleDateString()}</td><td style="padding:6px;color:#27AE60;font-weight:600;">\${fmt(p.amount)}</td><td style="padding:6px;">\${esc(p.method||'')}</td><td style="padding:6px;">\${esc(p.notes||'')}</td></tr>\`).join('') +
            '</table>'
          : '<p style="color:#999;">No payments recorded yet</p>'
        }
      </div>

      <div id="paymentForm\${a.id}" style="display:none; background:#f8f9fa; padding:16px; border-radius:8px; margin-bottom:16px;">
        <h4 style="margin-bottom:10px;">Record Payment</h4>
        <div class="form-row">
          <div class="form-group"><label>Amount</label><input type="number" id="payAmt\${a.id}" step="0.01"></div>
          <div class="form-group"><label>Method</label><select id="payMethod\${a.id}"><option>Cash</option><option>Check</option><option>Card</option><option>Stripe</option><option>Other</option></select></div>
        </div>
        <div class="form-group"><label>Notes</label><input type="text" id="payNotes\${a.id}"></div>
        <button class="btn btn-green btn-sm" onclick="recordPayment(\${a.id})">Save Payment</button>
      </div>

      <div id="noteForm\${a.id}" style="display:none; background:#f8f9fa; padding:16px; border-radius:8px; margin-bottom:16px;">
        <h4 style="margin-bottom:10px;">Add Note</h4>
        <div class="form-group"><textarea id="noteText\${a.id}" placeholder="Type your note..."></textarea></div>
        <button class="btn btn-primary btn-sm" onclick="saveNote(\${a.id})">Save Note</button>
      </div>

      <div id="childForm\${a.id}" style="display:none; background:#f8f9fa; padding:16px; border-radius:8px; margin-bottom:16px;">
        <h4 style="margin-bottom:10px;">Add Child</h4>
        <div class="form-row">
          <div class="form-group"><label>Child Name</label><input type="text" id="childName\${a.id}"></div>
          <div class="form-group"><label>Classroom</label><input type="text" id="childClass\${a.id}"></div>
        </div>
        <button class="btn btn-primary btn-sm" onclick="saveChild(\${a.id})">Add Child</button>
      </div>

      \${a.notes ? \`<div class="detail-section"><h3>Notes</h3><pre style="white-space:pre-wrap; font-family:inherit; font-size:13px; color:#555;">\${esc(a.notes)}</pre></div>\` : ''}

      <div class="detail-section">
        <h3>Activity</h3>
        \${a.activity && a.activity.length > 0
          ? a.activity.map(act => \`<div class="activity-item"><span class="time">\${new Date(act.created_at).toLocaleString()}</span> — <span class="action">\${esc(act.user_name || '')}</span>: \${esc(act.action)} \${act.details ? '— '+esc(act.details) : ''}</div>\`).join('')
          : '<p style="color:#999;">No activity yet</p>'
        }
      </div>
    \`;
    openModal('detailModal');
  } catch(e) { console.error(e); toast('Error loading account', 'error'); }
}

function showPaymentForm(id) { document.getElementById('paymentForm'+id).style.display = 'block'; }
function showNoteForm(id) { document.getElementById('noteForm'+id).style.display = 'block'; }
function showAddChildForm(id) { document.getElementById('childForm'+id).style.display = 'block'; }

async function recordPayment(id) {
  const amount = parseFloat(document.getElementById('payAmt'+id).value);
  if (!amount || amount <= 0) return toast('Enter a valid amount', 'error');
  try {
    await fetch('/api/accounts/'+id+'/payments', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ amount, method: document.getElementById('payMethod'+id).value, notes: document.getElementById('payNotes'+id).value })
    });
    toast('Payment recorded!', 'success');
    showDetail(id);
    loadStats();
    loadAccounts();
  } catch(e) { toast('Error recording payment', 'error'); }
}

async function saveNote(id) {
  const note = document.getElementById('noteText'+id).value.trim();
  if (!note) return;
  try {
    await fetch('/api/accounts/'+id+'/notes', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ note })
    });
    toast('Note added', 'success');
    showDetail(id);
  } catch(e) { toast('Error saving note', 'error'); }
}

async function saveChild(id) {
  const name = document.getElementById('childName'+id).value.trim();
  if (!name) return toast('Enter child name', 'error');
  try {
    await fetch('/api/accounts/'+id+'/children', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ child_name: name, classroom: document.getElementById('childClass'+id).value })
    });
    toast('Child added', 'success');
    showDetail(id);
  } catch(e) { toast('Error adding child', 'error'); }
}

async function quickStatus(id, status) {
  try {
    await fetch('/api/accounts/'+id, {
      method: 'PUT', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ status })
    });
    toast('Status updated to ' + status, 'success');
    showDetail(id);
    loadAccounts();
    loadStats();
  } catch(e) { toast('Error updating status', 'error'); }
}

// Add Account
function showAddModal() { openModal('addModal'); }

async function saveNewAccount() {
  const familyName = document.getElementById('addFamilyName').value.trim();
  if (!familyName) return toast('Family name is required', 'error');

  try {
    await fetch('/api/accounts', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        family_name: familyName,
        parent_first: document.getElementById('addFirstName').value.trim(),
        parent_last: document.getElementById('addLastName').value.trim(),
        email: document.getElementById('addEmail').value.trim(),
        phone: document.getElementById('addPhone').value.trim(),
        center: document.getElementById('addCenter').value,
        track: document.getElementById('addTrack').value,
        balance: parseFloat(document.getElementById('addBalance').value) || 0,
        notes: document.getElementById('addNotes').value.trim()
      })
    });
    toast('Account created!', 'success');
    closeModal('addModal');
    loadAccounts();
    loadStats();
    // Clear form
    ['addFamilyName','addFirstName','addLastName','addEmail','addPhone','addBalance','addNotes'].forEach(id => document.getElementById(id).value = '');
  } catch(e) { toast('Error creating account', 'error'); }
}

// Edit Account
async function showEditModal(id) {
  editingAccountId = id;
  try {
    const res = await fetch('/api/accounts/' + id);
    const a = await res.json();
    document.getElementById('editBody').innerHTML = \`
      <div class="form-row">
        <div class="form-group"><label>Family Name</label><input type="text" id="editFamilyName" value="\${esc(a.family_name||'')}"></div>
        <div class="form-group"><label>Balance</label><input type="number" id="editBalance" step="0.01" value="\${a.balance||0}"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>First Name</label><input type="text" id="editFirst" value="\${esc(a.parent_first||'')}"></div>
        <div class="form-group"><label>Last Name</label><input type="text" id="editLast" value="\${esc(a.parent_last||'')}"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>Email</label><input type="email" id="editEmail" value="\${esc(a.email||'')}"></div>
        <div class="form-group"><label>Phone</label><input type="tel" id="editPhone" value="\${esc(a.phone||'')}"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>Center</label>
          <select id="editCenter">
            <option value="" \${!a.center?'selected':''}>Select</option>
            <option value="Peace Boulevard" \${a.center==='Peace Boulevard'?'selected':''}>Peace Boulevard</option>
            <option value="Niles" \${a.center==='Niles'?'selected':''}>Niles</option>
            <option value="Montessori" \${a.center==='Montessori'?'selected':''}>Montessori</option>
          </select>
        </div>
        <div class="form-group"><label>Track</label>
          <select id="editTrack">
            <option value="current" \${a.track==='current'?'selected':''}>Current</option>
            <option value="former" \${a.track==='former'?'selected':''}>Former</option>
          </select>
        </div>
      </div>
      <div class="form-group"><label>Status</label>
        <select id="editStatus">
          <option value="active" \${a.status==='active'?'selected':''}>Active</option>
          <option value="past_due" \${a.status==='past_due'?'selected':''}>Past Due</option>
          <option value="hold" \${a.status==='hold'?'selected':''}>On Hold</option>
          <option value="arrangement" \${a.status==='arrangement'?'selected':''}>Arrangement</option>
          <option value="paid" \${a.status==='paid'?'selected':''}>Paid</option>
          <option value="closed" \${a.status==='closed'?'selected':''}>Closed</option>
        </select>
      </div>
      <div class="form-group"><label>Notes</label><textarea id="editNotes">\${esc(a.notes||'')}</textarea></div>
    \`;
    openModal('editModal');
  } catch(e) { toast('Error loading account', 'error'); }
}

async function saveEditAccount() {
  if (!editingAccountId) return;
  try {
    await fetch('/api/accounts/'+editingAccountId, {
      method: 'PUT', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        family_name: document.getElementById('editFamilyName').value.trim(),
        parent_first: document.getElementById('editFirst').value.trim(),
        parent_last: document.getElementById('editLast').value.trim(),
        email: document.getElementById('editEmail').value.trim(),
        phone: document.getElementById('editPhone').value.trim(),
        center: document.getElementById('editCenter').value,
        track: document.getElementById('editTrack').value,
        status: document.getElementById('editStatus').value,
        balance: parseFloat(document.getElementById('editBalance').value) || 0,
        notes: document.getElementById('editNotes').value
      })
    });
    toast('Account updated!', 'success');
    closeModal('editModal');
    showDetail(editingAccountId);
    loadAccounts();
    loadStats();
  } catch(e) { toast('Error saving changes', 'error'); }
}

// Import
function showImportModal() {
  document.getElementById('importPreview').style.display = 'none';
  document.getElementById('importPreview').innerHTML = '';
  openModal('importModal');
}

async function handleImportFile(file) {
  if (!file) return;
  const formData = new FormData();
  formData.append('file', file);
  formData.append('source', document.getElementById('importSource').value);
  formData.append('center', document.getElementById('importCenter').value);
  formData.append('track', document.getElementById('importTrack').value);
  formData.append('preview', 'true');

  try {
    const res = await fetch('/api/import', { method: 'POST', body: formData });
    const data = await res.json();

    if (data.error) return toast(data.error, 'error');

    const preview = document.getElementById('importPreview');
    preview.style.display = 'block';

    let mappingHtml = '<h4 style="margin-bottom:8px;">Column Mapping Detected</h4>';
    mappingHtml += '<div style="font-size:12px; color:#666; margin-bottom:12px;">Headers: ' + data.headers.map(h => '<code>'+esc(h)+'</code>').join(', ') + '</div>';

    mappingHtml += '<table class="preview-table"><thead><tr><th>Family Name</th><th>Email</th><th>Phone</th><th>Balance</th><th>Child</th><th>Center</th></tr></thead><tbody>';
    data.preview.forEach(r => {
      mappingHtml += \`<tr><td>\${esc(r.family_name||r.parent_last||'')}</td><td>\${esc(r.email||'')}</td><td>\${esc(r.phone||'')}</td><td>\${r.balance ? fmt(r.balance) : '—'}</td><td>\${esc(r.child_name||'')}</td><td>\${esc(r.center||'')}</td></tr>\`;
    });
    mappingHtml += '</tbody></table>';
    mappingHtml += \`<p style="font-size:13px; margin:12px 0;">Total rows to import: <strong>\${data.total_rows}</strong> (accounts with $0 balance will be skipped)</p>\`;
    mappingHtml += \`<button class="btn btn-primary" onclick="executeImport()">Import \${data.total_rows} Rows</button>\`;

    preview.innerHTML = mappingHtml;

    // Store the file for actual import
    window._importFile = file;
  } catch(e) { toast('Error previewing file', 'error'); }
}

async function executeImport() {
  if (!window._importFile) return toast('No file to import', 'error');

  const formData = new FormData();
  formData.append('file', window._importFile);
  formData.append('source', document.getElementById('importSource').value);
  formData.append('center', document.getElementById('importCenter').value);
  formData.append('track', document.getElementById('importTrack').value);

  try {
    const res = await fetch('/api/import', { method: 'POST', body: formData });
    const data = await res.json();

    if (data.error) return toast(data.error, 'error');

    const preview = document.getElementById('importPreview');
    preview.innerHTML = \`
      <div style="text-align:center; padding:20px;">
        <div style="font-size:48px; margin-bottom:10px;">✅</div>
        <h3 style="color:#27AE60;">Import Complete</h3>
        <p style="margin-top:10px;">
          <strong>\${data.imported}</strong> new accounts imported<br>
          <strong>\${data.updated}</strong> existing accounts updated<br>
          <strong>\${data.skipped}</strong> rows skipped (no name or $0 balance)
        </p>
        \${data.errors && data.errors.length > 0 ? '<p style="color:#E74C3C; font-size:12px; margin-top:10px;">' + data.errors.length + ' errors (see console)</p>' : ''}
        <button class="btn btn-primary" style="margin-top:16px;" onclick="closeModal('importModal'); loadAccounts(); loadStats();">Done</button>
      </div>
    \`;
    toast(\`Imported \${data.imported} accounts!\`, 'success');
  } catch(e) { toast('Import failed', 'error'); }
}

// Activity Log
async function loadActivity() {
  try {
    const res = await fetch('/api/activity?limit=100');
    const logs = await res.json();
    document.getElementById('activityBody').innerHTML = logs.length > 0
      ? logs.map(l => \`<div class="activity-item">
          <span class="time">\${new Date(l.created_at).toLocaleString()}</span> —
          <span class="action">\${esc(l.user_name||'System')}</span>: \${esc(l.action)}
          \${l.family_name ? ' — <em>'+esc(l.family_name)+'</em>' : ''}
          \${l.details ? '<br><span style="font-size:12px;color:#888;">'+esc(l.details)+'</span>' : ''}
        </div>\`).join('')
      : '<p style="color:#999;">No activity recorded yet.</p>';
    openModal('activityModal');
  } catch(e) { toast('Error loading activity', 'error'); }
}

// Utility
function esc(s) {
  if (!s) return '';
  const d = document.createElement('div');
  d.textContent = String(s);
  return d.innerHTML;
}
</script>
</body>
</html>`;

// ============================================================
// START SERVER
// ============================================================
initDB().then(() => {
  app.listen(PORT, () => {
    console.log(\`TCC Collections Hub running on port \${PORT}\`);
  });
}).catch(err => {
  console.error('Failed to initialize database:', err);
  process.exit(1);
});
