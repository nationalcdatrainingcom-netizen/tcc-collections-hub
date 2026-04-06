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
        parent2_first VARCHAR(255),
        parent2_last VARCHAR(255),
        email VARCHAR(255),
        email2 VARCHAR(255),
        phone VARCHAR(50),
        phone2 VARCHAR(50),
        center VARCHAR(100),
        track VARCHAR(20) DEFAULT 'current' CHECK (track IN ('current', 'former')),
        status VARCHAR(30) DEFAULT 'active' CHECK (status IN ('active', 'past_due', 'hold', 'arrangement', 'paid', 'closed')),
        balance NUMERIC(10,2) DEFAULT 0,
        original_balance NUMERIC(10,2) DEFAULT 0,
        aging_current NUMERIC(10,2) DEFAULT 0,
        aging_1_30 NUMERIC(10,2) DEFAULT 0,
        aging_31_60 NUMERIC(10,2) DEFAULT 0,
        aging_61_90 NUMERIC(10,2) DEFAULT 0,
        aging_90_plus NUMERIC(10,2) DEFAULT 0,
        is_cdc BOOLEAN DEFAULT false,
        cdc_expected NUMERIC(10,2) DEFAULT 0,
        cdc_received NUMERIC(10,2) DEFAULT 0,
        cdc_gap NUMERIC(10,2) DEFAULT 0,
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
        child_first VARCHAR(255),
        child_last VARCHAR(255),
        child_name VARCHAR(255) NOT NULL,
        classroom VARCHAR(100),
        enrollment_status VARCHAR(30) DEFAULT 'enrolled',
        attendance_hold BOOLEAN DEFAULT false,
        balance NUMERIC(10,2) DEFAULT 0,
        aging_current NUMERIC(10,2) DEFAULT 0,
        aging_1_30 NUMERIC(10,2) DEFAULT 0,
        aging_31_60 NUMERIC(10,2) DEFAULT 0,
        aging_61_90 NUMERIC(10,2) DEFAULT 0,
        aging_90_plus NUMERIC(10,2) DEFAULT 0,
        is_cdc BOOLEAN DEFAULT false,
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

      CREATE TABLE IF NOT EXISTS col_cdc_statements (
        id SERIAL PRIMARY KEY,
        voucher VARCHAR(50),
        voucher_date DATE,
        provider_id VARCHAR(50),
        center VARCHAR(100),
        pay_period_start DATE,
        pay_period_end DATE,
        total_pay NUMERIC(10,2) DEFAULT 0,
        net_total_pay NUMERIC(10,2) DEFAULT 0,
        total_children INTEGER DEFAULT 0,
        children_paid INTEGER DEFAULT 0,
        children_no_auth INTEGER DEFAULT 0,
        children_duplicate INTEGER DEFAULT 0,
        status VARCHAR(30) DEFAULT 'pending' CHECK (status IN ('pending', 'reviewed', 'approved', 'archived')),
        approved_by VARCHAR(100),
        approved_at TIMESTAMP,
        raw_json TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS col_cdc_children (
        id SERIAL PRIMARY KEY,
        statement_id INTEGER REFERENCES col_cdc_statements(id) ON DELETE CASCADE,
        account_id INTEGER REFERENCES col_accounts(id) ON DELETE SET NULL,
        child_name VARCHAR(255) NOT NULL,
        child_name_normalized VARCHAR(255),
        case_no VARCHAR(50),
        child_id_no VARCHAR(50),
        hours_auth INTEGER DEFAULT 0,
        hours_billed INTEGER DEFAULT 0,
        hours_paid INTEGER DEFAULT 0,
        fc NUMERIC(10,2) DEFAULT 0,
        max_rate NUMERIC(10,2) DEFAULT 0,
        amount_paid NUMERIC(10,2) DEFAULT 0,
        error_desc VARCHAR(255),
        is_paid BOOLEAN DEFAULT false,
        is_no_auth BOOLEAN DEFAULT false,
        is_duplicate BOOLEAN DEFAULT false,
        balance_shifted BOOLEAN DEFAULT false,
        shift_amount NUMERIC(10,2) DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_col_cdc_statements_center ON col_cdc_statements(center);
      CREATE INDEX IF NOT EXISTS idx_col_cdc_children_statement ON col_cdc_children(statement_id);
      CREATE INDEX IF NOT EXISTS idx_col_cdc_children_account ON col_cdc_children(account_id);
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
// PLAYGROUND TWO-FILE IMPORT (AR Aging + Guardians)
// ============================================================
const playgroundUpload = upload.fields([
  { name: 'arFile', maxCount: 1 },
  { name: 'guardianFile', maxCount: 1 }
]);

app.post('/api/import/playground', playgroundUpload, async (req, res) => {
  try {
    const arBuffer = req.files?.arFile?.[0]?.buffer;
    const guardianBuffer = req.files?.guardianFile?.[0]?.buffer;
    if (!arBuffer || !guardianBuffer) return res.status(400).json({ error: 'Both AR Aging and Guardians files are required' });

    const center = req.body.center || '';
    const preview = req.body.preview === 'true';

    // Parse AR Aging CSV: Last Name, First Name, Current, 1-30 days, 31-60 days, 61-90 days, 90+ days, Total, Total aging
    const arRecords = parse(arBuffer.toString('utf-8'), { columns: false, skip_empty_lines: true, relax_column_count: true });
    const arHeader = arRecords[0];
    const arData = arRecords.slice(1).filter(r => r[0] && r[0].trim() !== '' && r[0].trim() !== 'Total');

    // Parse Guardians CSV: Last name, First name, Relationship, Last Name, First Name, Phone, Email
    const gRecords = parse(guardianBuffer.toString('utf-8'), { columns: false, skip_empty_lines: true, relax_column_count: true });
    const gData = gRecords.slice(1);

    // Build guardian lookup: childKey -> [{relationship, parentFirst, parentLast, phone, email}]
    const guardianMap = {};
    for (const g of gData) {
      const childLast = (g[0] || '').trim().toLowerCase();
      const childFirst = (g[1] || '').trim().toLowerCase();
      const key = childLast + '|' + childFirst;
      if (!guardianMap[key]) guardianMap[key] = [];
      guardianMap[key].push({
        relationship: (g[2] || '').trim(),
        parentLast: (g[3] || '').trim(),
        parentFirst: (g[4] || '').trim(),
        phone: cleanPhone(g[5]),
        email: (g[6] || '').trim()
      });
    }

    // Process AR data: merge with guardians, group by family (parent email or last name)
    const familyMap = {}; // key -> { parentInfo, children[], totalBalance, aging }

    for (const row of arData) {
      const childLast = (row[0] || '').trim();
      const childFirst = (row[1] || '').trim();
      const agingTotal = parseBalance(row[8]);

      // We only care about children who owe money (positive Total aging)
      // But also track CDC children (negative Current with $0 aging) for reference
      const currentBal = parseBalance(row[2]);
      const isCDC = currentBal < 0 && agingTotal === 0;

      const childKey = childLast.toLowerCase() + '|' + childFirst.toLowerCase();
      const guardians = guardianMap[childKey] || [];

      // Find primary parent (prefer Mother/Parent with email)
      let primary = guardians.find(g => g.email && (g.relationship === 'Parent' || g.relationship === 'Mother'));
      if (!primary) primary = guardians.find(g => g.email);
      if (!primary) primary = guardians[0] || { parentFirst: '', parentLast: childLast, phone: '', email: '' };

      // Secondary parent
      let secondary = guardians.find(g => g !== primary && (g.email || g.phone));

      // Group by parent email (best) or parent last name (fallback)
      const familyKey = primary.email ? primary.email.toLowerCase() : (primary.parentLast || childLast).toLowerCase();

      if (!familyMap[familyKey]) {
        familyMap[familyKey] = {
          familyName: primary.parentLast || childLast,
          parentFirst: primary.parentFirst,
          parentLast: primary.parentLast || childLast,
          email: primary.email,
          phone: primary.phone,
          parent2First: secondary ? secondary.parentFirst : '',
          parent2Last: secondary ? secondary.parentLast : '',
          email2: secondary ? secondary.email : '',
          phone2: secondary ? secondary.phone : '',
          children: [],
          totalBalance: 0,
          aging: { current: 0, d1_30: 0, d31_60: 0, d61_90: 0, d90_plus: 0 },
          isCDC: false,
          cdcExpected: 0
        };
      }

      const family = familyMap[familyKey];
      const childBalance = agingTotal;
      const childAging = {
        current: parseBalance(row[2]),
        d1_30: parseBalance(row[3]),
        d31_60: parseBalance(row[4]),
        d61_90: parseBalance(row[5]),
        d90_plus: parseBalance(row[6])
      };

      family.children.push({
        childFirst, childLast,
        childName: childFirst + ' ' + childLast,
        balance: childBalance,
        aging: childAging,
        isCDC
      });

      if (childBalance > 0) {
        family.totalBalance += childBalance;
        family.aging.d1_30 += childAging.d1_30;
        family.aging.d31_60 += childAging.d31_60;
        family.aging.d61_90 += childAging.d61_90;
        family.aging.d90_plus += childAging.d90_plus;
      }

      if (isCDC) {
        family.isCDC = true;
        family.cdcExpected += Math.abs(currentBal);
      }
    }

    // Filter to only families that owe money OR are CDC
    const owingFamilies = Object.values(familyMap).filter(f => f.totalBalance > 0 || f.isCDC);

    if (preview) {
      return res.json({
        total_children: arData.length,
        total_families: Object.keys(familyMap).length,
        owing_families: owingFamilies.length,
        total_owed: owingFamilies.reduce((sum, f) => sum + f.totalBalance, 0),
        cdc_families: owingFamilies.filter(f => f.isCDC).length,
        cdc_expected: owingFamilies.reduce((sum, f) => sum + f.cdcExpected, 0),
        families: owingFamilies.map(f => ({
          familyName: f.parentFirst ? f.parentFirst + ' ' + f.parentLast : f.familyName,
          email: f.email,
          phone: f.phone,
          balance: f.totalBalance,
          isCDC: f.isCDC,
          cdcExpected: f.cdcExpected,
          children: f.children.map(c => ({ name: c.childName, balance: c.balance, isCDC: c.isCDC })),
          aging: f.aging
        }))
      });
    }

    // Actual import
    const client = await pool.connect();
    let imported = 0, updated = 0, skipped = 0;
    const errors = [];

    try {
      await client.query('BEGIN');

      for (const family of owingFamilies) {
        try {
          // Only import families that actually owe money (skip CDC-only with no family balance)
          if (family.totalBalance <= 0 && !family.isCDC) { skipped++; continue; }

          const displayName = family.parentLast + (family.parentFirst ? ', ' + family.parentFirst : '');

          // Check for existing account
          const existing = await client.query(
            `SELECT id FROM col_accounts WHERE
              (LOWER(REPLACE(email, ' ', '')) = LOWER(REPLACE($1, ' ', '')) AND $1 != '')
              OR (LOWER(REPLACE(family_name, ' ', '')) = LOWER(REPLACE($2, ' ', '')))`,
            [family.email || '', displayName]
          );

          let accountId;
          if (existing.rows.length > 0) {
            accountId = existing.rows[0].id;
            await client.query(
              `UPDATE col_accounts SET
                balance = $1, email = COALESCE(NULLIF($2,''), email), phone = COALESCE(NULLIF($3,''), phone),
                email2 = COALESCE(NULLIF($4,''), email2), phone2 = COALESCE(NULLIF($5,''), phone2),
                parent2_first = COALESCE(NULLIF($6,''), parent2_first), parent2_last = COALESCE(NULLIF($7,''), parent2_last),
                center = COALESCE(NULLIF($8,''), center),
                aging_1_30 = $9, aging_31_60 = $10, aging_61_90 = $11, aging_90_plus = $12,
                is_cdc = $13, cdc_expected = $14,
                status = CASE WHEN $1 > 0 THEN 'past_due' ELSE status END,
                source = 'playground', updated_at = NOW()
               WHERE id = $15`,
              [family.totalBalance, family.email, family.phone,
               family.email2, family.phone2, family.parent2First, family.parent2Last,
               center, family.aging.d1_30, family.aging.d31_60, family.aging.d61_90, family.aging.d90_plus,
               family.isCDC, family.cdcExpected, accountId]
            );
            updated++;
          } else {
            const ins = await client.query(
              `INSERT INTO col_accounts (family_name, parent_first, parent_last, email, phone,
                parent2_first, parent2_last, email2, phone2,
                center, track, status, balance, original_balance,
                aging_1_30, aging_31_60, aging_61_90, aging_90_plus,
                is_cdc, cdc_expected, source)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'current',$11,$12,$12,$13,$14,$15,$16,$17,$18,'playground')
               RETURNING id`,
              [displayName, family.parentFirst, family.parentLast, family.email, family.phone,
               family.parent2First, family.parent2Last, family.email2, family.phone2,
               center, family.totalBalance > 0 ? 'past_due' : 'active', family.totalBalance,
               family.aging.d1_30, family.aging.d31_60, family.aging.d61_90, family.aging.d90_plus,
               family.isCDC, family.cdcExpected]
            );
            accountId = ins.rows[0].id;
            imported++;
          }

          // Add children
          for (const child of family.children) {
            const existingChild = await client.query(
              `SELECT id FROM col_children WHERE account_id = $1 AND LOWER(child_name) = LOWER($2)`,
              [accountId, child.childName]
            );
            if (existingChild.rows.length === 0) {
              await client.query(
                `INSERT INTO col_children (account_id, child_first, child_last, child_name, balance,
                  aging_current, aging_1_30, aging_31_60, aging_61_90, aging_90_plus, is_cdc)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
                [accountId, child.childFirst, child.childLast, child.childName, child.balance,
                 child.aging.current, child.aging.d1_30, child.aging.d31_60, child.aging.d61_90, child.aging.d90_plus,
                 child.isCDC]
              );
            } else {
              await client.query(
                `UPDATE col_children SET balance=$1, aging_current=$2, aging_1_30=$3, aging_31_60=$4,
                  aging_61_90=$5, aging_90_plus=$6, is_cdc=$7 WHERE id=$8`,
                [child.balance, child.aging.current, child.aging.d1_30, child.aging.d31_60,
                 child.aging.d61_90, child.aging.d90_plus, child.isCDC, existingChild.rows[0].id]
              );
            }
          }
        } catch (rowErr) {
          errors.push(`${family.familyName}: ${rowErr.message}`);
        }
      }

      await client.query('COMMIT');

      await pool.query(
        'INSERT INTO col_activity_log (user_name, action, details) VALUES ($1,$2,$3)',
        ['System', 'playground_import', `${center}: Imported ${imported} new, updated ${updated}, skipped ${skipped}. ${errors.length} errors.`]
      );

      res.json({ imported, updated, skipped, errors: errors.slice(0, 20), total_families: owingFamilies.length, center });
    } catch (txErr) {
      await client.query('ROLLBACK');
      throw txErr;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Playground import error:', err);
    res.status(500).json({ error: err.message });
  }
});

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
// CDC RECONCILIATION ROUTES
// ============================================================
const { execSync } = require('child_process');
const fs = require('fs');
const os = require('os');

// --- Upload & Parse CDC Statement PDF ---
app.post('/api/cdc/upload', upload.single('cdcFile'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No PDF file uploaded' });

    // Write to temp file
    const tmpPath = path.join(os.tmpdir(), 'cdc_' + Date.now() + '.pdf');
    fs.writeFileSync(tmpPath, req.file.buffer);

    // Run Python parser
    let parsed;
    try {
      const output = execSync(`python3 parse_cdc.py "${tmpPath}"`, {
        cwd: process.cwd(),
        timeout: 30000,
        encoding: 'utf-8'
      });
      parsed = JSON.parse(output);
    } catch (pyErr) {
      fs.unlinkSync(tmpPath);
      return res.status(500).json({ error: 'PDF parsing failed: ' + (pyErr.stderr || pyErr.message) });
    }

    fs.unlinkSync(tmpPath);

    if (parsed.errors && parsed.errors.length > 0) {
      return res.status(400).json({ error: 'Parser errors: ' + parsed.errors.join(', ') });
    }

    // Match children to existing accounts
    for (const child of parsed.children) {
      const nameParts = child.name.trim().split(/\s+/);
      const firstName = nameParts[0];
      const lastName = nameParts[nameParts.length - 1];

      // Try to match by child name in col_children
      const match = await pool.query(
        `SELECT c.id as child_id, c.account_id, a.family_name, a.email, a.phone
         FROM col_children c JOIN col_accounts a ON c.account_id = a.id
         WHERE (LOWER(c.child_first) = LOWER($1) AND LOWER(c.child_last) = LOWER($2))
            OR LOWER(c.child_name) ILIKE $3
         LIMIT 1`,
        [firstName, lastName, '%' + firstName + '%' + lastName + '%']
      );

      if (match.rows.length > 0) {
        child.matched_account_id = match.rows[0].account_id;
        child.matched_family = match.rows[0].family_name;
        child.matched_email = match.rows[0].email;
      } else {
        child.matched_account_id = null;
        child.matched_family = null;
        child.matched_email = null;
      }
    }

    // Check for duplicate statement
    if (parsed.voucher) {
      const existing = await pool.query(
        'SELECT id FROM col_cdc_statements WHERE voucher = $1',
        [parsed.voucher]
      );
      if (existing.rows.length > 0) {
        parsed.duplicate_warning = true;
        parsed.existing_statement_id = existing.rows[0].id;
      }
    }

    res.json(parsed);
  } catch (err) {
    console.error('CDC upload error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Save CDC Statement (after review) ---
app.post('/api/cdc/save', async (req, res) => {
  try {
    const { parsed } = req.body;
    if (!parsed) return res.status(400).json({ error: 'No parsed data provided' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // Parse pay period dates
      let periodStart = null, periodEnd = null;
      if (parsed.pay_period) {
        const parts = parsed.pay_period.split(' - ');
        if (parts.length === 2) {
          periodStart = parts[0].trim();
          periodEnd = parts[1].trim();
        }
      }

      // Insert statement
      const stmt = await client.query(
        `INSERT INTO col_cdc_statements (voucher, voucher_date, provider_id, center,
          pay_period_start, pay_period_end, total_pay, net_total_pay,
          total_children, children_paid, children_no_auth, children_duplicate,
          status, raw_json)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'pending', $13)
         RETURNING id`,
        [parsed.voucher, parsed.voucher_date || null, parsed.provider_id, parsed.center,
         periodStart, periodEnd, parsed.total_pay, parsed.net_total_pay || parsed.total_pay,
         parsed.summary?.total_children || 0, parsed.summary?.children_paid || 0,
         parsed.summary?.children_no_auth || 0, parsed.summary?.children_duplicate || 0,
         JSON.stringify(parsed)]
      );
      const statementId = stmt.rows[0].id;

      // Insert children
      for (const child of parsed.children) {
        await client.query(
          `INSERT INTO col_cdc_children (statement_id, account_id, child_name, child_name_normalized,
            case_no, child_id_no, hours_auth, hours_billed, hours_paid,
            fc, max_rate, amount_paid, error_desc, is_paid, is_no_auth, is_duplicate)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
          [statementId, child.matched_account_id, child.name, child.name_normalized,
           child.case_no, child.child_id, child.hours_auth, child.hours_billed, child.hours_paid,
           child.fc, child.max_rate, child.amount_paid, child.error, child.is_paid,
           child.is_no_auth, child.is_duplicate]
        );
      }

      await client.query('COMMIT');

      await pool.query(
        'INSERT INTO col_activity_log (user_name, action, details) VALUES ($1, $2, $3)',
        ['Mary', 'cdc_statement_uploaded', `${parsed.center}: Voucher ${parsed.voucher}, ${parsed.summary?.total_children || 0} children, $${parsed.total_pay}`]
      );

      res.json({ success: true, statement_id: statementId });
    } catch (txErr) {
      await client.query('ROLLBACK');
      throw txErr;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('CDC save error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- Approve CDC Statement (shift no-auth balances to families) ---
app.post('/api/cdc/approve/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { shifts } = req.body; // Array of { child_id, account_id, amount }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      let totalShifted = 0;
      let familiesAffected = 0;

      for (const shift of (shifts || [])) {
        if (!shift.account_id || !shift.amount || shift.amount <= 0) continue;

        // Update account balance
        await client.query(
          `UPDATE col_accounts SET
            balance = balance + $1,
            cdc_gap = cdc_gap + $1,
            status = CASE WHEN status = 'active' THEN 'past_due' ELSE status END,
            updated_at = NOW()
           WHERE id = $2`,
          [shift.amount, shift.account_id]
        );

        // Mark CDC child as shifted
        await client.query(
          `UPDATE col_cdc_children SET balance_shifted = true, shift_amount = $1
           WHERE id = $2`,
          [shift.amount, shift.cdc_child_id]
        );

        // Log the shift
        await pool.query(
          'INSERT INTO col_activity_log (user_name, action, account_id, details) VALUES ($1, $2, $3, $4)',
          ['Mary', 'cdc_balance_shifted', shift.account_id,
           `CDC No Authorization: $${shift.amount} shifted to family balance for ${shift.child_name}`]
        );

        totalShifted += shift.amount;
        familiesAffected++;
      }

      // Mark statement as approved
      await client.query(
        `UPDATE col_cdc_statements SET status = 'approved', approved_by = 'Mary', approved_at = NOW()
         WHERE id = $1`,
        [id]
      );

      await client.query('COMMIT');

      res.json({ success: true, total_shifted: totalShifted, families_affected: familiesAffected });
    } catch (txErr) {
      await client.query('ROLLBACK');
      throw txErr;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('CDC approve error:', err);
    res.status(500).json({ error: err.message });
  }
});

// --- List CDC Statements ---
app.get('/api/cdc/statements', async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT s.*,
        (SELECT COUNT(*) FROM col_cdc_children WHERE statement_id = s.id AND is_no_auth = true AND balance_shifted = false) as pending_shifts
       FROM col_cdc_statements s
       ORDER BY s.created_at DESC LIMIT 50`
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- Get CDC Statement Detail ---
app.get('/api/cdc/statements/:id', async (req, res) => {
  try {
    const stmt = await pool.query('SELECT * FROM col_cdc_statements WHERE id = $1', [req.params.id]);
    if (stmt.rows.length === 0) return res.status(404).json({ error: 'Statement not found' });

    const children = await pool.query(
      `SELECT cc.*, a.family_name, a.email, a.phone
       FROM col_cdc_children cc
       LEFT JOIN col_accounts a ON cc.account_id = a.id
       WHERE cc.statement_id = $1
       ORDER BY cc.child_name`,
      [req.params.id]
    );

    res.json({ ...stmt.rows[0], children: children.rows });
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
    <button class="header-btn primary" onclick="showCDCModal()">🏛️ CDC Reconciliation</button>
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
  <div class="modal" style="max-width: 850px;">
    <div class="modal-header">
      <h2>Import from Playground</h2>
      <button class="modal-close" onclick="closeModal('importModal')">&times;</button>
    </div>
    <div class="modal-body" id="importBody">
      <div class="form-group">
        <label>Center *</label>
        <select id="importCenter" style="max-width:300px;">
          <option value="">Select Center</option>
          <option value="Peace Boulevard">Peace Boulevard</option>
          <option value="Niles">Niles</option>
          <option value="Montessori">Montessori</option>
        </select>
      </div>

      <div style="display:grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom:16px;">
        <div class="import-zone" id="arZone" style="padding:24px;">
          <p style="font-size:14px; font-weight:600; margin-bottom:6px;">📊 Step 1: AR Aging Report</p>
          <p style="font-size:12px; color:#888; margin-bottom:10px;">Accounts Receivable Aging (Non Zero) - Monthly</p>
          <label for="arFile" style="padding:8px 16px; background:#E3F2FD; border-radius:6px; font-size:13px;">Choose File</label>
          <input type="file" id="arFile" accept=".csv" onchange="fileSelected('ar', this.files[0])" style="display:none;">
          <div id="arFileName" style="margin-top:8px; font-size:12px; color:#27AE60;"></div>
        </div>
        <div class="import-zone" id="guardianZone" style="padding:24px;">
          <p style="font-size:14px; font-weight:600; margin-bottom:6px;">👪 Step 2: Primary Guardians</p>
          <p style="font-size:12px; color:#888; margin-bottom:10px;">Primary Guardians report for parent contact info</p>
          <label for="guardianFile" style="padding:8px 16px; background:#E3F2FD; border-radius:6px; font-size:13px;">Choose File</label>
          <input type="file" id="guardianFile" accept=".csv" onchange="fileSelected('guardian', this.files[0])" style="display:none;">
          <div id="guardianFileName" style="margin-top:8px; font-size:12px; color:#27AE60;"></div>
        </div>
      </div>

      <button class="btn btn-primary" id="previewBtn" onclick="previewPlaygroundImport()" disabled>Preview Import</button>

      <div id="importPreview" style="display:none; margin-top:16px;"></div>
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

<!-- CDC Reconciliation Modal -->
<div class="modal-overlay" id="cdcModal">
  <div class="modal" style="max-width: 950px;">
    <div class="modal-header">
      <h2>CDC Reconciliation</h2>
      <button class="modal-close" onclick="closeModal('cdcModal')">&times;</button>
    </div>
    <div class="modal-body" id="cdcBody">
      <div style="display:flex; gap:16px; margin-bottom:16px;">
        <button class="btn btn-primary btn-sm" onclick="showCDCUpload()" id="cdcUploadTab" style="opacity:1;">Upload Statement</button>
        <button class="btn btn-outline btn-sm" onclick="showCDCHistory()" id="cdcHistoryTab">Statement History</button>
      </div>
      <div id="cdcUploadSection">
        <div class="import-zone" style="padding:24px;">
          <p style="font-size:14px; font-weight:600; margin-bottom:6px;">Upload CDC Statement PDF (DHS-1381)</p>
          <p style="font-size:12px; color:#888; margin-bottom:10px;">Upload the biweekly CDC Statement of Payments from MDHHS</p>
          <label for="cdcFile" style="padding:8px 16px; background:#E3F2FD; border-radius:6px; font-size:13px; cursor:pointer;">Choose PDF File</label>
          <input type="file" id="cdcFile" accept=".pdf" onchange="handleCDCUpload(this.files[0])" style="display:none;">
          <div id="cdcFileName" style="margin-top:8px; font-size:12px; color:#27AE60;"></div>
        </div>
        <div id="cdcParseResult" style="display:none; margin-top:16px;"></div>
      </div>
      <div id="cdcHistorySection" style="display:none;"></div>
    </div>
  </div>
</div>

<!-- CDC Review Modal -->
<div class="modal-overlay" id="cdcReviewModal">
  <div class="modal" style="max-width: 950px;">
    <div class="modal-header">
      <h2 id="cdcReviewTitle">Review CDC Statement</h2>
      <button class="modal-close" onclick="closeModal('cdcReviewModal')">&times;</button>
    </div>
    <div class="modal-body" id="cdcReviewBody"></div>
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
          <td>\${esc(a.center || '-')}</td>
          <td><span class="badge badge-\${a.track}">\${a.track}</span></td>
          <td><span class="badge badge-\${a.status}">\${a.status.replace('_',' ')}</span></td>
          <td style="font-weight:600; color:\${a.balance > 0 ? '#E74C3C' : '#27AE60'};">\${fmt(a.balance)}</td>
          <td style="font-size:12px;">\${esc(a.email || '-')}</td>
          <td style="font-size:12px;">\${esc(a.phone || '-')}</td>
        </tr>\`;
      }).join('');
    }

    const start = currentOffset + 1;
    const end = Math.min(currentOffset + PAGE_SIZE, currentTotal);
    document.getElementById('pageInfo').textContent = currentTotal > 0
      ? \`Showing \${start}-\${end} of \${currentTotal} accounts\`
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
          <div class="detail-item"><span class="label">Source</span><span class="value">\${esc(a.source || '-')}</span></div>
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
          ? a.activity.map(act => \`<div class="activity-item"><span class="time">\${new Date(act.created_at).toLocaleString()}</span> - <span class="action">\${esc(act.user_name || '')}</span>: \${esc(act.action)} \${act.details ? '- '+esc(act.details) : ''}</div>\`).join('')
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
  document.getElementById('arFileName').textContent = '';
  document.getElementById('guardianFileName').textContent = '';
  document.getElementById('previewBtn').disabled = true;
  window._arFile = null;
  window._guardianFile = null;
  openModal('importModal');
}

function fileSelected(type, file) {
  if (!file) return;
  if (type === 'ar') {
    window._arFile = file;
    document.getElementById('arFileName').textContent = '✓ ' + file.name;
  } else {
    window._guardianFile = file;
    document.getElementById('guardianFileName').textContent = '✓ ' + file.name;
  }
  document.getElementById('previewBtn').disabled = !(window._arFile && window._guardianFile);
}

async function previewPlaygroundImport() {
  const center = document.getElementById('importCenter').value;
  if (!center) return toast('Please select a center', 'error');
  if (!window._arFile || !window._guardianFile) return toast('Both files are required', 'error');

  const formData = new FormData();
  formData.append('arFile', window._arFile);
  formData.append('guardianFile', window._guardianFile);
  formData.append('center', center);
  formData.append('preview', 'true');

  try {
    const res = await fetch('/api/import/playground', { method: 'POST', body: formData });
    const data = await res.json();
    if (data.error) return toast(data.error, 'error');

    const preview = document.getElementById('importPreview');
    preview.style.display = 'block';

    let html = '<div style="background:#f8f9fa; padding:16px; border-radius:8px; margin-bottom:16px;">';
    html += '<div style="display:grid; grid-template-columns:repeat(4,1fr); gap:12px; text-align:center;">';
    html += '<div><div style="font-size:24px; font-weight:700; color:#1B4F72;">' + data.total_children + '</div><div style="font-size:11px; color:#888;">Total Children</div></div>';
    html += '<div><div style="font-size:24px; font-weight:700; color:#E74C3C;">' + data.owing_families + '</div><div style="font-size:11px; color:#888;">Families Owing</div></div>';
    html += '<div><div style="font-size:24px; font-weight:700; color:#E74C3C;">' + fmt(data.total_owed) + '</div><div style="font-size:11px; color:#888;">Total Owed</div></div>';
    html += '<div><div style="font-size:24px; font-weight:700; color:#F39C12;">' + data.cdc_families + '</div><div style="font-size:11px; color:#888;">CDC Families</div></div>';
    html += '</div></div>';

    if (data.families.length > 0) {
      html += '<table class="preview-table"><thead><tr><th>Parent</th><th>Email</th><th>Phone</th><th>Children</th><th>Balance</th><th>CDC</th><th>Aging</th></tr></thead><tbody>';
      data.families.filter(f => f.balance > 0).sort((a,b) => b.balance - a.balance).forEach(f => {
        const kids = f.children.map(c => c.name + (c.isCDC ? ' (CDC)' : '')).join(', ');
        const aging = [];
        if (f.aging.d1_30 > 0) aging.push('1-30d: ' + fmt(f.aging.d1_30));
        if (f.aging.d31_60 > 0) aging.push('31-60d: ' + fmt(f.aging.d31_60));
        if (f.aging.d61_90 > 0) aging.push('61-90d: ' + fmt(f.aging.d61_90));
        if (f.aging.d90_plus > 0) aging.push('90+d: ' + fmt(f.aging.d90_plus));
        html += '<tr><td style="font-weight:600;">' + esc(f.familyName) + '</td><td style="font-size:11px;">' + esc(f.email||'-') + '</td><td style="font-size:11px;">' + esc(f.phone||'-') + '</td><td style="font-size:11px;">' + esc(kids) + '</td><td style="font-weight:600; color:#E74C3C;">' + fmt(f.balance) + '</td><td>' + (f.isCDC ? '<span class="badge badge-arrangement">CDC</span>' : '') + '</td><td style="font-size:10px;">' + aging.join('<br>') + '</td></tr>';
      });
      html += '</tbody></table>';
    }

    html += '<div style="margin-top:16px; display:flex; gap:10px; align-items:center;">';
    html += '<button class="btn btn-primary" onclick="executePlaygroundImport()">Import ' + data.owing_families + ' Families into ' + esc(center) + '</button>';
    html += '<span style="font-size:12px; color:#888;">Only families with balances > $0 will be imported</span>';
    html += '</div>';

    preview.innerHTML = html;
  } catch(e) { console.error(e); toast('Error previewing import', 'error'); }
}

async function executePlaygroundImport() {
  const center = document.getElementById('importCenter').value;
  const formData = new FormData();
  formData.append('arFile', window._arFile);
  formData.append('guardianFile', window._guardianFile);
  formData.append('center', center);

  try {
    const res = await fetch('/api/import/playground', { method: 'POST', body: formData });
    const data = await res.json();
    if (data.error) return toast(data.error, 'error');

    document.getElementById('importPreview').innerHTML = \`
      <div style="text-align:center; padding:24px;">
        <div style="font-size:48px; margin-bottom:10px;">✅</div>
        <h3 style="color:#27AE60;">Import Complete - \${esc(data.center)}</h3>
        <p style="margin-top:12px; font-size:15px;">
          <strong>\${data.imported}</strong> new family accounts imported<br>
          <strong>\${data.updated}</strong> existing accounts updated<br>
          <strong>\${data.total_families}</strong> total families processed
        </p>
        \${data.errors.length > 0 ? '<p style="color:#E74C3C; font-size:12px; margin-top:8px;">' + data.errors.length + ' errors</p>' : ''}
        <button class="btn btn-primary" style="margin-top:16px;" onclick="closeModal('importModal'); loadAccounts(); loadStats();">Done</button>
      </div>
    \`;
    toast('Imported ' + data.imported + ' accounts for ' + data.center + '!', 'success');
  } catch(e) { toast('Import failed', 'error'); }
}

// CDC Reconciliation
let cdcParsedData = null;

function showCDCModal() {
  document.getElementById('cdcParseResult').style.display = 'none';
  document.getElementById('cdcParseResult').innerHTML = '';
  document.getElementById('cdcFileName').textContent = '';
  showCDCUpload();
  openModal('cdcModal');
}

function showCDCUpload() {
  document.getElementById('cdcUploadSection').style.display = 'block';
  document.getElementById('cdcHistorySection').style.display = 'none';
  document.getElementById('cdcUploadTab').className = 'btn btn-primary btn-sm';
  document.getElementById('cdcHistoryTab').className = 'btn btn-outline btn-sm';
}

async function showCDCHistory() {
  document.getElementById('cdcUploadSection').style.display = 'none';
  document.getElementById('cdcHistorySection').style.display = 'block';
  document.getElementById('cdcUploadTab').className = 'btn btn-outline btn-sm';
  document.getElementById('cdcHistoryTab').className = 'btn btn-primary btn-sm';

  try {
    const res = await fetch('/api/cdc/statements');
    const stmts = await res.json();
    const section = document.getElementById('cdcHistorySection');

    if (stmts.length === 0) {
      section.innerHTML = '<p style="color:#999; text-align:center; padding:20px;">No CDC statements uploaded yet.</p>';
      return;
    }

    let html = '<table class="preview-table"><thead><tr><th>Date</th><th>Center</th><th>Voucher</th><th>Pay Period</th><th>Children</th><th>No Auth</th><th>Total Paid</th><th>Status</th><th></th></tr></thead><tbody>';
    stmts.forEach(s => {
      const statusClass = s.status === 'approved' ? 'badge-paid' : s.status === 'pending' ? 'badge-past_due' : 'badge-active';
      const periodStart = s.pay_period_start ? new Date(s.pay_period_start).toLocaleDateString() : '';
      const periodEnd = s.pay_period_end ? new Date(s.pay_period_end).toLocaleDateString() : '';
      html += '<tr><td>' + new Date(s.created_at).toLocaleDateString() + '</td><td>' + esc(s.center) + '</td><td>' + esc(s.voucher) + '</td><td style="font-size:11px;">' + periodStart + ' - ' + periodEnd + '</td><td>' + s.total_children + '</td><td style="color:#E74C3C; font-weight:600;">' + s.children_no_auth + '</td><td>' + fmt(s.total_pay) + '</td><td><span class="badge ' + statusClass + '">' + s.status + '</span></td><td><button class="btn btn-sm btn-outline" onclick="viewCDCStatement(' + s.id + ')">View</button></td></tr>';
    });
    html += '</tbody></table>';
    section.innerHTML = html;
  } catch(e) { toast('Error loading CDC history', 'error'); }
}

async function handleCDCUpload(file) {
  if (!file) return;
  document.getElementById('cdcFileName').textContent = 'Parsing ' + file.name + '...';

  const formData = new FormData();
  formData.append('cdcFile', file);

  try {
    const res = await fetch('/api/cdc/upload', { method: 'POST', body: formData });
    const data = await res.json();

    if (data.error) { toast(data.error, 'error'); document.getElementById('cdcFileName').textContent = ''; return; }

    cdcParsedData = data;
    document.getElementById('cdcFileName').textContent = file.name;

    const result = document.getElementById('cdcParseResult');
    result.style.display = 'block';

    const s = data.summary;
    let html = '<div style="background:#f8f9fa; padding:16px; border-radius:8px; margin-bottom:16px;">';
    html += '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">';
    html += '<div><strong>' + esc(data.center) + '</strong> - Voucher ' + esc(data.voucher) + '</div>';
    html += '<div style="font-size:13px; color:#888;">Pay Period: ' + esc(data.pay_period) + '</div>';
    html += '</div>';

    if (data.duplicate_warning) {
      html += '<div style="background:#FFF3E0; padding:10px; border-radius:6px; margin-bottom:12px; font-size:13px; color:#E65100;">This statement (voucher ' + esc(data.voucher) + ') has already been uploaded. Saving again will create a duplicate.</div>';
    }

    html += '<div style="display:grid; grid-template-columns:repeat(5,1fr); gap:12px; text-align:center;">';
    html += '<div><div style="font-size:24px; font-weight:700; color:#1B4F72;">' + s.total_children + '</div><div style="font-size:11px; color:#888;">Total Children</div></div>';
    html += '<div><div style="font-size:24px; font-weight:700; color:#27AE60;">' + s.children_paid + '</div><div style="font-size:11px; color:#888;">Paid</div></div>';
    html += '<div><div style="font-size:24px; font-weight:700; color:#E74C3C;">' + s.children_no_auth + '</div><div style="font-size:11px; color:#888;">No Authorization</div></div>';
    html += '<div><div style="font-size:24px; font-weight:700; color:#888;">' + s.children_duplicate + '</div><div style="font-size:11px; color:#888;">Duplicate</div></div>';
    html += '<div><div style="font-size:24px; font-weight:700; color:#27AE60;">' + fmt(s.total_paid) + '</div><div style="font-size:11px; color:#888;">CDC Paid</div></div>';
    html += '</div></div>';

    // Show No Authorization children prominently
    const noAuth = data.children.filter(c => c.is_no_auth);
    if (noAuth.length > 0) {
      html += '<div style="background:#FCE4EC; padding:16px; border-radius:8px; margin-bottom:16px;">';
      html += '<h4 style="color:#C62828; margin-bottom:10px;">No Authorization - Family Responsibility</h4>';
      html += '<table class="preview-table"><thead><tr><th>Child Name</th><th>Case No.</th><th>Matched Family</th><th>Family Email</th><th>Expected Amount</th></tr></thead><tbody>';
      noAuth.forEach(c => {
        const expected = c.hours_billed > 0 ? (c.hours_billed * c.max_rate).toFixed(2) : 'Unknown';
        html += '<tr><td style="font-weight:600;">' + esc(c.name) + '</td><td style="font-size:11px;">' + esc(c.case_no) + '</td><td>' + (c.matched_family ? esc(c.matched_family) : '<span style="color:#E74C3C;">Not matched</span>') + '</td><td style="font-size:11px;">' + esc(c.matched_email || '-') + '</td><td style="font-weight:600; color:#E74C3C;">$' + expected + '</td></tr>';
      });
      html += '</tbody></table>';
      html += '</div>';
    }

    // Show all paid children
    const paid = data.children.filter(c => c.is_paid);
    if (paid.length > 0) {
      html += '<details style="margin-bottom:16px;"><summary style="cursor:pointer; font-weight:600; color:#1B4F72; margin-bottom:8px;">Paid Children (' + paid.length + ') - click to expand</summary>';
      html += '<table class="preview-table"><thead><tr><th>Child</th><th>Hours Billed</th><th>Hours Paid</th><th>Rate</th><th>FC</th><th>Amount Paid</th><th>Notes</th></tr></thead><tbody>';
      paid.forEach(c => {
        html += '<tr><td>' + esc(c.name) + '</td><td>' + c.hours_billed + '</td><td>' + c.hours_paid + '</td><td>$' + (c.max_rate||0).toFixed(2) + '</td><td>$' + (c.fc||0).toFixed(2) + '</td><td style="color:#27AE60; font-weight:600;">' + fmt(c.amount_paid) + '</td><td style="font-size:11px; color:#888;">' + esc(c.error || '') + '</td></tr>';
      });
      html += '</tbody></table></details>';
    }

    html += '<div style="display:flex; gap:10px; margin-top:16px;">';
    html += '<button class="btn btn-primary" onclick="saveCDCStatement()">Save Statement</button>';
    html += '<span style="font-size:12px; color:#888; align-self:center;">Saves the statement for review. You can approve and shift balances after saving.</span>';
    html += '</div>';

    result.innerHTML = html;
  } catch(e) { console.error(e); toast('Error parsing CDC statement', 'error'); document.getElementById('cdcFileName').textContent = ''; }
}

async function saveCDCStatement() {
  if (!cdcParsedData) return toast('No parsed data to save', 'error');

  try {
    const res = await fetch('/api/cdc/save', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ parsed: cdcParsedData })
    });
    const data = await res.json();
    if (data.error) return toast(data.error, 'error');

    toast('CDC statement saved!', 'success');
    viewCDCStatement(data.statement_id);
  } catch(e) { toast('Error saving statement', 'error'); }
}

async function viewCDCStatement(id) {
  try {
    const res = await fetch('/api/cdc/statements/' + id);
    const data = await res.json();
    if (data.error) return toast(data.error, 'error');

    document.getElementById('cdcReviewTitle').textContent = 'CDC Statement - ' + (data.center || '') + ' - ' + (data.voucher || '');

    const noAuth = data.children.filter(c => c.is_no_auth && !c.balance_shifted);
    const shifted = data.children.filter(c => c.balance_shifted);
    const paid = data.children.filter(c => c.is_paid);

    let html = '<div style="display:grid; grid-template-columns:repeat(4,1fr); gap:12px; text-align:center; background:#f8f9fa; padding:16px; border-radius:8px; margin-bottom:16px;">';
    html += '<div><div style="font-size:20px; font-weight:700;">' + data.total_children + '</div><div style="font-size:11px; color:#888;">Children</div></div>';
    html += '<div><div style="font-size:20px; font-weight:700; color:#27AE60;">' + fmt(data.total_pay) + '</div><div style="font-size:11px; color:#888;">CDC Paid</div></div>';
    html += '<div><div style="font-size:20px; font-weight:700; color:#E74C3C;">' + noAuth.length + '</div><div style="font-size:11px; color:#888;">Pending Shifts</div></div>';
    html += '<div><span class="badge badge-' + (data.status === 'approved' ? 'paid' : 'past_due') + '">' + data.status + '</span></div>';
    html += '</div>';

    if (noAuth.length > 0 && data.status !== 'approved') {
      html += '<div style="background:#FCE4EC; padding:16px; border-radius:8px; margin-bottom:16px;">';
      html += '<h4 style="color:#C62828; margin-bottom:10px;">No Authorization - Approve to Shift Balance to Families</h4>';
      html += '<p style="font-size:12px; color:#888; margin-bottom:10px;">Approving will add these amounts to each family\'s balance and mark them as past due.</p>';
      html += '<table class="preview-table"><thead><tr><th>Child</th><th>Matched Family</th><th>Email</th><th>Estimated Amount</th></tr></thead><tbody>';
      noAuth.forEach(c => {
        const est = c.hours_billed > 0 && c.max_rate > 0 ? (c.hours_billed * c.max_rate).toFixed(2) : '0.00';
        html += '<tr><td style="font-weight:600;">' + esc(c.child_name) + '</td><td>' + (c.family_name ? esc(c.family_name) : '<span style="color:#E74C3C;">Not matched</span>') + '</td><td style="font-size:11px;">' + esc(c.email || '-') + '</td><td style="font-weight:600; color:#E74C3C;">$' + est + '</td></tr>';
      });
      html += '</tbody></table>';
      html += '<button class="btn btn-danger" style="margin-top:12px;" onclick="approveCDCShifts(' + data.id + ')">Approve & Shift Balances to Families</button>';
      html += '</div>';
    }

    if (shifted.length > 0) {
      html += '<div style="background:#FFF3E0; padding:16px; border-radius:8px; margin-bottom:16px;">';
      html += '<h4 style="color:#E65100; margin-bottom:10px;">Already Shifted to Families (' + shifted.length + ')</h4>';
      shifted.forEach(c => {
        html += '<div style="padding:4px 0; font-size:13px;">' + esc(c.child_name) + ' - $' + (c.shift_amount||0).toFixed(2) + ' shifted to ' + esc(c.family_name || 'unknown') + '</div>';
      });
      html += '</div>';
    }

    if (paid.length > 0) {
      html += '<details><summary style="cursor:pointer; font-weight:600; color:#1B4F72; margin-bottom:8px;">Paid Children (' + paid.length + ')</summary>';
      html += '<table class="preview-table"><thead><tr><th>Child</th><th>Amount Paid</th><th>Hours</th><th>Notes</th></tr></thead><tbody>';
      paid.forEach(c => {
        html += '<tr><td>' + esc(c.child_name) + '</td><td style="color:#27AE60;">' + fmt(c.amount_paid) + '</td><td>' + c.hours_paid + '/' + c.hours_billed + '</td><td style="font-size:11px;">' + esc(c.error_desc||'') + '</td></tr>';
      });
      html += '</tbody></table></details>';
    }

    document.getElementById('cdcReviewBody').innerHTML = html;
    closeModal('cdcModal');
    openModal('cdcReviewModal');
  } catch(e) { console.error(e); toast('Error loading statement', 'error'); }
}

async function approveCDCShifts(statementId) {
  try {
    const res = await fetch('/api/cdc/statements/' + statementId);
    const data = await res.json();

    const noAuth = data.children.filter(c => c.is_no_auth && !c.balance_shifted && c.account_id);
    const shifts = noAuth.map(c => ({
      cdc_child_id: c.id,
      account_id: c.account_id,
      child_name: c.child_name,
      amount: c.hours_billed > 0 && c.max_rate > 0 ? parseFloat((c.hours_billed * c.max_rate).toFixed(2)) : 0
    })).filter(s => s.amount > 0);

    if (shifts.length === 0) return toast('No matched families with amounts to shift', 'error');

    const totalToShift = shifts.reduce((sum, s) => sum + s.amount, 0);
    if (!confirm('This will add $' + totalToShift.toFixed(2) + ' to ' + shifts.length + ' family accounts. Continue?')) return;

    const approveRes = await fetch('/api/cdc/approve/' + statementId, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ shifts })
    });
    const result = await approveRes.json();

    if (result.error) return toast(result.error, 'error');

    toast('Shifted $' + result.total_shifted.toFixed(2) + ' to ' + result.families_affected + ' families', 'success');
    viewCDCStatement(statementId);
    loadStats();
    loadAccounts();
  } catch(e) { console.error(e); toast('Error approving shifts', 'error'); }
}

// Activity Log
async function loadActivity() {
  try {
    const res = await fetch('/api/activity?limit=100');
    const logs = await res.json();
    document.getElementById('activityBody').innerHTML = logs.length > 0
      ? logs.map(l => \`<div class="activity-item">
          <span class="time">\${new Date(l.created_at).toLocaleString()}</span> -
          <span class="action">\${esc(l.user_name||'System')}</span>: \${esc(l.action)}
          \${l.family_name ? ' - <em>'+esc(l.family_name)+'</em>' : ''}
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
    console.log(`TCC Collections Hub running on port ${PORT}`);
  });
}).catch(err => {
  console.error('Failed to initialize database:', err);
  process.exit(1);
});
