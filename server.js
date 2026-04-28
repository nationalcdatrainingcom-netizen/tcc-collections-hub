require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { execFile } = require('child_process');

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

// SendGrid is optional — only used for emailing payment links to families.
// If SENDGRID_API_KEY isn't set, the "Send Email" button is hidden and you
// can still copy/paste the Stripe URLs into your own email client.
let sendgrid = null;
try {
  if (process.env.SENDGRID_API_KEY) {
    sendgrid = require('@sendgrid/mail');
    sendgrid.setApiKey(process.env.SENDGRID_API_KEY);
    console.log('SendGrid initialized');
  } else {
    console.log('SendGrid disabled (SENDGRID_API_KEY not set)');
  }
} catch (e) {
  console.log('SendGrid module not installed — email sending will be unavailable');
}

// Twilio is optional — only used for SMS touches in the 7-touch outreach campaign.
// If TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_FROM_NUMBER aren't set,
// SMS touches are queued but skipped with status 'skipped_sms_disabled'.
// When you set SMS_ENABLED=true in env vars, SMS touches will start sending.
let twilio = null;
try {
  if (process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN) {
    twilio = require('twilio')(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
    console.log('Twilio initialized');
    if (process.env.SMS_ENABLED !== 'true') {
      console.log('SMS sending DISABLED (set SMS_ENABLED=true after Twilio A2P 10DLC approves)');
    } else {
      console.log('SMS sending ENABLED');
    }
  } else {
    console.log('Twilio disabled (TWILIO_ACCOUNT_SID/AUTH_TOKEN not set)');
  }
} catch (e) {
  console.log('Twilio module not installed — SMS sending will be unavailable');
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
  res.json({
    allowDirectAccess: ALLOW_DIRECT_ACCESS,
    user: ALLOW_DIRECT_ACCESS ? DIRECT_ACCESS_USER : null,
    capabilities: {
      stripe: !!stripe,
      sendgrid: !!sendgrid,
      twilio: !!twilio,
      sms_enabled: !!twilio && process.env.SMS_ENABLED === 'true',
    },
  });
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

    -- Weekly subscription link columns (for balances >$600 where weekly is offered)
    ALTER TABLE collections_families ADD COLUMN IF NOT EXISTS weekly_link_url TEXT;
    ALTER TABLE collections_families ADD COLUMN IF NOT EXISTS weekly_link_id TEXT;

    -- Email unsubscribe tracking (RFC 2369 List-Unsubscribe + one-click List-Unsubscribe-Post)
    -- A family who clicks unsubscribe in their email client never receives automated
    -- emails again, but Mary can still manually email them if needed.
    ALTER TABLE collections_families ADD COLUMN IF NOT EXISTS email_unsubscribed BOOLEAN DEFAULT false;
    ALTER TABLE collections_families ADD COLUMN IF NOT EXISTS email_unsubscribed_at TIMESTAMPTZ;
    ALTER TABLE collections_families ADD COLUMN IF NOT EXISTS email_unsubscribe_token TEXT;

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

    -- 7-touch outreach campaign system. When Mary clicks "Start Sequence" on a
    -- family, an enrollment row is created and 7 touch rows are scheduled.
    -- A background scheduler ticks every 15 minutes and fires due touches.
    -- The sequence auto-stops on payment, settlement, or unsubscribe.
    CREATE TABLE IF NOT EXISTS campaign_enrollments (
      id SERIAL PRIMARY KEY,
      family_id INTEGER REFERENCES collections_families(id) ON DELETE CASCADE,
      cadence TEXT NOT NULL DEFAULT 'standard',  -- 'standard' (only one for now)
      status TEXT NOT NULL DEFAULT 'active',     -- 'active', 'paused', 'completed', 'cancelled'
      enrolled_at TIMESTAMPTZ DEFAULT NOW(),
      paused_at TIMESTAMPTZ,
      paused_reason TEXT,
      completed_at TIMESTAMPTZ,
      completed_reason TEXT,
      last_payment_at TIMESTAMPTZ,
      created_by TEXT,
      UNIQUE(family_id)  -- only one active enrollment per family at a time
    );
    CREATE INDEX IF NOT EXISTS idx_enrollments_status ON campaign_enrollments(status);

    CREATE TABLE IF NOT EXISTS campaign_touches (
      id SERIAL PRIMARY KEY,
      enrollment_id INTEGER REFERENCES campaign_enrollments(id) ON DELETE CASCADE,
      family_id INTEGER REFERENCES collections_families(id) ON DELETE CASCADE,
      touch_number INTEGER NOT NULL,             -- 1..7
      channel TEXT NOT NULL,                     -- 'email' or 'sms'
      scheduled_for TIMESTAMPTZ NOT NULL,
      sent_at TIMESTAMPTZ,
      send_status TEXT,                          -- 'sent', 'failed', 'skipped_sms_disabled', 'skipped_no_phone'
      error_detail TEXT,
      sendgrid_message_id TEXT,                  -- for click/open tracking
      twilio_sid TEXT,                           -- for SMS delivery tracking
      opened_at TIMESTAMPTZ,
      clicked_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_touches_due ON campaign_touches(scheduled_for, sent_at) WHERE sent_at IS NULL;
    CREATE INDEX IF NOT EXISTS idx_touches_enrollment ON campaign_touches(enrollment_id);

    -- Full audit-trail ledger for past-due families. Every charge, payment,
    -- adjustment, refund, and credit is one row here. Remaining balance is
    -- computed as original_balance - SUM(signed amounts) in this table.
    -- Positive amounts reduce what the family owes (payments, credits).
    -- Negative amounts increase what they owe (new charges, reversals).
    CREATE TABLE IF NOT EXISTS collections_transactions (
      id SERIAL PRIMARY KEY,
      family_id INTEGER REFERENCES collections_families(id) ON DELETE CASCADE,
      txn_date DATE NOT NULL DEFAULT CURRENT_DATE,
      txn_type TEXT NOT NULL,                        -- 'starting_balance', 'charge', 'payment', 'credit', 'refund', 'pay_in_full_discount', 'other'
      txn_type_label TEXT,                           -- optional custom label when type='other'
      amount NUMERIC(10,2) NOT NULL,                 -- signed: positive = reduces balance, negative = increases
      description TEXT,                              -- required for manual adjustments
      reason TEXT,                                   -- required for adjustments — why this change was made
      stripe_payment_intent_id TEXT,                 -- link to Stripe payment if applicable
      created_by TEXT,                               -- who added this (for audit)
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_coll_txn_family ON collections_transactions(family_id, txn_date DESC);

    -- Link table: which roster children belong to which past-due family.
    -- Enables email/phone to be pulled from children records (parent contact fields)
    -- when provided, and shows the family's children on the detail page.
    CREATE TABLE IF NOT EXISTS collections_family_children (
      id SERIAL PRIMARY KEY,
      family_id INTEGER REFERENCES collections_families(id) ON DELETE CASCADE,
      child_id INTEGER REFERENCES children(id) ON DELETE CASCADE,
      UNIQUE(family_id, child_id)
    );

    -- Parent contact fields on children — populated from roster imports later
    ALTER TABLE children ADD COLUMN IF NOT EXISTS parent_email TEXT;
    ALTER TABLE children ADD COLUMN IF NOT EXISTS parent_phone TEXT;
    ALTER TABLE children ADD COLUMN IF NOT EXISTS parent_name TEXT;

    -- Tuition rate sheets, seeded from the official center rate sheets.
    -- One row per program tier per center. The billing engine reads these
    -- to compute what a family should be charged for a given week/period.
    -- If a legacy version of this table exists (with rate_type/weekly_amount
    -- columns), drop it so the new schema applies cleanly. The table is
    -- never written to except by the seeded rates loader, so no data is lost.
    DO $migrate$
    BEGIN
      IF EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_name='tuition_rates' AND column_name='rate_type') THEN
        DROP TABLE tuition_rates CASCADE;
      END IF;
    END
    $migrate$;

    CREATE TABLE IF NOT EXISTS tuition_rates (
      id SERIAL PRIMARY KEY,
      center TEXT NOT NULL,
      program_key TEXT NOT NULL,
      description TEXT NOT NULL,
      weekly_rate NUMERIC(10,2) NOT NULL,
      age_min_months INTEGER,
      age_max_months INTEGER,
      hours_min NUMERIC(5,2),
      hours_max NUMERIC(5,2),
      effective_date DATE NOT NULL DEFAULT '2025-01-01',
      notes TEXT,
      active BOOLEAN DEFAULT true,
      UNIQUE(center, program_key, effective_date)
    );

    -- Guardian / parent contact records imported from Playground guardian exports.
    -- A child has 1..N guardians (parent, grandmother, family friend, etc.).
    -- The is_primary flag marks the primary guardian (first "Parent" role or
    -- explicit "Primary Guardian" role). Auto-linking past-due families uses
    -- the primary guardian.
    CREATE TABLE IF NOT EXISTS guardians (
      id SERIAL PRIMARY KEY,
      center TEXT NOT NULL,
      child_id INTEGER REFERENCES children(id) ON DELETE CASCADE,
      child_first TEXT,
      child_last TEXT,
      guardian_first TEXT,
      guardian_last TEXT,
      relationship TEXT,
      email TEXT,
      phone TEXT,
      role TEXT,
      is_primary BOOLEAN DEFAULT false,
      source_account_number TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(center, child_id, guardian_first, guardian_last, relationship)
    );
    CREATE INDEX IF NOT EXISTS idx_guardians_child ON guardians(child_id);
    CREATE INDEX IF NOT EXISTS idx_guardians_name ON guardians(LOWER(guardian_last), LOWER(guardian_first));
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
  // Seed tuition rates from the 2025 rate sheets (Niles, St. Joseph/Peace, MCC 2025-26).
  // Each center uses its own sheet exclusively. Rates are upserted on every boot so
  // corrections to the rate sheets propagate automatically.
  const RATES_2025 = {
    'Niles': [
      // key, description, weekly_rate, age_min_mo, age_max_mo, hours_min, hours_max, notes
      ['infant_toddler',        'Infants & Toddlers (Under 2½ years)',          352.00, 0,   30,  null, null, 'Full time, all under 2.5'],
      ['older_twos_threes',     'Older Twos / Threes (2½ – under 3)',           248.50, 30,  36,  null, null, ''],
      ['prek_preschool_part',   'Pre-K / Preschool (3–5) — Part-Time',          176.00, 36,  60,  0, 30, '<30 hrs/week'],
      ['prek_preschool_full',   'Pre-K / Preschool (3–5) — Full-Time',          233.00, 36,  60,  30, null, '≥30 hrs/week'],
      ['gsrp_wrap_mr',          'GSRP Wrap-Around Care M–R',                     62.00, null,null,null,null, '4-day wrap'],
      ['gsrp_wrap_mf',          'GSRP Wrap-Around Care M–F',                     93.00, null,null,null,null, '5-day wrap'],
      ['schoolage_ba',          'School-Age Before/After School (K–6)',          88.00, 60,  144, null,null, 'K–6th grade'],
      ['schoolage_nonschool_part','School-Age Non-School Week <30 hrs',         160.50, 60,  144, 0, 30,   'Non-school week part-time'],
      ['schoolage_nonschool_full','School-Age Non-School Week ≥30 hrs',         197.00, 60,  144, 30,null, 'Non-school week full-time'],
      ['summer_camp',           'Summer Camp (includes activities)',            217.50, 60,  144, null,null, 'Summer only'],
    ],
    'Peace': [
      ['infant_toddler',        'Infants & Toddlers (Under 2½) — Full-Time',    419.25, 0,   30,  null, null, 'Full time only'],
      ['older_twos',            'Older Twos (2½ – under 3)',                    305.25, 30,  36,  null, null, ''],
      ['prek_preschool_part',   'Pre-K / Preschool (2½–5) — Part-Time',         176.00, 30,  60,  0, 30, '<30 hrs/week'],
      ['prek_preschool_full',   'Pre-K / Preschool (2½–5) — Full-Time',         233.00, 30,  60,  30, null, '≥30 hrs/week'],
      ['gsrp_wrap_mr',          'GSRP Wrap-Around Care M–R',                     62.00, null,null,null,null, '4-day wrap'],
      ['gsrp_wrap_mf',          'GSRP Wrap-Around Care M–F',                     98.50, null,null,null,null, '5-day wrap'],
      ['schoolage_ba',          'School-Age Before/After School (K–6)',          88.00, 60,  144, null,null, 'K–6th grade'],
      ['schoolage_nonschool_part','School-Age Non-School Week <30 hrs',         170.75, 60,  144, 0, 30,   'Non-school week part-time'],
      ['schoolage_nonschool_full','School-Age Non-School Week ≥30 hrs',         202.00, 60,  144, 30,null, 'Non-school week full-time'],
      ['summer_camp',           'Summer Camp (includes activities)',            233.00, 60,  144, null,null, 'Summer only'],
    ],
    'Montessori': [
      ['under25_fullday',       'Under 2½ — Full Day (8am–5pm)',                400.00, 0,   30,  null, null, 'Annual $20,060 / Monthly $1,672'],
      ['under25_extended',      'Under 2½ — Extended Day (7am–6pm)',            419.00, 0,   30,  null, null, 'Annual $21,535 / Monthly $1,794'],
      ['preprimary_schoolday',  'Pre-Primary/Primary (2½–6) — School Day (8–3)', 280.00, 30,  72,  null, null, 'Annual $13,520 / Monthly $1,126.67'],
      ['preprimary_extended',   'Pre-Primary/Primary (2½–6) — Extended (7–6)',   385.00, 30,  72,  null, null, 'Annual $19,500 / Monthly $1,625'],
      ['summer_camp',           'School-Age Summer Camp 1st–6th',               233.00, 72,  144, null, null, 'Summer only'],
      ['wrap_around',           'Wrap-Around (GSRP & Elementary) 7–8am & 3–6pm', 90.00, null,null,null, null, 'Monthly $380'],
    ],
  };
  for (const [center, rows] of Object.entries(RATES_2025)) {
    for (const r of rows) {
      await pool.query(
        `INSERT INTO tuition_rates
           (center, program_key, description, weekly_rate, age_min_months, age_max_months, hours_min, hours_max, effective_date, notes)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'2025-01-01',$9)
         ON CONFLICT (center, program_key, effective_date) DO UPDATE SET
           description=EXCLUDED.description,
           weekly_rate=EXCLUDED.weekly_rate,
           age_min_months=EXCLUDED.age_min_months,
           age_max_months=EXCLUDED.age_max_months,
           hours_min=EXCLUDED.hours_min,
           hours_max=EXCLUDED.hours_max,
           notes=EXCLUDED.notes,
           active=true`,
        [center, ...r]
      );
    }
  }

  // Backfill starting_balance transactions for any pre-existing collections_families
  // that were imported before the transaction ledger was introduced. Without this,
  // their remaining_balance computes to $0 and link generation fails.
  // Idempotent: only inserts where no transaction exists for that family yet.
  const backfill = await pool.query(`
    INSERT INTO collections_transactions
      (family_id, txn_date, txn_type, amount, description, created_by)
    SELECT
      f.id,
      COALESCE(f.created_at::date, CURRENT_DATE),
      'starting_balance',
      -ABS(f.original_balance::numeric),
      'Starting balance (backfilled)',
      'migration_backfill'
    FROM collections_families f
    WHERE f.original_balance IS NOT NULL
      AND f.original_balance > 0
      AND NOT EXISTS (
        SELECT 1 FROM collections_transactions t WHERE t.family_id = f.id
      )
    RETURNING family_id
  `);
  if (backfill.rowCount > 0) {
    console.log(`Backfilled starting_balance transactions for ${backfill.rowCount} families`);
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

// ═════════════════════════════════════════════════════════════════════════════
// ── CDC Statement PDF Upload (auto-detect CDC status) ───────────────────────
// ═════════════════════════════════════════════════════════════════════════════
// Accepts a DHS-1381 Statement of Payments PDF, shells out to parse_cdc.py,
// and auto-applies CDC status to children based on whether they got paid.
//
// Rules:
//   • paid > 0       → set is_cdc=true, subsidy_type='cdc'/'cdc_gsrp', fill case_no,
//                       child_id_number, authorized_hours (overwrite existing values)
//   • no_auth or $0  → flag for review, leave is_cdc alone
//   • not in roster + paid → auto-create as CDC child
//   • not in roster + no_auth → auto-create as 'unknown', flag
//   • CDC child on roster but NOT on this statement → flag MISSING_FROM_STATEMENT

function runCDCStatementParser(pdfBuffer) {
  return new Promise((resolve, reject) => {
    // Write PDF to a temp file so Python can read it
    const tmpPath = path.join(os.tmpdir(), `cdc_${Date.now()}_${Math.random().toString(36).slice(2)}.pdf`);
    try {
      fs.writeFileSync(tmpPath, pdfBuffer);
    } catch (e) {
      return reject(new Error(`Could not write temp PDF: ${e.message}`));
    }

    const script = path.join(__dirname, 'parse_cdc.py');
    execFile('python3', [script, tmpPath], { maxBuffer: 20 * 1024 * 1024, timeout: 60000 }, (err, stdout, stderr) => {
      try { fs.unlinkSync(tmpPath); } catch {}
      if (err) {
        return reject(new Error(`Parser failed: ${stderr || err.message}`));
      }
      try {
        resolve(JSON.parse(stdout));
      } catch (e) {
        reject(new Error(`Parser returned invalid JSON: ${e.message}\nOutput: ${stdout.slice(0, 300)}`));
      }
    });
  });
}

// Normalize a CDC-statement name ("LAST, FIRST MIDDLE" or "LAST FIRST") to
// {firstName, lastName} so we can match against the roster.
function splitStatementName(raw) {
  if (!raw) return null;
  let s = String(raw).trim();
  // Most DHS PDFs use "LASTNAME, FIRSTNAME [MIDDLE]"
  if (s.includes(',')) {
    const [last, rest] = s.split(',').map(x => x.trim());
    const firstParts = rest.split(/\s+/);
    return { firstName: firstParts[0] || '', lastName: last };
  }
  // Fallback: "FIRST LAST"
  const parts = s.split(/\s+/);
  if (parts.length < 2) return { firstName: parts[0] || '', lastName: '' };
  return { firstName: parts[0], lastName: parts.slice(1).join(' ') };
}

app.post('/api/upload/cdc-statement-pdf', ssoAuth, upload.single('file'), async (req, res) => {
  const startTime = Date.now();
  const { center, period_number } = req.body;
  if (!req.file || !center || !period_number) {
    return res.status(400).json({ error: 'Missing center, period_number, or PDF file' });
  }
  if (!/\.pdf$/i.test(req.file.originalname || '') && req.file.mimetype !== 'application/pdf') {
    return res.status(400).json({ error: 'File must be a PDF' });
  }

  let parsed;
  try {
    parsed = await runCDCStatementParser(req.file.buffer);
  } catch (e) {
    console.error('[cdc-statement-pdf] parser error:', e);
    return res.status(500).json({ error: e.message });
  }

  if (parsed.errors?.length) {
    // Non-fatal — parser may still have extracted children even with warnings
    console.warn('[cdc-statement-pdf] parser warnings:', parsed.errors);
  }

  const results = {
    parsed_children: parsed.children?.length || 0,
    paid_matched: 0,       // in roster AND paid — is_cdc set to true
    paid_created: 0,       // NOT in roster, paid — auto-created as CDC
    no_auth_flagged: 0,    // on statement but no_auth or $0
    missing_from_statement: 0, // CDC in roster but not on this statement
    details: [],           // per-child outcome for the UI
    flags: [],
    statement_info: {
      voucher: parsed.voucher || '',
      statement_date: parsed.statement_date || '',
      pay_period: parsed.pay_period || '',
      total_pay: parsed.total_pay || 0,
      net_total_pay: parsed.net_total_pay || 0,
      detected_center: parsed.center || '',
    },
  };

  // Track which children we saw on the statement, for the missing-from-statement check
  const seenChildIds = new Set();

  for (const entry of parsed.children || []) {
    const name = splitStatementName(entry.name);
    if (!name || !name.lastName) {
      results.details.push({
        name: entry.name, status: 'UNPARSEABLE_NAME', amount: entry.amount_paid,
      });
      continue;
    }

    // Find by name first
    let childRow = await pool.query(
      `SELECT id, is_cdc, is_gsrp, subsidy_type, child_id_number, case_number
       FROM children
       WHERE center=$1 AND LOWER(first_name)=LOWER($2) AND LOWER(last_name)=LOWER($3)`,
      [center, name.firstName, name.lastName]
    );

    // If no name match but we have a Child ID Number, try matching by that
    if (!childRow.rows[0] && entry.child_id) {
      childRow = await pool.query(
        `SELECT id, is_cdc, is_gsrp, subsidy_type, child_id_number, case_number
         FROM children WHERE center=$1 AND child_id_number=$2`,
        [center, entry.child_id]
      );
    }

    let childId = childRow.rows[0]?.id || null;
    const existing = childRow.rows[0];
    const isPaid = entry.amount_paid > 0 && !entry.is_no_auth;
    const fullName = `${name.firstName} ${name.lastName}`.trim();

    // ── Not in roster ────────────────────────────────────────────────────────
    if (!childId) {
      if (isPaid) {
        // Auto-create as CDC child
        const ins = await pool.query(
          `INSERT INTO children (center, first_name, last_name, subsidy_type, is_cdc, is_active,
             child_id_number, case_number, authorized_hours, notes)
           VALUES ($1,$2,$3,'cdc',true,true,$4,$5,$6,'Auto-created from CDC statement PDF')
           ON CONFLICT (center, first_name, last_name) DO UPDATE
             SET is_cdc=true, subsidy_type=EXCLUDED.subsidy_type,
                 child_id_number=EXCLUDED.child_id_number,
                 case_number=EXCLUDED.case_number,
                 authorized_hours=EXCLUDED.authorized_hours,
                 updated_at=NOW()
           RETURNING id`,
          [center, name.firstName, name.lastName,
           entry.child_id || null, entry.case_no || null, entry.hours_auth || null]
        );
        childId = ins.rows[0].id;
        results.paid_created++;
        results.details.push({
          name: fullName, status: 'CREATED_AND_SET_CDC', amount: entry.amount_paid,
          child_id: entry.child_id, case_no: entry.case_no,
        });
      } else {
        // On statement but not paid — create as 'unknown' so the name lookup works next time
        const ins = await pool.query(
          `INSERT INTO children (center, first_name, last_name, subsidy_type, notes)
           VALUES ($1,$2,$3,'unknown','On CDC statement (no auth / $0) — please review')
           ON CONFLICT (center, first_name, last_name) DO UPDATE SET notes=EXCLUDED.notes RETURNING id`,
          [center, name.firstName, name.lastName]
        );
        childId = ins.rows[0].id;
        const flagType = entry.is_no_auth ? 'NO_AUTHORIZATION' : 'NOT_FOUND_IN_DHS';
        await createFlag(center, childId, fullName, flagType,
          `${entry.is_no_auth ? 'No authorization' : entry.error || '$0 paid'} on CDC statement for period ${period_number}`,
          period_number);
        results.no_auth_flagged++;
        results.flags.push({ childName: fullName, type: flagType });
        results.details.push({
          name: fullName, status: 'FLAGGED_NO_AUTH', amount: entry.amount_paid, reason: entry.error || 'no auth / $0',
        });
      }
    }
    // ── Already in roster ───────────────────────────────────────────────────
    else if (isPaid) {
      // Paid → set is_cdc=true, overwrite statement fields
      const newSubsidy = existing.is_gsrp ? 'cdc_gsrp' : 'cdc';
      await pool.query(
        `UPDATE children SET
           is_cdc=true,
           subsidy_type=$1,
           child_id_number=$2,
           case_number=$3,
           authorized_hours=$4,
           updated_at=NOW()
         WHERE id=$5`,
        [newSubsidy, entry.child_id || null, entry.case_no || null, entry.hours_auth || null, childId]
      );
      results.paid_matched++;
      results.details.push({
        name: fullName, status: 'MATCHED_AND_SET_CDC', amount: entry.amount_paid,
        previously_cdc: existing.is_cdc, child_id: entry.child_id, case_no: entry.case_no,
      });
    }
    // In roster + not paid → flag, don't change is_cdc
    else {
      const flagType = entry.is_no_auth ? 'NO_AUTHORIZATION' : 'NOT_FOUND_IN_DHS';
      await createFlag(center, childId, fullName, flagType,
        `${entry.is_no_auth ? 'No authorization' : entry.error || '$0 paid'} on CDC statement for period ${period_number}`,
        period_number);
      results.no_auth_flagged++;
      results.flags.push({ childName: fullName, type: flagType });
      results.details.push({
        name: fullName, status: 'FLAGGED_NO_AUTH', amount: entry.amount_paid, reason: entry.error || 'no auth / $0',
      });
    }

    // Record the cdc_periods entry for reconciliation page
    if (childId) {
      seenChildIds.add(childId);
      await pool.query(
        `INSERT INTO cdc_periods (period_number, center, child_id, child_name, billed, amount_paid, status, notes)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
         ON CONFLICT DO NOTHING`,
        [period_number, center, childId, fullName, true, entry.amount_paid || null,
         entry.is_no_auth ? 'no_auth' : isPaid ? 'paid' : 'unknown',
         entry.error || null]
      );
    }
  }

  // ── Children marked CDC in roster but NOT on this statement ──────────────
  const cdcKids = await pool.query(
    `SELECT id, first_name, last_name FROM children
     WHERE center=$1 AND is_cdc=true AND is_active=true`,
    [center]
  );
  for (const c of cdcKids.rows) {
    if (seenChildIds.has(c.id)) continue;
    const fullName = `${c.first_name} ${c.last_name}`;
    await createFlag(center, c.id, fullName, 'MISSING_FROM_STATEMENT',
      `CDC child not on statement for period ${period_number} — family should be billed directly`,
      period_number);
    results.missing_from_statement++;
    results.flags.push({ childName: fullName, type: 'MISSING_FROM_STATEMENT' });
    results.details.push({ name: fullName, status: 'MISSING_FROM_STATEMENT' });
  }

  const ms = Date.now() - startTime;
  console.log(`[cdc-statement-pdf] ${center} period ${period_number}: parsed ${results.parsed_children}, ` +
              `matched ${results.paid_matched}, created ${results.paid_created}, ` +
              `flagged ${results.no_auth_flagged}, missing ${results.missing_from_statement} (${ms}ms)`);

  res.json({ ...results, elapsed_ms: ms });
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
    const clean = String(str).trim().toUpperCase();
    const match = clean.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)?/);
    if (!match) return null;
    let h = parseInt(match[1]);
    const m = parseInt(match[2]);
    const suf = (match[3] || '').toUpperCase();
    // Reject impossible values outright
    if (isNaN(h) || isNaN(m) || h < 0 || h > 23 || m < 0 || m > 59) return null;
    if (suf === 'PM' && h !== 12) h += 12;
    if (suf === 'AM' && h === 12) h = 0;
    if (h > 23) return null;
    return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}`;
  } catch { return null; }
}

// ═════════════════════════════════════════════════════════════════════════════
// ── Tuition Rates ────────────────────────────────────────────────────────────
// ═════════════════════════════════════════════════════════════════════════════
app.get('/api/tuition-rates', ssoAuth, async (req, res) => {
  const { center } = req.query;
  const params = [];
  let q = `SELECT * FROM tuition_rates WHERE active=true`;
  if (center) { params.push(center); q += ` AND center=$${params.length}`; }
  q += ` ORDER BY center, weekly_rate DESC`;
  const { rows } = await pool.query(q, params);
  res.json(rows);
});

app.put('/api/tuition-rates/:id', ssoAuth, async (req, res) => {
  const { weekly_rate, description, notes, active } = req.body;
  const { rows } = await pool.query(
    `UPDATE tuition_rates SET
       weekly_rate = COALESCE($1::numeric, weekly_rate),
       description = COALESCE($2, description),
       notes = COALESCE($3, notes),
       active = COALESCE($4::boolean, active)
     WHERE id=$5 RETURNING *`,
    [weekly_rate, description, notes, active, req.params.id]
  );
  if (!rows[0]) return res.status(404).json({ error: 'Rate not found' });
  res.json(rows[0]);
});

// ═════════════════════════════════════════════════════════════════════════════
// ── Guardians Import + Auto-Link ─────────────────────────────────────────────
// ═════════════════════════════════════════════════════════════════════════════
// Accepts any of three Playground guardian CSV formats:
//   1. MCC "Guardian Contacts" export — columns: Last name, First name (child),
//      Relationship, Last Name, First Name (guardian), Phone, Email
//   2. Peace/St. Joseph export — columns: First Name, Last Name (guardian),
//      Email, Relationship, Students, Account Number, Cell Phone, ..., Role
//   3. MCC "Check-in Authorization" export — columns: Signed up, Name, Students,
//      Relationship, Permission Level, Email, Phone, Family #
//
// For each row:
//   • Parse child name(s) from the appropriate column
//   • Parse guardian name/email/phone
//   • Match to an existing child by (center, first_name, last_name), case-insensitive
//   • Insert/update into `guardians`
//   • For the primary guardian (first "Parent"/"Primary Guardian" role per child),
//     populate `children.parent_name/parent_email/parent_phone`
//   • After import, auto-link past-due families where their family_name matches
//     the primary guardian's full name

function detectGuardianCSVFormat(headers) {
  const h = headers.map(x => x.trim());
  // Format 1: MCC Guardian Contacts — has both capitalized and lowercase 'Last name'
  if (h.includes('Last name') && h.includes('Last Name') && h.includes('Relationship') && h.includes('Email')) {
    return 'mcc_guardian';
  }
  // Format 2: Peace — has Role column and Cell Phone
  if (h.includes('Role') && h.includes('Cell Phone') && h.includes('Students')) {
    return 'peace';
  }
  // Format 3: MCC Friday check-in auth — has Permission Level and Family #
  if (h.includes('Permission Level') && h.includes('Family #')) {
    return 'mcc_checkin';
  }
  return null;
}

function normalizeName(s) {
  return String(s || '').trim().toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '') // strip accents
    .replace(/['\-]/g, '')                              // strip apostrophes and hyphens
    .replace(/\s+/g, ' ');
}

function isPrimaryRelationship(relationship, role) {
  const rel = String(relationship || '').toLowerCase();
  const rl = String(role || '').toLowerCase();
  if (rl.includes('primary guardian')) return true;
  if (rel === 'parent' || rel === 'mother' || rel === 'father') return true;
  return false;
}

// Extract child records from a single CSV row (different formats name children differently)
function extractChildrenFromRow(row, format) {
  if (format === 'mcc_guardian') {
    const last = (row['Last name'] || '').trim();
    const first = (row['First name'] || '').trim();
    if (!last) return [];
    return [{ first, last }];
  }
  if (format === 'peace') {
    // "Ashton Morgan and Amiri Williams" → [{first:"Ashton",last:"Morgan"},{first:"Amiri",last:"Williams"}]
    const students = (row['Students'] || '').trim();
    if (!students) return [];
    return students.split(/\s+and\s+/i).map(s => {
      const parts = s.trim().split(/\s+/);
      if (parts.length < 2) return { first: parts[0] || '', last: '' };
      return { first: parts[0], last: parts.slice(1).join(' ') };
    }).filter(c => c.last);
  }
  if (format === 'mcc_checkin') {
    // "Students" is typically just last name, and "Name" is guardian full name
    const last = (row['Students'] || '').trim();
    if (!last) return [];
    return [{ first: '', last }];
  }
  return [];
}

function extractGuardianFromRow(row, format) {
  if (format === 'mcc_guardian') {
    return {
      first: (row['First Name'] || '').trim(),
      last: (row['Last Name'] || '').trim(),
      relationship: (row['Relationship'] || '').trim(),
      role: '',
      email: (row['Email'] || '').trim(),
      phone: (row['Phone'] || '').trim(),
      accountNumber: '',
    };
  }
  if (format === 'peace') {
    const phone = (row['Cell Phone'] || row['Work Phone'] || row['Other Phone'] || '').trim();
    return {
      first: (row['First Name'] || '').trim(),
      last: (row['Last Name'] || '').trim(),
      relationship: (row['Relationship'] || '').trim(),
      role: (row['Role'] || '').trim(),
      email: (row['Email'] || '').trim(),
      phone,
      accountNumber: (row['Account Number'] || '').trim(),
    };
  }
  if (format === 'mcc_checkin') {
    // "Name" is typically "firstname " with trailing space
    const fullName = (row['Name'] || '').trim();
    const parts = fullName.split(/\s+/);
    return {
      first: parts[0] || '',
      last: parts.slice(1).join(' ') || '',
      relationship: (row['Relationship'] || '').trim(),
      role: (row['Permission Level'] || '').trim(),
      email: (row['Email'] || '').trim(),
      phone: (row['Phone'] || '').trim(),
      accountNumber: (row['Family #'] || '').trim(),
    };
  }
  return null;
}

app.post('/api/upload/guardians', ssoAuth, upload.single('file'), async (req, res) => {
  const startTime = Date.now();
  const { center } = req.body;
  if (!req.file || !center) return res.status(400).json({ error: 'Missing center or file' });

  let text = req.file.buffer.toString();
  if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1); // strip BOM
  let records;
  try {
    records = parse(text, { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) { return res.status(400).json({ error: 'CSV parse error: ' + e.message }); }
  if (!records.length) return res.status(400).json({ error: 'CSV is empty' });

  const headers = Object.keys(records[0]);
  const format = detectGuardianCSVFormat(headers);
  if (!format) {
    return res.status(400).json({
      error: 'Unknown CSV format. Expected Playground Guardian Contacts, Peace Guardian, or Check-in Authorization export.',
      headers_seen: headers,
    });
  }

  // Preload all active children at this center for matching
  const { rows: allKids } = await pool.query(
    `SELECT id, first_name, last_name FROM children WHERE center=$1 AND is_active=true`,
    [center]
  );
  const kidIndex = new Map();
  for (const k of allKids) {
    // Index by last-name-only AND by last+first — covers cases where CSV has no first name
    const fullKey = `${normalizeName(k.first_name)}|${normalizeName(k.last_name)}`;
    const lastKey = `${normalizeName(k.last_name)}`;
    kidIndex.set(fullKey, k);
    // For last-only matching, only index if unique; else leave null to avoid false matches
    if (!kidIndex.has(lastKey)) kidIndex.set(lastKey, k);
    else kidIndex.set(lastKey, null); // ambiguous
  }

  let rowsProcessed = 0, guardiansInserted = 0, childrenUpdated = 0, unmatched = 0;
  const unmatchedNames = [];
  const primaryByChild = new Map(); // child_id → first primary guardian info

  for (const row of records) {
    rowsProcessed++;
    const kids = extractChildrenFromRow(row, format);
    const guardian = extractGuardianFromRow(row, format);
    if (!guardian || (!guardian.first && !guardian.last)) continue;

    for (const kid of kids) {
      // Find child: prefer full match, fall back to last-only if unambiguous
      const fullKey = `${normalizeName(kid.first)}|${normalizeName(kid.last)}`;
      const lastKey = `${normalizeName(kid.last)}`;
      let childRow = kidIndex.get(fullKey);
      if (!childRow && kid.first === '') childRow = kidIndex.get(lastKey);

      if (!childRow) {
        unmatched++;
        if (unmatchedNames.length < 20) unmatchedNames.push(`${kid.first} ${kid.last}`.trim());
        continue;
      }

      const isPrimary = isPrimaryRelationship(guardian.relationship, guardian.role);
      try {
        await pool.query(
          `INSERT INTO guardians
             (center, child_id, child_first, child_last, guardian_first, guardian_last,
              relationship, email, phone, role, is_primary, source_account_number)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
           ON CONFLICT (center, child_id, guardian_first, guardian_last, relationship) DO UPDATE SET
             email = CASE WHEN EXCLUDED.email <> '' THEN EXCLUDED.email ELSE guardians.email END,
             phone = CASE WHEN EXCLUDED.phone <> '' THEN EXCLUDED.phone ELSE guardians.phone END,
             role = EXCLUDED.role,
             is_primary = EXCLUDED.is_primary OR guardians.is_primary,
             source_account_number = COALESCE(NULLIF(EXCLUDED.source_account_number, ''), guardians.source_account_number)`,
          [center, childRow.id, childRow.first_name, childRow.last_name,
           guardian.first, guardian.last, guardian.relationship,
           guardian.email, guardian.phone, guardian.role, isPrimary, guardian.accountNumber]
        );
        guardiansInserted++;

        // Remember the first primary guardian for each child to backfill children.parent_*
        if (isPrimary && !primaryByChild.has(childRow.id)) {
          primaryByChild.set(childRow.id, { guardian, childRow });
        }
      } catch (e) {
        console.error('Guardian insert error:', e.message);
      }
    }
  }

  // Backfill parent_name / parent_email / parent_phone on children
  for (const [childId, { guardian }] of primaryByChild) {
    const fullName = `${guardian.first} ${guardian.last}`.trim();
    await pool.query(
      `UPDATE children SET
         parent_name = COALESCE(NULLIF($1,''), parent_name),
         parent_email = COALESCE(NULLIF($2,''), parent_email),
         parent_phone = COALESCE(NULLIF($3,''), parent_phone)
       WHERE id=$4`,
      [fullName, guardian.email, guardian.phone, childId]
    );
    childrenUpdated++;
  }

  // ── Auto-link past-due families ────────────────────────────────────────────
  // For every active past-due family at this center with no children linked yet,
  // find roster children whose primary guardian's full name matches the
  // family_name. Link them silently (per user preference).
  let autoLinked = 0;
  const { rows: activeFams } = await pool.query(
    `SELECT id, family_name FROM collections_families
     WHERE status='active' AND (center=$1 OR center IS NULL)`,
    [center]
  );
  for (const fam of activeFams) {
    // Skip families that already have linked children
    const { rows: existing } = await pool.query(
      `SELECT 1 FROM collections_family_children WHERE family_id=$1 LIMIT 1`, [fam.id]);
    if (existing.length) continue;

    // Find children whose parent_name matches the family name (normalized)
    const famNorm = normalizeName(fam.family_name);
    const { rows: candidates } = await pool.query(
      `SELECT id, first_name, last_name, parent_name FROM children
       WHERE center=$1 AND parent_name IS NOT NULL`,
      [center]
    );
    const matches = candidates.filter(c => normalizeName(c.parent_name) === famNorm);
    for (const m of matches) {
      await pool.query(
        `INSERT INTO collections_family_children (family_id, child_id)
         VALUES ($1, $2) ON CONFLICT DO NOTHING`,
        [fam.id, m.id]
      );
      autoLinked++;
    }
  }

  const elapsedMs = Date.now() - startTime;
  console.log(`[guardians upload] ${center} format=${format}: ${guardiansInserted} guardians, ${childrenUpdated} kids updated, ${autoLinked} auto-links (${elapsedMs}ms)`);

  res.json({
    format_detected: format,
    rows_processed: rowsProcessed,
    guardians_inserted: guardiansInserted,
    children_updated: childrenUpdated,
    auto_linked_families: autoLinked,
    unmatched_children_count: unmatched,
    unmatched_sample: unmatchedNames,
    elapsed_ms: elapsedMs,
  });
});

// List guardians for a child
app.get('/api/children/:id/guardians', ssoAuth, async (req, res) => {
  const { rows } = await pool.query(
    `SELECT * FROM guardians WHERE child_id=$1 ORDER BY is_primary DESC, guardian_last, guardian_first`,
    [req.params.id]
  );
  res.json(rows);
});

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

function parseTimeToMinutes(t) {
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
        const aIn = typeof a.checkin_time === 'string' ? parseTimeToMinutes(a.checkin_time) : timeStrToMin(a.checkin_time);
        const bIn = typeof b.checkin_time === 'string' ? parseTimeToMinutes(b.checkin_time) : timeStrToMin(b.checkin_time);
        return (aIn||0) - (bIn||0);
      });
      for (const b of sortedBlocks) {
        const inM  = typeof b.checkin_time  === 'string' ? parseTimeToMinutes(b.checkin_time)  : timeStrToMin(b.checkin_time);
        const outM = typeof b.checkout_time === 'string' ? parseTimeToMinutes(b.checkout_time) : timeStrToMin(b.checkout_time);
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
  // Handles both "8:14 AM" and Postgres TIME format "13:45:00"
  return parseTimeToMinutes(t);
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

    // ── Subscription events for the monthly Pay-What-You-Can plan ──
    case 'invoice.payment_succeeded': {
      // Fired every month when Stripe successfully charges the recurring price.
      // Find the family via the subscription metadata.
      const inv = obj;
      let famId = familyId;
      if (!famId && inv.subscription) {
        try {
          const sub = await stripe.subscriptions.retrieve(inv.subscription);
          if (sub.metadata?.family_id) famId = parseInt(sub.metadata.family_id);
        } catch { /* ignore */ }
      }
      if (!famId) break;

      const amount = (inv.amount_paid || 0) / 100;
      const piId = inv.payment_intent || `inv_${inv.id}`;

      // Record in collections_payments for traceability
      await pool.query(
        `INSERT INTO collections_payments
         (family_id, stripe_payment_intent_id, stripe_charge_id, stripe_customer_email,
          amount, currency, status, paid_at, link_type, raw_event)
         VALUES ($1,$2,$3,$4,$5,$6,'succeeded',$7,'pay_what_you_can_monthly',$8)
         ON CONFLICT (stripe_payment_intent_id) DO UPDATE
           SET status='succeeded', amount=EXCLUDED.amount, paid_at=EXCLUDED.paid_at`,
        [famId, piId, inv.charge || null, inv.customer_email || null,
         amount, inv.currency || 'usd', new Date(inv.created * 1000),
         JSON.stringify(event)]
      );

      // Auto-cancel the subscription if balance is now ≤ $0
      // (the ledger entry for this payment is added by the wrapped handleStripeEvent
      // hook later in the file, so balance will reflect this payment after that runs)
      // We schedule the check as a follow-up async task.
      setTimeout(async () => {
        try {
          const fam = await familyWithBalance(famId);
          if (fam && fam.remaining_balance <= 0.01 && inv.subscription) {
            await stripe.subscriptions.cancel(inv.subscription);
            await pool.query(
              `UPDATE collections_families SET status='settled', settled_at=NOW(), updated_at=NOW() WHERE id=$1`,
              [famId]
            );
            console.log(`[subscription] Auto-cancelled ${inv.subscription} for family ${famId} — balance settled`);
          }
        } catch (e) {
          console.error('[subscription] Auto-cancel check failed:', e.message);
        }
      }, 1500);
      break;
    }

    case 'invoice.payment_failed': {
      // Fired when the recurring monthly charge fails (e.g., declined card).
      const inv = obj;
      let famId = familyId;
      if (!famId && inv.subscription) {
        try {
          const sub = await stripe.subscriptions.retrieve(inv.subscription);
          if (sub.metadata?.family_id) famId = parseInt(sub.metadata.family_id);
        } catch { /* ignore */ }
      }
      if (!famId) break;

      const amount = (inv.amount_due || 0) / 100;
      const piId = inv.payment_intent || `inv_${inv.id}`;
      await pool.query(
        `INSERT INTO collections_payments
         (family_id, stripe_payment_intent_id, stripe_customer_email, amount, currency,
          status, failure_reason, link_type, raw_event)
         VALUES ($1,$2,$3,$4,$5,'failed',$6,'pay_what_you_can_monthly',$7)
         ON CONFLICT (stripe_payment_intent_id) DO UPDATE
           SET status='failed', failure_reason=EXCLUDED.failure_reason`,
        [famId, piId, inv.customer_email || null, amount, inv.currency || 'usd',
         'Monthly subscription charge failed (likely card declined or expired)',
         JSON.stringify(event)]
      );
      break;
    }

    case 'customer.subscription.deleted': {
      // Fired when the subscription is cancelled (manually, via auto-cancel, or by the family)
      const sub = obj;
      const famId = familyId || (sub.metadata?.family_id ? parseInt(sub.metadata.family_id) : null);
      if (!famId) break;
      await pool.query(
        `INSERT INTO collections_events (family_id, event_type, stripe_event_id, detail, raw_event)
         VALUES ($1, 'subscription_cancelled', $2, $3, $4)
         ON CONFLICT (stripe_event_id) DO NOTHING`,
        [famId, event.id, `Subscription ${sub.id} cancelled (status: ${sub.cancellation_details?.reason || 'unknown'})`,
         JSON.stringify(event)]
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

// Remaining balance = -SUM(signed transaction amounts).
// Starting balance is stored as a negative (money owed). Payments/credits are
// positive (reduce owed). So balance_owed = -sum(amount). If sum is zero or
// positive, the family has paid in full (or overpaid).
async function familyWithBalance(familyId) {
  const { rows: fRows } = await pool.query(`SELECT * FROM collections_families WHERE id=$1`, [familyId]);
  if (!fRows[0]) return null;
  const family = fRows[0];
  const { rows: tRows } = await pool.query(
    `SELECT COALESCE(SUM(amount), 0) AS net,
            COALESCE(SUM(amount) FILTER (WHERE txn_type IN ('payment','refund')), 0) AS payments_net
     FROM collections_transactions WHERE family_id=$1`,
    [familyId]
  );
  const netAmount = parseFloat(tRows[0].net) || 0;
  const paymentsNet = parseFloat(tRows[0].payments_net) || 0;
  const original = parseFloat(family.original_balance) || 0;
  const remaining = Math.max(0, -netAmount);
  return {
    ...family,
    total_paid: paymentsNet,                // sum of payments only (not credits/adjustments)
    remaining_balance: remaining,           // what they still owe
    discount_pay_in_full: remaining / 2,
  };
}

app.get('/api/collections/families', ssoAuth, async (req, res) => {
  const { status } = req.query;
  const params = [];
  let q = `SELECT f.*,
             COALESCE((SELECT SUM(amount) FROM collections_transactions
                       WHERE family_id=f.id), 0) AS txn_net,
             COALESCE((SELECT SUM(amount) FROM collections_transactions
                       WHERE family_id=f.id AND txn_type IN ('payment','refund')), 0) AS payments_net,
             COALESCE((SELECT COUNT(*) FROM collections_payments
                       WHERE family_id=f.id AND status='failed'), 0) AS failed_count
           FROM collections_families f WHERE 1=1`;
  if (status) { params.push(status); q += ` AND f.status=$${params.length}`; }
  q += ` ORDER BY f.status ASC, f.family_name ASC`;
  const { rows } = await pool.query(q, params);
  res.json(rows.map(r => {
    const net = parseFloat(r.txn_net) || 0;
    const remaining = Math.max(0, -net);
    return {
      ...r,
      total_paid: parseFloat(r.payments_net) || 0,
      remaining_balance: remaining,
      discount_pay_in_full: remaining / 2,
      failed_count: parseInt(r.failed_count),
    };
  }));
});

app.get('/api/collections/families/:id', ssoAuth, async (req, res) => {
  const family = await familyWithBalance(req.params.id);
  if (!family) return res.status(404).json({ error: 'Not found' });
  const { rows: payments } = await pool.query(
    `SELECT * FROM collections_payments WHERE family_id=$1 ORDER BY created_at DESC`, [req.params.id]);
  const { rows: events } = await pool.query(
    `SELECT * FROM collections_events WHERE family_id=$1 ORDER BY created_at DESC LIMIT 50`, [req.params.id]);
  const { rows: transactions } = await pool.query(
    `SELECT * FROM collections_transactions WHERE family_id=$1 ORDER BY txn_date DESC, created_at DESC`, [req.params.id]);
  const { rows: linkedChildren } = await pool.query(
    `SELECT c.id, c.first_name, c.last_name, c.center, c.parent_email, c.parent_phone, c.parent_name
     FROM collections_family_children fc
     JOIN children c ON c.id = fc.child_id
     WHERE fc.family_id = $1
     ORDER BY c.last_name, c.first_name`, [req.params.id]);

  // If the family record has no email/phone but a linked child does, surface those
  if (!family.primary_contact_email && linkedChildren.length) {
    family.primary_contact_email = linkedChildren.find(c => c.parent_email)?.parent_email || null;
  }
  if (!family.primary_contact_phone && linkedChildren.length) {
    family.primary_contact_phone = linkedChildren.find(c => c.parent_phone)?.parent_phone || null;
  }

  res.json({ family, payments, events, transactions, linked_children: linkedChildren });
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
  // Seed the ledger with a starting balance entry (negative = money owed)
  await pool.query(
    `INSERT INTO collections_transactions
       (family_id, txn_date, txn_type, amount, description, created_by)
     VALUES ($1, CURRENT_DATE, 'starting_balance', $2, 'Starting balance', $3)`,
    [rows[0].id, -Math.abs(parseFloat(original_balance)), req.user?.email || 'system']
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
      const { rows: insRows } = await pool.query(
        `INSERT INTO collections_families
         (family_name, primary_contact_email, primary_contact_phone, children_names,
          original_balance, center, left_date, notes)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
        [name,
         row['Email'] || row['email'] || null,
         row['Phone'] || row['phone'] || null,
         row['Children'] || row['children'] || row['Children Names'] || null,
         balance,
         row['Center'] || row['center'] || null,
         row['Left Date'] || row['left_date'] || null,
         row['Notes'] || row['notes'] || null]
      );
      // Seed ledger with the starting balance
      await pool.query(
        `INSERT INTO collections_transactions
           (family_id, txn_date, txn_type, amount, description, created_by)
         VALUES ($1, CURRENT_DATE, 'starting_balance', $2, 'Starting balance (imported)', $3)`,
        [insRows[0].id, -Math.abs(balance), req.user?.email || 'csv_import']
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

  // ─── Compute the suggested amounts ───────────────────────────────────────
  // Mary may override these via req.body { monthly_cents, weekly_cents }.
  //
  // Rule: Small balances (≤$600) show all three options. Big balances (>$600)
  // show pay-in-full + weekly only — because $25/month would take more than
  // 2 years on a >$600 balance, monthly stops being realistic. Weekly at $25
  // is always achievable, even on huge balances ($25/wk × 52 = $1,300/yr —
  // a $20K balance takes 16 years but it's real recovery vs. $0).
  // Pay-in-full is always offered.
  //
  // Threshold rationale: $25/month × 24 months = $600. If balance ≤ $600,
  // monthly at $25 minimum clears it within 2 years and is a fine option.
  const MIN_CENTS = COLLECTIONS_MIN_PAYMENT_CENTS;          // $25 minimum
  const SMALL_BALANCE_CUTOFF_CENTS = 60000;                 // $600
  const isSmallBalance = remainingCents <= SMALL_BALANCE_CUTOFF_CENTS;
  const discountCents = Math.round(remainingCents / 2);

  const suggestedMonthlyCents = Math.max(MIN_CENTS, Math.round(remainingCents / 24));
  const monthlyCents = req.body?.monthly_cents
    ? Math.max(MIN_CENTS, Math.round(parseFloat(req.body.monthly_cents)))
    : suggestedMonthlyCents;

  // 104 weeks = 2 years
  const suggestedWeeklyCents = Math.max(MIN_CENTS, Math.round(remainingCents / 104));
  const weeklyCents = req.body?.weekly_cents
    ? Math.max(MIN_CENTS, Math.round(parseFloat(req.body.weekly_cents)))
    : suggestedWeeklyCents;

  try {
    // ─── Pay in Full (50% off) — one-time payment ──────────────────────────
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

    // ─── Monthly subscription — only for SMALL balances (≤$600) ───────────
    // Big balances (>$600) skip monthly because $25/month would take more
    // than 2 years; weekly at $25 is more achievable.
    let monthlyLink = null;
    if (isSmallBalance) {
      const monthlyPrice = await stripe.prices.create({
        currency: 'usd',
        unit_amount: monthlyCents,
        recurring: { interval: 'month' },
        product_data: { name: `Monthly Payment Plan — ${family.family_name}` },
      });
      monthlyLink = await stripe.paymentLinks.create({
        line_items: [{ price: monthlyPrice.id, quantity: 1 }],
        metadata: { family_id: String(family.id), link_type: 'monthly' },
        subscription_data: {
          metadata: { family_id: String(family.id), link_type: 'monthly' },
          description: `Monthly payment toward balance owed to The Children's Center. We'll cancel automatically when your balance is paid off.`,
        },
        after_completion: { type: 'hosted_confirmation', hosted_confirmation: {
          custom_message: `Thank you! Your monthly payment of $${(monthlyCents/100).toFixed(2)} is set up. We'll charge your card automatically each month until your balance is paid off, then cancel the subscription. Contact us anytime to change the amount.`,
        }},
      });
    }

    // ─── Weekly subscription — always offered (recurring auto-charge) ───────
    // Even for huge balances, $25/week is achievable and produces real recovery.
    const weeklyPrice = await stripe.prices.create({
      currency: 'usd',
      unit_amount: weeklyCents,
      recurring: { interval: 'week' },
      product_data: { name: `Weekly Payment Plan — ${family.family_name}` },
    });
    const weeklyLink = await stripe.paymentLinks.create({
      line_items: [{ price: weeklyPrice.id, quantity: 1 }],
      metadata: { family_id: String(family.id), link_type: 'weekly' },
      subscription_data: {
        metadata: { family_id: String(family.id), link_type: 'weekly' },
        description: `Weekly payment toward balance owed to The Children's Center. We'll cancel automatically when your balance is paid off.`,
      },
      after_completion: { type: 'hosted_confirmation', hosted_confirmation: {
        custom_message: `Thank you! Your weekly payment of $${(weeklyCents/100).toFixed(2)} is set up. We'll charge your card automatically each week until your balance is paid off. Contact us anytime to change the amount.`,
      }},
    });

    const { rows } = await pool.query(
      `UPDATE collections_families SET
         payinfull_link_url=$1, payinfull_link_id=$2,
         paywhatyoucan_link_url=$3, paywhatyoucan_link_id=$4,
         weekly_link_url=$5, weekly_link_id=$6,
         updated_at=NOW()
       WHERE id=$7 RETURNING *`,
      [payInFullLink.url, payInFullLink.id,
       monthlyLink ? monthlyLink.url : null, monthlyLink ? monthlyLink.id : null,
       weeklyLink.url, weeklyLink.id,
       family.id]
    );

    res.json({
      pay_in_full: { url: payInFullLink.url, amount_cents: discountCents },
      monthly: monthlyLink ? {
        url: monthlyLink.url,
        amount_cents: monthlyCents,
        suggested_cents: suggestedMonthlyCents,
        was_overridden: monthlyCents !== suggestedMonthlyCents,
      } : null,
      monthly_offered: isSmallBalance,
      weekly: {
        url: weeklyLink.url,
        amount_cents: weeklyCents,
        suggested_cents: suggestedWeeklyCents,
        was_overridden: weeklyCents !== suggestedWeeklyCents,
      },
      family: rows[0],
    });
  } catch (e) {
    console.error('Stripe link creation failed:', e);
    const detail = {
      error: 'Stripe error: ' + e.message,
      stripe_code: e.code || null,
      stripe_type: e.type || null,
      stripe_param: e.param || null,
      stripe_doc_url: e.doc_url || null,
    };
    res.status(500).json(detail);
  }
});

app.post('/api/collections/families/:id/deactivate-links', ssoAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Stripe not configured' });
  const { rows } = await pool.query(
    `SELECT payinfull_link_id, paywhatyoucan_link_id, weekly_link_id
     FROM collections_families WHERE id=$1`, [req.params.id]);
  if (!rows[0]) return res.status(404).json({ error: 'Not found' });
  const deactivated = [];
  for (const linkId of [rows[0].payinfull_link_id, rows[0].paywhatyoucan_link_id, rows[0].weekly_link_id]) {
    if (!linkId) continue;
    try {
      await stripe.paymentLinks.update(linkId, { active: false });
      deactivated.push(linkId);
    } catch (e) { /* skip */ }
  }
  await pool.query(
    `UPDATE collections_families SET payinfull_link_url=NULL, payinfull_link_id=NULL,
       paywhatyoucan_link_url=NULL, paywhatyoucan_link_id=NULL,
       weekly_link_url=NULL, weekly_link_id=NULL, updated_at=NOW() WHERE id=$1`,
    [req.params.id]
  );
  res.json({ deactivated });
});

// ═════════════════════════════════════════════════════════════════════════════
// ── SendGrid: email payment links to families ────────────────────────────────
// ═════════════════════════════════════════════════════════════════════════════
// Branded HTML email templates with both payment options. The subject line and
// content are warm but firm — these go to families who left with a balance.
function buildPaymentLinkEmail({ family, payInFullUrl, payInFullAmount,
                                  monthlyUrl, monthlyAmount, weeklyUrl, weeklyAmount,
                                  fromName, unsubscribeUrl }) {
  // Use the primary guardian's first name if available, else fall back to family last name
  const greeting = (family.primary_guardian_first_name || family.family_name.split(/[\s,]+/)[0] || family.family_name).trim();
  const fmtMoney = (n) => {
    const v = parseFloat(n) || 0;
    return v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  const remaining = fmtMoney(family.remaining_balance || 0);
  const halfOff = fmtMoney(payInFullAmount || family.discount_pay_in_full || 0);
  const savings = fmtMoney((parseFloat(family.remaining_balance || 0)) - (parseFloat(payInFullAmount || family.discount_pay_in_full || 0)));
  const monthly = monthlyAmount ? fmtMoney(monthlyAmount) : null;
  const weekly = weeklyAmount ? fmtMoney(weeklyAmount) : null;

  const subject = `A Personal Note from Mary at The Children's Center`;

  // Build the option cards conditionally — monthly only shown if monthlyUrl is set
  const monthlyCardHTML = monthlyUrl ? `
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:14px;border:1px solid #d4d4d4;border-radius:8px;background:#ffffff">
          <tr><td style="padding:20px">
            <div style="font-size:11px;color:#1a2744;text-transform:uppercase;letter-spacing:1px;font-weight:500">A simple monthly option</div>
            <div style="font-family:'Georgia',serif;font-size:20px;color:#1a2744;margin-top:6px">Monthly payment plan</div>
            <p style="font-size:14px;line-height:1.5;margin:10px 0 16px;color:#555">
              Just <strong>$${monthly}/month</strong>, charged automatically. Cancels itself when your balance is paid off.
            </p>
            <a href="${monthlyUrl}" style="display:inline-block;background:#1a2744;color:#ffffff;font-weight:500;padding:10px 22px;border-radius:6px;text-decoration:none;font-size:14px">
              Set up $${monthly}/month →
            </a>
          </td></tr>
        </table>` : '';

  const weeklyCardHTML = weeklyUrl ? `
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border:1px solid #d4d4d4;border-radius:8px;background:#ffffff">
          <tr><td style="padding:20px">
            <div style="font-size:11px;color:#1a2744;text-transform:uppercase;letter-spacing:1px;font-weight:500">${monthlyUrl ? 'Or a smaller weekly option' : 'A simple weekly option'}</div>
            <div style="font-family:'Georgia',serif;font-size:20px;color:#1a2744;margin-top:6px">Weekly payment plan</div>
            <p style="font-size:14px;line-height:1.5;margin:10px 0 16px;color:#555">
              As low as <strong>$${weekly}/week</strong>, charged automatically. Cancels itself when your balance is paid off.
            </p>
            <a href="${weeklyUrl}" style="display:inline-block;background:#1a2744;color:#ffffff;font-weight:500;padding:10px 22px;border-radius:6px;text-decoration:none;font-size:14px">
              Set up $${weekly}/week →
            </a>
          </td></tr>
        </table>` : '';

  const html = `<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>${subject}</title></head>
<body style="margin:0;padding:0;font-family:Helvetica,Arial,sans-serif;background:#f7f5ef;color:#1a2744">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5ef;padding:24px 0">
  <tr><td align="center">
    <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05)">

      <tr><td style="background:#1a2744;padding:24px 32px">
        <div style="font-family:'Georgia',serif;font-size:22px;color:#ffffff">The Children's Center</div>
        <div style="color:rgba(255,255,255,.7);font-size:13px;margin-top:4px">A note from Mary Wardlaw</div>
      </td></tr>

      <tr><td style="padding:32px">
        <p style="font-size:15px;line-height:1.7;margin:0 0 16px">Dear ${greeting},</p>

        <p style="font-size:15px;line-height:1.7;margin:0 0 16px">I've been thinking about your family and wanted to reach out personally.</p>

        <p style="font-size:15px;line-height:1.7;margin:0 0 16px">It was truly an honor to care for your child, and we're grateful you were part of our community. Because of that, I wanted to extend a simple invitation — an opportunity to clear your account in a way that feels good, manageable, and complete.</p>

        <p style="font-size:15px;line-height:1.7;margin:0 0 16px">We understand that life gets busy and circumstances change. At the same time, we know our families value integrity and honoring commitments, and we want to support you in doing that in the easiest way possible.</p>

        <p style="font-size:15px;line-height:1.7;margin:0 0 20px">If clearing your balance has been on your mind, we've created a few flexible options:</p>

        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:14px;border:2px solid #c5a572;border-radius:8px;background:#fdfbf6">
          <tr><td style="padding:20px">
            <div style="font-size:11px;color:#c5a572;text-transform:uppercase;letter-spacing:1px;font-weight:500">A one-time opportunity</div>
            <div style="font-family:'Georgia',serif;font-size:20px;color:#1a2744;margin-top:6px">Settle at a reduced amount</div>
            <p style="font-size:14px;line-height:1.5;margin:10px 0 16px;color:#555">
              Pay <strong style="color:#c0392b">$${halfOff}</strong> now and we'll consider your account fully settled — a savings of <strong>$${savings}</strong>.
            </p>
            <a href="${payInFullUrl}" style="display:inline-block;background:#c5a572;color:#ffffff;font-weight:500;padding:10px 22px;border-radius:6px;text-decoration:none;font-size:14px">
              Pay $${halfOff} →
            </a>
          </td></tr>
        </table>

        ${monthlyCardHTML}
        ${weeklyCardHTML}

        <p style="font-size:15px;line-height:1.7;margin:24px 0 16px">Our goal is to make this feel doable, relieving, and even worth celebrating — because there's something powerful about closing the loop and moving forward with a clear mind.</p>

        <p style="font-size:15px;line-height:1.7;margin:0 0 24px">We believe in you, we value your family, and we're here to make this easy.</p>

        <p style="font-size:15px;line-height:1.7;margin:0 0 4px">With appreciation,</p>
        <p style="font-size:15px;line-height:1.7;margin:0;font-weight:500">Mary Wardlaw</p>
        <p style="font-size:13px;line-height:1.7;margin:0;color:#555">Owner | The Children's Center</p>

        <p style="font-size:12.5px;line-height:1.6;margin:28px 0 0;color:#888;font-style:italic">
          P.S. If these options don't quite fit, please reply to this email or call <strong>(269) 683-0405</strong> — we'll work something out together.
        </p>
      </td></tr>

      <tr><td style="background:#f7f5ef;padding:16px 32px;font-size:11px;color:#888;text-align:center;line-height:1.6">
        The Children's Center · 210 Main Street, Niles, MI 49120<br>
        219 Peace Boulevard, St. Joseph, MI 49085 · (269) 683-0405
        ${unsubscribeUrl ? `<br><br><a href="${unsubscribeUrl}" style="color:#888;text-decoration:underline">Unsubscribe from these emails</a>` : ''}
      </td></tr>

    </table>
  </td></tr>
</table>
</body></html>`;

  // Plain-text version mirrors the HTML structure
  let text = `Dear ${greeting},

I've been thinking about your family and wanted to reach out personally.

It was truly an honor to care for your child, and we're grateful you were part of our community. Because of that, I wanted to extend a simple invitation — an opportunity to clear your account in a way that feels good, manageable, and complete.

We understand that life gets busy and circumstances change. At the same time, we know our families value integrity and honoring commitments, and we want to support you in doing that in the easiest way possible.

If clearing your balance has been on your mind, we've created a few flexible options:

★ A ONE-TIME OPPORTUNITY — Settle at a reduced amount
Pay $${halfOff} now (savings of $${savings}) and we'll consider your account fully settled.
${payInFullUrl}
`;
  if (monthlyUrl) {
    text += `
A SIMPLE MONTHLY OPTION — $${monthly}/month
Charged automatically. Cancels itself when your balance is paid off.
${monthlyUrl}
`;
  }
  if (weeklyUrl) {
    text += `
${monthlyUrl ? 'OR A SMALLER WEEKLY OPTION' : 'A SIMPLE WEEKLY OPTION'} — $${weekly}/week
Charged automatically. Cancels itself when your balance is paid off.
${weeklyUrl}
`;
  }
  text += `
Our goal is to make this feel doable, relieving, and even worth celebrating — because there's something powerful about closing the loop and moving forward with a clear mind.

We believe in you, we value your family, and we're here to make this easy.

With appreciation,
Mary Wardlaw
Owner | The Children's Center

P.S. If these options don't quite fit, please reply to this email or call (269) 683-0405 — we'll work something out together.

The Children's Center
210 Main Street, Niles, MI 49120
219 Peace Boulevard, St. Joseph, MI 49085`;
  if (unsubscribeUrl) {
    text += `

To stop receiving these emails: ${unsubscribeUrl}`;
  }

  return { subject, html, text };
}

// Send the payment-link email to the family.
// Recipient resolution: explicit `to` in body, else family.primary_contact_email,
// else any linked child's parent_email. Returns 422 if no email is available.
app.post('/api/collections/families/:id/send-email', ssoAuth, async (req, res) => {
  if (!sendgrid) {
    return res.status(503).json({
      error: 'SendGrid not configured. Set SENDGRID_API_KEY in Render environment variables.',
    });
  }

  const family = await familyWithBalance(req.params.id);
  if (!family) return res.status(404).json({ error: 'Family not found' });

  // Need at least pay-in-full + weekly to send (monthly may be null for big balances)
  if (!family.payinfull_link_url || !family.weekly_link_url) {
    return res.status(400).json({
      error: 'Generate Stripe payment links first (click "Generate Payment Links" on this family\'s page).',
    });
  }

  // Resolve the email recipient AND the primary guardian's first name (for the greeting)
  let toEmail = (req.body.to || '').trim() || family.primary_contact_email;
  let primaryGuardianFirstName = null;
  if (!toEmail || true) {
    // Always look up linked children to get parent name; fall back for email if needed
    const { rows } = await pool.query(
      `SELECT c.parent_email, c.parent_name FROM collections_family_children fc
       JOIN children c ON c.id=fc.child_id
       WHERE fc.family_id=$1
       ORDER BY (c.parent_email IS NOT NULL AND c.parent_email <> '') DESC, c.id ASC
       LIMIT 1`, [req.params.id]
    );
    if (!toEmail) toEmail = rows[0]?.parent_email;
    if (rows[0]?.parent_name) {
      // Extract first name from "First Last" or "Last, First"
      const name = rows[0].parent_name.trim();
      if (name.includes(',')) {
        primaryGuardianFirstName = name.split(',')[1]?.trim().split(/\s+/)[0];
      } else {
        primaryGuardianFirstName = name.split(/\s+/)[0];
      }
    }
  }
  if (!toEmail) {
    return res.status(422).json({
      error: 'No email address on file. Add one to this family or link a child whose parent_email is set.',
    });
  }

  // Pass the primary guardian first name to the template (used for "Dear FirstName,")
  family.primary_guardian_first_name = primaryGuardianFirstName;

  const fromEmail = process.env.SENDGRID_FROM_EMAIL || 'billing@childrenscenterinc.com';
  const fromName = process.env.SENDGRID_FROM_NAME || "The Children's Center Billing";

  // Fetch the actual monthly/weekly amounts from Stripe (since they vary per family)
  let monthlyAmount = null, weeklyAmount = null;
  try {
    if (family.paywhatyoucan_link_id) {
      const link = await stripe.paymentLinks.retrieve(family.paywhatyoucan_link_id, { expand: ['line_items.data.price'] });
      const price = link.line_items?.data?.[0]?.price;
      if (price?.unit_amount) monthlyAmount = price.unit_amount / 100;
    }
    if (family.weekly_link_id) {
      const link = await stripe.paymentLinks.retrieve(family.weekly_link_id, { expand: ['line_items.data.price'] });
      const price = link.line_items?.data?.[0]?.price;
      if (price?.unit_amount) weeklyAmount = price.unit_amount / 100;
    }
  } catch (e) {
    console.warn('Could not fetch link prices:', e.message);
  }

  // Block sends if family has unsubscribed
  if (family.email_unsubscribed) {
    return res.status(403).json({
      error: `${family.family_name} has unsubscribed from automated emails. You can still email them manually outside this system.`,
    });
  }

  // Generate a one-time unsubscribe token if this family doesn't have one yet.
  // The token is used in the List-Unsubscribe header so a single click in the
  // recipient's email client (Gmail's "Unsubscribe" button) opts them out instantly.
  let unsubscribeToken = family.email_unsubscribe_token;
  if (!unsubscribeToken) {
    unsubscribeToken = require('crypto').randomBytes(24).toString('hex');
    await pool.query(
      `UPDATE collections_families SET email_unsubscribe_token=$1 WHERE id=$2`,
      [unsubscribeToken, family.id]
    );
  }
  const appUrl = process.env.APP_URL || 'https://tcc-collections-hub.onrender.com';
  const unsubscribeUrl = `${appUrl}/api/collections/unsubscribe?token=${unsubscribeToken}`;

  const { subject, html, text } = buildPaymentLinkEmail({
    family,
    payInFullUrl: family.payinfull_link_url,
    payInFullAmount: family.discount_pay_in_full,
    monthlyUrl: family.paywhatyoucan_link_url || null,
    monthlyAmount,
    weeklyUrl: family.weekly_link_url || null,
    weeklyAmount,
    fromName,
    unsubscribeUrl,
  });

  try {
    await sendgrid.send({
      to: toEmail,
      from: { email: fromEmail, name: fromName },
      subject, html, text,
      categories: ['collections', 'payment-link'],
      customArgs: { family_id: String(family.id) },
      // RFC 8058 one-click unsubscribe — tells Gmail/Outlook to show their built-in
      // Unsubscribe button. Massively boosts deliverability and reputation.
      headers: {
        'List-Unsubscribe': `<${unsubscribeUrl}>`,
        'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
      },
    });

    // Log the send in the events table
    await pool.query(
      `INSERT INTO collections_events (family_id, event_type, detail, created_at)
       VALUES ($1, 'email_sent', $2, NOW())`,
      [family.id, `Payment-link email sent to ${toEmail}`]
    );

    res.json({ sent: true, to: toEmail, from: fromEmail });
  } catch (e) {
    console.error('SendGrid send error:', e?.response?.body || e.message);
    const detail = e?.response?.body?.errors?.map(x => x.message).join('; ') || e.message;
    res.status(500).json({ error: `Email send failed: ${detail}` });
  }
});

// ── Unsubscribe endpoint (no auth required — accessed via email link) ────────
// Both GET and POST are supported per RFC 8058. Email clients use POST for
// one-click unsubscribe; humans hitting the URL use GET. Either way, the token
// is the proof of authority — only the family receiving the email knows it.
async function handleUnsubscribe(req, res) {
  const token = (req.query.token || req.body?.token || '').trim();
  if (!token) return res.status(400).send('Missing unsubscribe token.');

  const { rows } = await pool.query(
    `UPDATE collections_families SET
       email_unsubscribed = true,
       email_unsubscribed_at = NOW()
     WHERE email_unsubscribe_token = $1
     RETURNING id, family_name, email_unsubscribed_at`,
    [token]
  );

  if (!rows[0]) return res.status(404).send('Unsubscribe link not found or already used.');

  // Log the event
  await pool.query(
    `INSERT INTO collections_events (family_id, event_type, detail, created_at)
     VALUES ($1, 'email_unsubscribed', $2, NOW())`,
    [rows[0].id, `Unsubscribed via email link from ${req.ip || 'unknown'}`]
  );

  // Return a friendly confirmation page (HTML for human visitors)
  res.send(`<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Unsubscribed</title>
<style>
  body { font-family:Helvetica,Arial,sans-serif; background:#f7f5ef; color:#1a2744; margin:0; padding:48px 16px; }
  .card { max-width:480px; margin:0 auto; background:#fff; border-radius:8px; padding:32px; box-shadow:0 1px 3px rgba(0,0,0,.05); text-align:center; }
  .check { font-size:48px; color:#1e8449; }
  h1 { font-family:Georgia,serif; margin:8px 0; }
  p { line-height:1.6; color:#555; }
</style></head><body>
<div class="card">
  <div class="check">✓</div>
  <h1>You've been unsubscribed</h1>
  <p>Hi from The Children's Center — we've received your request and you won't receive automated emails from us anymore.</p>
  <p>If you change your mind or have questions, please call <strong>(269) 683-0405</strong>. Thank you.</p>
  <p style="margin-top:24px;font-size:12px;color:#888">Mary Wardlaw, Owner | The Children's Center</p>
</div>
</body></html>`);
}
app.get('/api/collections/unsubscribe', handleUnsubscribe);
app.post('/api/collections/unsubscribe', handleUnsubscribe);

// ═════════════════════════════════════════════════════════════════════════════
// ── 7-Touch Outreach Campaign Engine ─────────────────────────────────────────
// ═════════════════════════════════════════════════════════════════════════════
// Mary clicks "Start Sequence" on a family. We create an enrollment + 7 touches.
// A scheduler ticks every 15 minutes and fires due touches. The sequence stops
// automatically on payment, settlement, or unsubscribe. Mary can also pause/cancel.

// Cadence: standard = 0/5/12/21/30/38/50 days
// Touch types: 1,2,3,5,7 = email | 4,6 = sms
const CAMPAIGN_TOUCHES = [
  { number: 1, channel: 'email', day_offset: 0,  template: 'touch1_warm_letter' },
  { number: 2, channel: 'email', day_offset: 5,  template: 'touch2_check_in' },
  { number: 3, channel: 'email', day_offset: 12, template: 'touch3_small_steps' },
  { number: 4, channel: 'sms',   day_offset: 21, template: 'touch4_sms_brief' },
  { number: 5, channel: 'email', day_offset: 30, template: 'touch5_offer_reminder' },
  { number: 6, channel: 'sms',   day_offset: 38, template: 'touch6_sms_final' },
  { number: 7, channel: 'email', day_offset: 50, template: 'touch7_final_note' },
];

// ── Touch content templates ──────────────────────────────────────────────────
// Each function takes (family, urls, amounts) and returns { subject, html, text }
// for emails, or { text } for SMS.

function campaignEmailLayout({ greeting, headerSubtitle, body, optionsHTML, closingHTML, unsubscribeUrl }) {
  return `<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;font-family:Helvetica,Arial,sans-serif;background:#f7f5ef;color:#1a2744">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5ef;padding:24px 0">
  <tr><td align="center">
    <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;background:#ffffff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05)">
      <tr><td style="background:#1a2744;padding:24px 32px">
        <div style="font-family:'Georgia',serif;font-size:22px;color:#ffffff">The Children's Center</div>
        <div style="color:rgba(255,255,255,.7);font-size:13px;margin-top:4px">${headerSubtitle}</div>
      </td></tr>
      <tr><td style="padding:32px">
        <p style="font-size:15px;line-height:1.7;margin:0 0 16px">Dear ${greeting},</p>
        ${body}
        ${optionsHTML}
        ${closingHTML}
      </td></tr>
      <tr><td style="background:#f7f5ef;padding:16px 32px;font-size:11px;color:#888;text-align:center;line-height:1.6">
        The Children's Center · 210 Main Street, Niles, MI 49120<br>
        219 Peace Boulevard, St. Joseph, MI 49085 · (269) 683-0405
        ${unsubscribeUrl ? `<br><br><a href="${unsubscribeUrl}" style="color:#888;text-decoration:underline">Unsubscribe from these emails</a>` : ''}
      </td></tr>
    </table>
  </td></tr>
</table>
</body></html>`;
}

function buildOptionCards({ payInFullUrl, halfOff, savings, monthlyUrl, monthly, weeklyUrl, weekly, emphasis }) {
  // emphasis: 'pay_in_full' | 'weekly' | null — controls which card has visual emphasis
  const goldEmphasis = emphasis === 'pay_in_full';
  const weeklyEmphasis = emphasis === 'weekly';

  let html = `
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:14px;border:${goldEmphasis ? '3px' : '2px'} solid #c5a572;border-radius:8px;background:#fdfbf6">
          <tr><td style="padding:20px">
            <div style="font-size:11px;color:#c5a572;text-transform:uppercase;letter-spacing:1px;font-weight:500">${goldEmphasis ? '★ Limited time' : 'A one-time opportunity'}</div>
            <div style="font-family:'Georgia',serif;font-size:20px;color:#1a2744;margin-top:6px">Settle at a reduced amount</div>
            <p style="font-size:14px;line-height:1.5;margin:10px 0 16px;color:#555">
              Pay <strong style="color:#c0392b">$${halfOff}</strong> now and we'll consider your account fully settled — a savings of <strong>$${savings}</strong>.
            </p>
            <a href="${payInFullUrl}" style="display:inline-block;background:#c5a572;color:#ffffff;font-weight:500;padding:10px 22px;border-radius:6px;text-decoration:none;font-size:14px">Pay $${halfOff} →</a>
          </td></tr>
        </table>`;

  if (monthlyUrl) {
    html += `
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:14px;border:1px solid #d4d4d4;border-radius:8px;background:#ffffff">
          <tr><td style="padding:20px">
            <div style="font-size:11px;color:#1a2744;text-transform:uppercase;letter-spacing:1px;font-weight:500">A simple monthly option</div>
            <div style="font-family:'Georgia',serif;font-size:20px;color:#1a2744;margin-top:6px">Monthly payment plan</div>
            <p style="font-size:14px;line-height:1.5;margin:10px 0 16px;color:#555">Just <strong>$${monthly}/month</strong>, charged automatically. Cancels itself when your balance is paid off.</p>
            <a href="${monthlyUrl}" style="display:inline-block;background:#1a2744;color:#ffffff;font-weight:500;padding:10px 22px;border-radius:6px;text-decoration:none;font-size:14px">Set up $${monthly}/month →</a>
          </td></tr>
        </table>`;
  }
  if (weeklyUrl) {
    html += `
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;border:${weeklyEmphasis ? '3px solid #6b8e23' : '1px solid #d4d4d4'};border-radius:8px;background:${weeklyEmphasis ? '#f0f6e8' : '#ffffff'}">
          <tr><td style="padding:20px">
            <div style="font-size:11px;color:${weeklyEmphasis ? '#6b8e23' : '#1a2744'};text-transform:uppercase;letter-spacing:1px;font-weight:500">${weeklyEmphasis ? '★ Smaller, easier steps' : (monthlyUrl ? 'Or a smaller weekly option' : 'A simple weekly option')}</div>
            <div style="font-family:'Georgia',serif;font-size:20px;color:#1a2744;margin-top:6px">Weekly payment plan</div>
            <p style="font-size:14px;line-height:1.5;margin:10px 0 16px;color:#555">As low as <strong>$${weekly}/week</strong>, charged automatically. Cancels itself when your balance is paid off.</p>
            <a href="${weeklyUrl}" style="display:inline-block;background:${weeklyEmphasis ? '#6b8e23' : '#1a2744'};color:#ffffff;font-weight:500;padding:10px 22px;border-radius:6px;text-decoration:none;font-size:14px">Set up $${weekly}/week →</a>
          </td></tr>
        </table>`;
  }
  return html;
}

function buildOptionPlaintext({ halfOff, savings, monthlyUrl, monthly, weeklyUrl, weekly, payInFullUrl }) {
  let s = `\n★ A one-time opportunity — Settle at a reduced amount\nPay $${halfOff} now (savings of $${savings}).\n${payInFullUrl}\n`;
  if (monthlyUrl) s += `\nA simple monthly option — $${monthly}/month\nCharged automatically. Cancels itself when your balance is paid off.\n${monthlyUrl}\n`;
  if (weeklyUrl) s += `\n${monthlyUrl ? 'Or a smaller weekly option' : 'A simple weekly option'} — $${weekly}/week\nCharged automatically. Cancels itself when your balance is paid off.\n${weeklyUrl}\n`;
  return s;
}

function buildTouchContent(touchTemplate, { greeting, halfOff, savings, monthly, weekly, payInFullUrl, monthlyUrl, weeklyUrl, unsubscribeUrl }) {
  const optionsHTML = buildOptionCards({ payInFullUrl, halfOff, savings, monthlyUrl, monthly, weeklyUrl, weekly,
    emphasis: touchTemplate === 'touch3_small_steps' ? 'weekly' : (touchTemplate === 'touch5_offer_reminder' ? 'pay_in_full' : null) });
  const optionsText = buildOptionPlaintext({ halfOff, savings, monthlyUrl, monthly, weeklyUrl, weekly, payInFullUrl });

  switch (touchTemplate) {
    case 'touch1_warm_letter':
      // (Already built — uses the existing buildPaymentLinkEmail function. We delegate.)
      return null;

    case 'touch2_check_in': {
      const subject = `Just making sure my note came through`;
      const body = `
        <p style="font-size:15px;line-height:1.7;margin:0 0 16px">I sent a note last week and wanted to make sure it didn't get lost in your inbox. We'd love to help you close the loop on your account in a way that feels good.</p>
        <p style="font-size:15px;line-height:1.7;margin:0 0 20px">Here are the options I mentioned, just in case:</p>`;
      const closing = `
        <p style="font-size:15px;line-height:1.7;margin:24px 0 16px">No pressure at all — just here to make this easy.</p>
        <p style="font-size:15px;line-height:1.7;margin:0 0 4px">With appreciation,</p>
        <p style="font-size:15px;line-height:1.7;margin:0;font-weight:500">Mary Wardlaw</p>
        <p style="font-size:13px;line-height:1.7;margin:0;color:#555">Owner | The Children's Center</p>`;
      return {
        subject,
        html: campaignEmailLayout({ greeting, headerSubtitle: 'A note from Mary Wardlaw', body, optionsHTML, closingHTML: closing, unsubscribeUrl }),
        text: `Dear ${greeting},\n\nI sent a note last week and wanted to make sure it didn't get lost in your inbox. We'd love to help you close the loop on your account in a way that feels good.\n\nHere are the options I mentioned, just in case:\n${optionsText}\nNo pressure at all — just here to make this easy.\n\nWith appreciation,\nMary Wardlaw\nOwner | The Children's Center\n\n${unsubscribeUrl ? `To stop receiving these emails: ${unsubscribeUrl}\n` : ''}`,
      };
    }

    case 'touch3_small_steps': {
      const subject = `Sometimes a small step is the right step`;
      const body = `
        <p style="font-size:15px;line-height:1.7;margin:0 0 16px">One thing I love about our families is how much they value doing right by their commitments — even when life is busy.</p>
        <p style="font-size:15px;line-height:1.7;margin:0 0 20px">If a big payment isn't possible right now, even <strong>$25 a week</strong> can quietly close this out over time. There's no judgment here, just a path forward whenever you're ready.</p>`;
      const closing = `
        <p style="font-size:15px;line-height:1.7;margin:24px 0 16px">Whatever pace works for your family, we're here for it.</p>
        <p style="font-size:15px;line-height:1.7;margin:0 0 4px">With appreciation,</p>
        <p style="font-size:15px;line-height:1.7;margin:0;font-weight:500">Mary Wardlaw</p>
        <p style="font-size:13px;line-height:1.7;margin:0;color:#555">Owner | The Children's Center</p>`;
      return {
        subject,
        html: campaignEmailLayout({ greeting, headerSubtitle: 'A note from Mary Wardlaw', body, optionsHTML, closingHTML: closing, unsubscribeUrl }),
        text: `Dear ${greeting},\n\nOne thing I love about our families is how much they value doing right by their commitments — even when life is busy.\n\nIf a big payment isn't possible right now, even $25 a week can quietly close this out over time. There's no judgment here, just a path forward whenever you're ready.\n${optionsText}\nWhatever pace works for your family, we're here for it.\n\nWith appreciation,\nMary Wardlaw\nOwner | The Children's Center\n\n${unsubscribeUrl ? `To stop receiving these emails: ${unsubscribeUrl}\n` : ''}`,
      };
    }

    case 'touch4_sms_brief': {
      // SMS — keep under 320 chars (2 segments). No links to unsubscribe (SMS reply STOP is industry standard).
      const text = `Hi ${greeting}, this is Mary from The Children's Center. Just wanted to reach out about your account — we have payment options as low as $25/week. Here's the link: ${payInFullUrl} — Mary. Reply STOP to opt out.`;
      return { text };
    }

    case 'touch5_offer_reminder': {
      const subject = `A reminder about the half-off option`;
      const body = `
        <p style="font-size:15px;line-height:1.7;margin:0 0 16px">I wanted to gently remind you about the special <strong>50%-off pay-in-full option</strong> I extended. It won't be available indefinitely, and I'd hate for you to miss the chance to settle for half of what's owed.</p>
        <p style="font-size:15px;line-height:1.7;margin:0 0 20px">Pay in full today for just <strong>$${halfOff}</strong> — savings of <strong>$${savings}</strong>.</p>`;
      const closing = `
        <p style="font-size:15px;line-height:1.7;margin:24px 0 16px">Of course, the monthly and weekly options remain available too. Whatever works for your family.</p>
        <p style="font-size:15px;line-height:1.7;margin:0 0 4px">With appreciation,</p>
        <p style="font-size:15px;line-height:1.7;margin:0;font-weight:500">Mary Wardlaw</p>
        <p style="font-size:13px;line-height:1.7;margin:0;color:#555">Owner | The Children's Center</p>`;
      return {
        subject,
        html: campaignEmailLayout({ greeting, headerSubtitle: 'A note from Mary Wardlaw', body, optionsHTML, closingHTML: closing, unsubscribeUrl }),
        text: `Dear ${greeting},\n\nI wanted to gently remind you about the special 50%-off pay-in-full option I extended. It won't be available indefinitely, and I'd hate for you to miss the chance to settle for half of what's owed.\n\nPay in full today for just $${halfOff} — savings of $${savings}.\n${optionsText}\nOf course, the monthly and weekly options remain available too. Whatever works for your family.\n\nWith appreciation,\nMary Wardlaw\nOwner | The Children's Center\n\n${unsubscribeUrl ? `To stop receiving these emails: ${unsubscribeUrl}\n` : ''}`,
      };
    }

    case 'touch6_sms_final': {
      const text = `Hi ${greeting}, Mary from The Children's Center. The 50% pay-in-full offer is still on the table — would love to help you close this out. Link: ${payInFullUrl} — Reply STOP to opt out.`;
      return { text };
    }

    case 'touch7_final_note': {
      const subject = `A final friendly note`;
      const body = `
        <p style="font-size:15px;line-height:1.7;margin:0 0 16px">I've reached out a few times now without hearing back, so I wanted to send one more note before we have to take the next step on your account.</p>
        <p style="font-size:15px;line-height:1.7;margin:0 0 20px">The options I've shared remain available — including the pay-in-full discount. We'd genuinely rather work this out together than have to escalate, and I want to give you every opportunity to do so.</p>`;
      const closing = `
        <p style="font-size:15px;line-height:1.7;margin:24px 0 16px">If now isn't the right moment, please call me directly at <strong>(269) 683-0405</strong>. I'll personally work with you to find a way forward.</p>
        <p style="font-size:15px;line-height:1.7;margin:0 0 4px">With appreciation,</p>
        <p style="font-size:15px;line-height:1.7;margin:0;font-weight:500">Mary Wardlaw</p>
        <p style="font-size:13px;line-height:1.7;margin:0;color:#555">Owner | The Children's Center</p>`;
      return {
        subject,
        html: campaignEmailLayout({ greeting, headerSubtitle: 'A note from Mary Wardlaw', body, optionsHTML, closingHTML: closing, unsubscribeUrl }),
        text: `Dear ${greeting},\n\nI've reached out a few times now without hearing back, so I wanted to send one more note before we have to take the next step on your account.\n\nThe options I've shared remain available — including the pay-in-full discount. We'd genuinely rather work this out together than have to escalate, and I want to give you every opportunity to do so.\n${optionsText}\nIf now isn't the right moment, please call me directly at (269) 683-0405. I'll personally work with you to find a way forward.\n\nWith appreciation,\nMary Wardlaw\nOwner | The Children's Center\n\n${unsubscribeUrl ? `To stop receiving these emails: ${unsubscribeUrl}\n` : ''}`,
      };
    }
  }
  return null;
}

// ── Campaign API endpoints ───────────────────────────────────────────────────
// Start a 7-touch sequence for a family.
// Touch 1 fires immediately (synchronously) so Mary sees confirmation right away.
// Touches 2-7 are scheduled and fire via the background scheduler.
app.post('/api/collections/families/:id/campaign/start', ssoAuth, async (req, res) => {
  const family = await familyWithBalance(req.params.id);
  if (!family) return res.status(404).json({ error: 'Family not found' });

  // Pre-flight checks
  if (family.status === 'settled') return res.status(400).json({ error: 'Family is already settled — no campaign needed.' });
  if (family.email_unsubscribed) return res.status(400).json({ error: 'Family has unsubscribed.' });
  if (!family.payinfull_link_url) return res.status(400).json({ error: 'Generate Stripe payment links first.' });

  // Resolve recipient email
  let toEmail = family.primary_contact_email;
  if (!toEmail) {
    const { rows } = await pool.query(
      `SELECT c.parent_email FROM collections_family_children fc
       JOIN children c ON c.id=fc.child_id
       WHERE fc.family_id=$1 AND c.parent_email IS NOT NULL AND c.parent_email <> ''
       LIMIT 1`, [req.params.id]);
    toEmail = rows[0]?.parent_email;
  }
  if (!toEmail) return res.status(400).json({ error: 'No email on file. Add one before enrolling.' });

  // Check if already enrolled (only one active enrollment per family at a time)
  const existing = await pool.query(
    `SELECT id, status FROM campaign_enrollments WHERE family_id=$1`, [req.params.id]);
  if (existing.rows[0] && existing.rows[0].status === 'active') {
    return res.status(409).json({ error: 'Family already has an active campaign. Cancel it first to restart.' });
  }

  // Create or revive enrollment
  let enrollmentId;
  if (existing.rows[0]) {
    await pool.query(
      `UPDATE campaign_enrollments SET status='active', enrolled_at=NOW(), paused_at=NULL,
         paused_reason=NULL, completed_at=NULL, completed_reason=NULL, created_by=$1 WHERE id=$2`,
      [req.user?.username || 'mary', existing.rows[0].id]);
    enrollmentId = existing.rows[0].id;
    // Clear old touches
    await pool.query(`DELETE FROM campaign_touches WHERE enrollment_id=$1`, [enrollmentId]);
  } else {
    const { rows } = await pool.query(
      `INSERT INTO campaign_enrollments (family_id, cadence, status, created_by)
       VALUES ($1, 'standard', 'active', $2) RETURNING id`,
      [req.params.id, req.user?.username || 'mary']);
    enrollmentId = rows[0].id;
  }

  // Schedule all 7 touches relative to NOW
  const now = new Date();
  for (const t of CAMPAIGN_TOUCHES) {
    const scheduledFor = new Date(now.getTime() + t.day_offset * 24 * 60 * 60 * 1000);
    await pool.query(
      `INSERT INTO campaign_touches (enrollment_id, family_id, touch_number, channel, scheduled_for)
       VALUES ($1, $2, $3, $4, $5)`,
      [enrollmentId, req.params.id, t.number, t.channel, scheduledFor]);
  }

  // Fire touch 1 synchronously (it's day 0 — Mary expects it sent right away)
  const touch1Result = await fireTouch(enrollmentId, 1);

  await pool.query(
    `INSERT INTO collections_events (family_id, event_type, detail) VALUES ($1, 'campaign_started', $2)`,
    [req.params.id, `7-touch sequence enrolled. Touch 1 ${touch1Result.success ? 'sent' : 'failed: ' + touch1Result.error}`]);

  res.json({
    enrollment_id: enrollmentId,
    touch1_sent: touch1Result.success,
    touch1_error: touch1Result.error,
    next_touches_scheduled: CAMPAIGN_TOUCHES.length - 1,
  });
});

app.post('/api/collections/families/:id/campaign/pause', ssoAuth, async (req, res) => {
  const reason = (req.body.reason || '').trim() || 'Paused by Mary';
  const { rows } = await pool.query(
    `UPDATE campaign_enrollments SET status='paused', paused_at=NOW(), paused_reason=$1
     WHERE family_id=$2 AND status='active' RETURNING id`,
    [reason, req.params.id]);
  if (!rows[0]) return res.status(404).json({ error: 'No active campaign to pause.' });
  await pool.query(
    `INSERT INTO collections_events (family_id, event_type, detail) VALUES ($1, 'campaign_paused', $2)`,
    [req.params.id, reason]);
  res.json({ paused: true });
});

app.post('/api/collections/families/:id/campaign/resume', ssoAuth, async (req, res) => {
  const { rows } = await pool.query(
    `UPDATE campaign_enrollments SET status='active', paused_at=NULL, paused_reason=NULL
     WHERE family_id=$1 AND status='paused' RETURNING id`,
    [req.params.id]);
  if (!rows[0]) return res.status(404).json({ error: 'No paused campaign to resume.' });
  await pool.query(
    `INSERT INTO collections_events (family_id, event_type, detail) VALUES ($1, 'campaign_resumed', 'Resumed by Mary')`,
    [req.params.id]);
  res.json({ resumed: true });
});

app.post('/api/collections/families/:id/campaign/cancel', ssoAuth, async (req, res) => {
  const reason = (req.body.reason || '').trim() || 'Cancelled by Mary';
  const { rows } = await pool.query(
    `UPDATE campaign_enrollments SET status='cancelled', completed_at=NOW(), completed_reason=$1
     WHERE family_id=$2 AND status IN ('active','paused') RETURNING id`,
    [reason, req.params.id]);
  if (!rows[0]) return res.status(404).json({ error: 'No campaign to cancel.' });
  await pool.query(
    `INSERT INTO collections_events (family_id, event_type, detail) VALUES ($1, 'campaign_cancelled', $2)`,
    [req.params.id, reason]);
  res.json({ cancelled: true });
});

// Get campaign status for a family — used by the family detail UI.
app.get('/api/collections/families/:id/campaign', ssoAuth, async (req, res) => {
  const enrollment = await pool.query(
    `SELECT * FROM campaign_enrollments WHERE family_id=$1`, [req.params.id]);
  if (!enrollment.rows[0]) return res.json({ enrolled: false });
  const touches = await pool.query(
    `SELECT * FROM campaign_touches WHERE enrollment_id=$1 ORDER BY touch_number`,
    [enrollment.rows[0].id]);
  res.json({ enrolled: true, enrollment: enrollment.rows[0], touches: touches.rows });
});

// List all enrollments — used by the global "Active Campaigns" page.
app.get('/api/campaigns/enrollments', ssoAuth, async (req, res) => {
  const status = req.query.status || 'active';
  const { rows } = await pool.query(
    `SELECT e.*, f.family_name, f.center, f.original_balance,
       (SELECT COUNT(*) FROM campaign_touches WHERE enrollment_id=e.id AND sent_at IS NOT NULL) AS sent_count,
       (SELECT MIN(scheduled_for) FROM campaign_touches WHERE enrollment_id=e.id AND sent_at IS NULL) AS next_send_at
     FROM campaign_enrollments e
     JOIN collections_families f ON f.id=e.family_id
     WHERE e.status=$1
     ORDER BY e.enrolled_at DESC`, [status]);
  res.json(rows);
});

// ── Touch firing logic ───────────────────────────────────────────────────────
// Sends a single touch. Called synchronously when touch 1 starts a campaign,
// or by the scheduler for touches 2-7.
async function fireTouch(enrollmentId, touchNumber) {
  const { rows: touchRows } = await pool.query(
    `SELECT t.*, e.status AS enrollment_status, e.family_id
     FROM campaign_touches t
     JOIN campaign_enrollments e ON e.id = t.enrollment_id
     WHERE t.enrollment_id=$1 AND t.touch_number=$2`,
    [enrollmentId, touchNumber]);
  const touch = touchRows[0];
  if (!touch) return { success: false, error: 'Touch not found' };
  if (touch.sent_at) return { success: false, error: 'Already sent' };
  if (touch.enrollment_status !== 'active') return { success: false, error: 'Enrollment not active: ' + touch.enrollment_status };

  // Re-fetch family fresh — auto-stop conditions may have changed since enrollment
  const family = await familyWithBalance(touch.family_id);
  if (!family) return { success: false, error: 'Family deleted' };

  if (family.status === 'settled') {
    await pool.query(`UPDATE campaign_enrollments SET status='completed', completed_at=NOW(), completed_reason='Family settled' WHERE id=$1`, [enrollmentId]);
    return { success: false, error: 'Family settled — campaign completed' };
  }
  if (family.email_unsubscribed) {
    await pool.query(`UPDATE campaign_enrollments SET status='completed', completed_at=NOW(), completed_reason='Family unsubscribed' WHERE id=$1`, [enrollmentId]);
    return { success: false, error: 'Family unsubscribed — campaign completed' };
  }

  // Resolve content
  const def = CAMPAIGN_TOUCHES.find(t => t.number === touchNumber);
  if (!def) return { success: false, error: 'No touch definition' };

  const greeting = await resolveGreeting(family);

  // Common content data
  const halfOff = (parseFloat(family.discount_pay_in_full) || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const savings = (parseFloat(family.remaining_balance) - parseFloat(family.discount_pay_in_full)).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  // Fetch monthly/weekly amounts from Stripe
  let monthlyAmount = null, weeklyAmount = null;
  try {
    if (stripe && family.paywhatyoucan_link_id) {
      const link = await stripe.paymentLinks.retrieve(family.paywhatyoucan_link_id, { expand: ['line_items.data.price'] });
      const price = link.line_items?.data?.[0]?.price;
      if (price?.unit_amount) monthlyAmount = price.unit_amount / 100;
    }
    if (stripe && family.weekly_link_id) {
      const link = await stripe.paymentLinks.retrieve(family.weekly_link_id, { expand: ['line_items.data.price'] });
      const price = link.line_items?.data?.[0]?.price;
      if (price?.unit_amount) weeklyAmount = price.unit_amount / 100;
    }
  } catch (e) { /* ignore */ }

  // Generate or fetch unsubscribe token
  let unsubscribeToken = family.email_unsubscribe_token;
  if (!unsubscribeToken) {
    unsubscribeToken = require('crypto').randomBytes(24).toString('hex');
    await pool.query(`UPDATE collections_families SET email_unsubscribe_token=$1 WHERE id=$2`,
      [unsubscribeToken, family.id]);
  }
  const appUrl = process.env.APP_URL || 'https://tcc-collections-hub.onrender.com';
  const unsubscribeUrl = `${appUrl}/api/collections/unsubscribe?token=${unsubscribeToken}`;

  // ─── Email touches ───────────────────────────────────────────────────────
  if (touch.channel === 'email') {
    if (!sendgrid) {
      await markTouchSkipped(touch.id, 'skipped_sendgrid_disabled', 'SendGrid not configured');
      return { success: false, error: 'SendGrid not configured' };
    }

    // Resolve recipient
    let toEmail = family.primary_contact_email;
    if (!toEmail) {
      const { rows: kids } = await pool.query(
        `SELECT c.parent_email FROM collections_family_children fc
         JOIN children c ON c.id=fc.child_id
         WHERE fc.family_id=$1 AND c.parent_email IS NOT NULL AND c.parent_email <> ''
         LIMIT 1`, [family.id]);
      toEmail = kids[0]?.parent_email;
    }
    if (!toEmail) {
      await markTouchSkipped(touch.id, 'skipped_no_email', 'No email on file');
      return { success: false, error: 'No email on file' };
    }

    let content;
    if (def.template === 'touch1_warm_letter') {
      // Touch 1 uses the existing buildPaymentLinkEmail (Mary's warm letter)
      family.primary_guardian_first_name = greeting;
      content = buildPaymentLinkEmail({
        family,
        payInFullUrl: family.payinfull_link_url,
        payInFullAmount: family.discount_pay_in_full,
        monthlyUrl: family.paywhatyoucan_link_url || null,
        monthlyAmount,
        weeklyUrl: family.weekly_link_url || null,
        weeklyAmount,
        fromName: process.env.SENDGRID_FROM_NAME || "The Children's Center Billing",
        unsubscribeUrl,
      });
    } else {
      content = buildTouchContent(def.template, {
        greeting,
        halfOff,
        savings,
        monthly: monthlyAmount ? monthlyAmount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : null,
        weekly: weeklyAmount ? weeklyAmount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : null,
        payInFullUrl: family.payinfull_link_url,
        monthlyUrl: family.paywhatyoucan_link_url || null,
        weeklyUrl: family.weekly_link_url || null,
        unsubscribeUrl,
      });
    }
    if (!content) {
      await markTouchSkipped(touch.id, 'failed', 'No template content');
      return { success: false, error: 'No content generated' };
    }

    try {
      const fromEmail = process.env.SENDGRID_FROM_EMAIL || 'billing@childrenscenterinc.com';
      const fromName = process.env.SENDGRID_FROM_NAME || "The Children's Center Billing";
      const result = await sendgrid.send({
        to: toEmail,
        from: { email: fromEmail, name: fromName },
        subject: content.subject,
        html: content.html,
        text: content.text,
        categories: ['collections', 'campaign', `touch-${touchNumber}`],
        customArgs: { family_id: String(family.id), touch_number: String(touchNumber), enrollment_id: String(enrollmentId) },
        headers: {
          'List-Unsubscribe': `<${unsubscribeUrl}>`,
          'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
        },
      });
      const messageId = result?.[0]?.headers?.['x-message-id'] || null;
      await pool.query(
        `UPDATE campaign_touches SET sent_at=NOW(), send_status='sent', sendgrid_message_id=$1 WHERE id=$2`,
        [messageId, touch.id]);
      await pool.query(
        `INSERT INTO collections_events (family_id, event_type, detail) VALUES ($1, 'campaign_touch_sent', $2)`,
        [family.id, `Touch ${touchNumber} email sent to ${toEmail}`]);
      // Check if this was the last touch
      await checkCampaignCompletion(enrollmentId);
      return { success: true };
    } catch (e) {
      const detail = e?.response?.body?.errors?.map(x => x.message).join('; ') || e.message;
      await markTouchSkipped(touch.id, 'failed', detail);
      return { success: false, error: detail };
    }
  }

  // ─── SMS touches ─────────────────────────────────────────────────────────
  if (touch.channel === 'sms') {
    // Skip if Twilio not set up OR SMS_ENABLED not turned on
    if (!twilio || process.env.SMS_ENABLED !== 'true') {
      const reason = !twilio ? 'Twilio not configured' : 'SMS_ENABLED not set to true (waiting on Twilio A2P approval)';
      await markTouchSkipped(touch.id, 'skipped_sms_disabled', reason);
      // Don't fail the campaign — just log and continue. The next touch will fire on schedule.
      return { success: true, skipped: true, reason };
    }

    let toPhone = family.primary_contact_phone;
    if (!toPhone) {
      const { rows: kids } = await pool.query(
        `SELECT c.parent_phone FROM collections_family_children fc
         JOIN children c ON c.id=fc.child_id
         WHERE fc.family_id=$1 AND c.parent_phone IS NOT NULL AND c.parent_phone <> ''
         LIMIT 1`, [family.id]);
      toPhone = kids[0]?.parent_phone;
    }
    if (!toPhone) {
      await markTouchSkipped(touch.id, 'skipped_no_phone', 'No phone on file');
      return { success: true, skipped: true, reason: 'No phone' };
    }
    // Normalize phone to E.164
    const e164Phone = normalizePhone(toPhone);
    if (!e164Phone) {
      await markTouchSkipped(touch.id, 'skipped_no_phone', 'Phone not parseable');
      return { success: true, skipped: true, reason: 'Phone format' };
    }

    const content = buildTouchContent(def.template, {
      greeting,
      halfOff, savings,
      monthly: monthlyAmount ? monthlyAmount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : null,
      weekly: weeklyAmount ? weeklyAmount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : null,
      payInFullUrl: family.payinfull_link_url,
      monthlyUrl: family.paywhatyoucan_link_url || null,
      weeklyUrl: family.weekly_link_url || null,
      unsubscribeUrl: null,  // SMS uses STOP keyword instead of URL
    });

    try {
      const msg = await twilio.messages.create({
        from: process.env.TWILIO_FROM_NUMBER,
        to: e164Phone,
        body: content.text,
      });
      await pool.query(
        `UPDATE campaign_touches SET sent_at=NOW(), send_status='sent', twilio_sid=$1 WHERE id=$2`,
        [msg.sid, touch.id]);
      await pool.query(
        `INSERT INTO collections_events (family_id, event_type, detail) VALUES ($1, 'campaign_touch_sent', $2)`,
        [family.id, `Touch ${touchNumber} SMS sent to ${e164Phone}`]);
      await checkCampaignCompletion(enrollmentId);
      return { success: true };
    } catch (e) {
      await markTouchSkipped(touch.id, 'failed', e.message);
      return { success: false, error: e.message };
    }
  }
}

async function markTouchSkipped(touchId, status, errorDetail) {
  await pool.query(
    `UPDATE campaign_touches SET sent_at=NOW(), send_status=$1, error_detail=$2 WHERE id=$3`,
    [status, errorDetail, touchId]);
}

async function checkCampaignCompletion(enrollmentId) {
  // If all 7 touches have a sent_at, mark enrollment completed.
  const { rows } = await pool.query(
    `SELECT COUNT(*) AS pending FROM campaign_touches WHERE enrollment_id=$1 AND sent_at IS NULL`,
    [enrollmentId]);
  if (parseInt(rows[0].pending) === 0) {
    await pool.query(
      `UPDATE campaign_enrollments SET status='completed', completed_at=NOW(),
         completed_reason='All 7 touches delivered' WHERE id=$1 AND status='active'`,
      [enrollmentId]);
  }
}

// Side-effect hook: when a payment is received for a family, update the
// enrollment record. Settlement is auto-detected by fireTouch (it re-checks
// the family status before each touch and stops the sequence if settled).
// This function additionally handles the "partial payment pauses sequence"
// rule: if remaining_balance > 0 after a payment, the sequence pauses for
// 30 days unless another payment comes through.
async function onPaymentReceived(familyId, paymentAmount) {
  const family = await familyWithBalance(familyId);
  if (!family) return;

  const { rows: enrollRows } = await pool.query(
    `SELECT * FROM campaign_enrollments WHERE family_id=$1 AND status IN ('active','paused')`, [familyId]);
  const enrollment = enrollRows[0];
  if (!enrollment) return;

  if (family.remaining_balance <= 0.01) {
    // Settled — completed
    await pool.query(
      `UPDATE campaign_enrollments SET status='completed', completed_at=NOW(),
         completed_reason='Family settled — payment cleared balance' WHERE id=$1`,
      [enrollment.id]);
    await pool.query(
      `INSERT INTO collections_events (family_id, event_type, detail)
       VALUES ($1, 'campaign_completed', $2)`,
      [familyId, `Campaign completed (settled via $${paymentAmount.toFixed(2)} payment)`]);
  } else {
    // Partial payment — pause for 30 days.
    // The scheduler will resume sending if no further payment within that window.
    await pool.query(
      `UPDATE campaign_enrollments SET status='paused', paused_at=NOW(),
         paused_reason='Partial payment received — paused 30 days', last_payment_at=NOW()
       WHERE id=$1`, [enrollment.id]);

    // Push remaining touches out by 30 days
    await pool.query(
      `UPDATE campaign_touches SET scheduled_for = scheduled_for + INTERVAL '30 days'
       WHERE enrollment_id=$1 AND sent_at IS NULL`, [enrollment.id]);

    await pool.query(
      `INSERT INTO collections_events (family_id, event_type, detail)
       VALUES ($1, 'campaign_paused_partial_payment', $2)`,
      [familyId, `Partial payment of $${paymentAmount.toFixed(2)}; campaign paused 30 days`]);

    // Auto-resume after 30 days if no further payment — handled by scheduler:
    // when scheduler ticks, we check paused enrollments and re-activate any
    // whose paused_at is > 30 days ago.
  }
}

async function resolveGreeting(family) {
  // Use primary guardian first name if linked-children info populates parent_name.
  const { rows } = await pool.query(
    `SELECT c.parent_name FROM collections_family_children fc
     JOIN children c ON c.id=fc.child_id
     WHERE fc.family_id=$1 AND c.parent_name IS NOT NULL AND c.parent_name <> ''
     LIMIT 1`, [family.id]);
  if (rows[0]?.parent_name) {
    const name = rows[0].parent_name.trim();
    if (name.includes(',')) return name.split(',')[1]?.trim().split(/\s+/)[0] || family.family_name;
    return name.split(/\s+/)[0] || family.family_name;
  }
  return family.family_name.split(/[\s,]+/)[0] || family.family_name;
}

function normalizePhone(phone) {
  if (!phone) return null;
  // Strip everything but digits
  const digits = String(phone).replace(/\D/g, '');
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  return null;
}

// ── Background scheduler ─────────────────────────────────────────────────────
// Runs every 15 minutes. Finds due-and-unsent touches and fires them.
// Volume-controlled: max 30 emails per tick to protect sender reputation.
async function runCampaignScheduler() {
  try {
    // Step 1: Auto-resume any campaigns paused for partial payment if 30+ days elapsed
    // and no further payment recorded.
    await pool.query(`
      UPDATE campaign_enrollments
      SET status='active', paused_at=NULL,
        paused_reason = 'Auto-resumed after 30 days no further payment'
      WHERE status='paused'
        AND paused_at IS NOT NULL
        AND paused_at < NOW() - INTERVAL '30 days'
        AND paused_reason LIKE '%Partial payment%'
    `);

    // Step 2: Fire due touches (max 30 per tick to protect sender reputation)
    const { rows: due } = await pool.query(`
      SELECT t.id, t.enrollment_id, t.touch_number, t.channel
      FROM campaign_touches t
      JOIN campaign_enrollments e ON e.id = t.enrollment_id
      WHERE t.sent_at IS NULL
        AND t.scheduled_for <= NOW()
        AND e.status = 'active'
      ORDER BY t.scheduled_for ASC
      LIMIT 30`);
    if (due.length === 0) return;
    console.log(`[scheduler] firing ${due.length} due touches`);
    for (const t of due) {
      try {
        await fireTouch(t.enrollment_id, t.touch_number);
      } catch (e) {
        console.error(`[scheduler] touch ${t.id} crashed:`, e.message);
      }
    }
  } catch (e) {
    console.error('[scheduler] tick failed:', e.message);
  }
}

// Kick off the scheduler. Tick every 15 minutes (900 seconds).
// Also runs once 30 seconds after server startup to catch any straggler from a restart.
setTimeout(runCampaignScheduler, 30 * 1000);
setInterval(runCampaignScheduler, 15 * 60 * 1000);

// Manual trigger endpoint — useful for testing without waiting 15 min.
app.post('/api/campaigns/scheduler/run-now', ssoAuth, async (req, res) => {
  await runCampaignScheduler();
  res.json({ ran: true });
});

// ── Transactions / Adjustments ───────────────────────────────────────────────
const TXN_TYPES_REDUCING = new Set(['payment','credit','pay_in_full_discount']);
const TXN_TYPES_INCREASING = new Set(['charge','refund']);
const ALL_TXN_TYPES = new Set([...TXN_TYPES_REDUCING, ...TXN_TYPES_INCREASING, 'starting_balance','other']);

// List a family's ledger
app.get('/api/collections/families/:id/transactions', ssoAuth, async (req, res) => {
  const { rows } = await pool.query(
    `SELECT * FROM collections_transactions WHERE family_id=$1 ORDER BY txn_date DESC, created_at DESC`,
    [req.params.id]
  );
  res.json(rows);
});

// Add a transaction (manual adjustment or payment record)
app.post('/api/collections/families/:id/transactions', ssoAuth, async (req, res) => {
  const familyId = req.params.id;
  const { txn_type, txn_type_label, amount, description, reason, txn_date } = req.body;

  if (!txn_type || !ALL_TXN_TYPES.has(txn_type)) {
    return res.status(400).json({ error: 'Invalid or missing txn_type' });
  }
  const amt = parseFloat(amount);
  if (isNaN(amt) || amt === 0) return res.status(400).json({ error: 'Amount must be a non-zero number' });
  if (!reason || String(reason).trim().length < 5) {
    return res.status(400).json({ error: 'Reason is required (at least 5 characters)' });
  }

  // Sign the amount based on type: reducing types flip to positive, increasing flip to negative.
  // User enters a positive dollar amount; we apply the sign server-side for consistency.
  const magnitude = Math.abs(amt);
  let signedAmount;
  if (TXN_TYPES_REDUCING.has(txn_type)) signedAmount = magnitude;
  else if (TXN_TYPES_INCREASING.has(txn_type)) signedAmount = -magnitude;
  else if (txn_type === 'starting_balance') signedAmount = -magnitude; // starting balance always owed
  else signedAmount = amt; // 'other' — respect user's sign

  const { rows } = await pool.query(
    `INSERT INTO collections_transactions
       (family_id, txn_date, txn_type, txn_type_label, amount, description, reason, created_by)
     VALUES ($1, COALESCE($2::date, CURRENT_DATE), $3, $4, $5, $6, $7, $8) RETURNING *`,
    [familyId, txn_date || null, txn_type, txn_type_label || null, signedAmount,
     description || null, reason, req.user?.email || 'manual']
  );

  // If paying in full settled them, mark family settled
  const check = await familyWithBalance(familyId);
  if (check && check.remaining_balance <= 0.01) {
    await pool.query(`UPDATE collections_families SET status='settled', settled_at=NOW() WHERE id=$1 AND status != 'settled'`, [familyId]);
  }
  res.json(rows[0]);
});

// Void a transaction (inserts an offsetting entry — never hard-deletes)
app.post('/api/collections/families/:id/transactions/:txnId/void', ssoAuth, async (req, res) => {
  const { reason } = req.body;
  if (!reason || String(reason).trim().length < 5) return res.status(400).json({ error: 'Void reason required' });
  const { rows: orig } = await pool.query(
    `SELECT * FROM collections_transactions WHERE id=$1 AND family_id=$2`,
    [req.params.txnId, req.params.id]
  );
  if (!orig[0]) return res.status(404).json({ error: 'Transaction not found' });
  await pool.query(
    `INSERT INTO collections_transactions
       (family_id, txn_date, txn_type, amount, description, reason, created_by)
     VALUES ($1, CURRENT_DATE, 'other', $2, $3, $4, $5)`,
    [req.params.id, -parseFloat(orig[0].amount),
     `VOID of txn #${orig[0].id} (${orig[0].txn_type})`,
     reason, req.user?.email || 'manual']
  );
  res.json({ voided: orig[0].id });
});

// ── Child Linking ────────────────────────────────────────────────────────────
// List candidate children (roster children not yet linked to any family)
app.get('/api/collections/families/:id/candidate-children', ssoAuth, async (req, res) => {
  const { q } = req.query;
  const params = [];
  let sql = `SELECT c.id, c.first_name, c.last_name, c.center, c.parent_name, c.parent_email
             FROM children c
             WHERE c.is_active = true`;
  if (q) {
    params.push(`%${q.toLowerCase()}%`);
    sql += ` AND (LOWER(c.last_name) LIKE $${params.length} OR LOWER(c.first_name) LIKE $${params.length} OR LOWER(c.parent_name) LIKE $${params.length})`;
  }
  sql += ` ORDER BY c.last_name, c.first_name LIMIT 50`;
  const { rows } = await pool.query(sql, params);
  res.json(rows);
});

// Link a child to a past-due family
app.post('/api/collections/families/:id/link-child', ssoAuth, async (req, res) => {
  const { child_id } = req.body;
  if (!child_id) return res.status(400).json({ error: 'child_id required' });
  await pool.query(
    `INSERT INTO collections_family_children (family_id, child_id)
     VALUES ($1, $2) ON CONFLICT (family_id, child_id) DO NOTHING`,
    [req.params.id, child_id]
  );
  res.json({ linked: true });
});

// Unlink
app.post('/api/collections/families/:id/unlink-child', ssoAuth, async (req, res) => {
  const { child_id } = req.body;
  await pool.query(
    `DELETE FROM collections_family_children WHERE family_id=$1 AND child_id=$2`,
    [req.params.id, child_id]
  );
  res.json({ unlinked: true });
});

// ── Printable Statement ──────────────────────────────────────────────────────
// Returns an HTML document suitable for browser print / save-as-PDF showing
// the full transaction ledger for a family. Hand this to a family that
// challenges their balance.
app.get('/api/collections/families/:id/statement', ssoAuth, async (req, res) => {
  const family = await familyWithBalance(req.params.id);
  if (!family) return res.status(404).send('Not found');

  const { rows: txns } = await pool.query(
    `SELECT * FROM collections_transactions WHERE family_id=$1 ORDER BY txn_date ASC, created_at ASC`,
    [req.params.id]
  );
  const { rows: linked } = await pool.query(
    `SELECT c.first_name, c.last_name FROM collections_family_children fc
     JOIN children c ON c.id = fc.child_id WHERE fc.family_id=$1
     ORDER BY c.last_name, c.first_name`,
    [req.params.id]
  );

  const fmtUSD = (n) => {
    const v = parseFloat(n) || 0;
    const sign = v < 0 ? '-' : '';
    return `${sign}$${Math.abs(v).toFixed(2)}`;
  };
  const fmtDate = (d) => {
    if (!d) return '';
    const dt = new Date(d);
    return dt.toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric', timeZone:'UTC' });
  };
  const typeLabel = (t, label) => {
    if (t === 'other' && label) return label;
    return ({
      starting_balance: 'Starting Balance',
      charge: 'Charge',
      payment: 'Payment',
      credit: 'Credit / Adjustment',
      refund: 'Refund',
      pay_in_full_discount: 'Pay-in-Full 50% Discount',
      other: 'Other',
    })[t] || t;
  };

  // Running balance — starts at 0, each txn is signed so balance = -runningSum.
  let running = 0;
  const rows = txns.map(t => {
    running += parseFloat(t.amount);
    return {
      ...t,
      running_owed: -running,
    };
  });

  const html = `<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<title>Statement of Account — ${family.family_name}</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: Helvetica, Arial, sans-serif; color: #000; padding: 12mm; font-size: 11px; background: #fff; }
  .header { border-bottom: 3px solid #1a2744; padding-bottom: 12px; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: flex-end; }
  .title { font-family: 'Times New Roman', serif; font-size: 26px; color: #1a2744; }
  .subtitle { font-size: 11px; color: #555; margin-top: 4px; }
  .meta-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px 24px; margin-bottom: 20px; }
  .meta-grid .label { font-size: 10px; color: #666; text-transform: uppercase; letter-spacing: .5px; }
  .meta-grid .value { font-size: 13px; font-weight: 600; color: #1a2744; }
  table { width: 100%; border-collapse: collapse; margin-bottom: 18px; }
  th { background: #1a2744; color: #fff; padding: 8px 10px; text-align: left; font-size: 11px; font-weight: 600; }
  td { padding: 7px 10px; border-bottom: 1px solid #ddd; font-size: 11px; vertical-align: top; }
  tr:nth-child(even) td { background: #f7f7f7; }
  .amt { text-align: right; font-family: monospace; }
  .amt-reduce { color: #1e8449; }
  .amt-increase { color: #c0392b; }
  .amt-balance { font-weight: 600; }
  .summary { border: 2px solid #1a2744; padding: 14px 18px; background: #f7f5ef; display: flex; justify-content: space-between; align-items: center; }
  .summary .label { font-size: 11px; color: #555; text-transform: uppercase; letter-spacing: .5px; }
  .summary .value { font-family: 'Times New Roman', serif; font-size: 28px; color: #c0392b; font-weight: 700; }
  .footer { margin-top: 24px; padding-top: 12px; border-top: 1px solid #ccc; font-size: 10px; color: #666; line-height: 1.6; }
  .reason-tag { font-style: italic; color: #666; font-size: 10px; display: block; margin-top: 2px; }
  .toolbar { margin-bottom: 14px; padding-bottom: 10px; border-bottom: 1px solid #ddd; }
  .toolbar button { padding: 6px 14px; background: #1a2744; color: #fff; border: none; border-radius: 4px; font-size: 12px; cursor: pointer; }
  @media print {
    .toolbar { display: none; }
    body { padding: 8mm; }
    @page { size: letter; margin: 12mm; }
  }
</style></head>
<body>

<div class="toolbar">
  <button onclick="window.print()">🖨 Print</button>
  <button onclick="window.close()" style="background:#fff;color:#1a2744;border:1px solid #1a2744;margin-left:8px">Close</button>
</div>

<div class="header">
  <div>
    <div class="title">Statement of Account</div>
    <div class="subtitle">The Children's Center, Inc.</div>
  </div>
  <div style="text-align:right;font-size:11px;color:#666">
    Generated ${fmtDate(new Date().toISOString())}<br>
    Account #${family.id}
  </div>
</div>

<div class="meta-grid">
  <div>
    <div class="label">Account Holder</div>
    <div class="value">${family.family_name}</div>
  </div>
  <div>
    <div class="label">Center</div>
    <div class="value">${family.center || '—'}</div>
  </div>
  ${linked.length ? `
  <div>
    <div class="label">Children</div>
    <div class="value">${linked.map(c => `${c.first_name} ${c.last_name}`).join(', ')}</div>
  </div>` : ''}
  ${family.left_date ? `
  <div>
    <div class="label">Left Date</div>
    <div class="value">${fmtDate(family.left_date)}</div>
  </div>` : ''}
  ${family.primary_contact_email ? `
  <div>
    <div class="label">Email</div>
    <div class="value" style="font-weight:400">${family.primary_contact_email}</div>
  </div>` : ''}
  ${family.primary_contact_phone ? `
  <div>
    <div class="label">Phone</div>
    <div class="value" style="font-weight:400">${family.primary_contact_phone}</div>
  </div>` : ''}
</div>

<table>
  <thead><tr>
    <th>Date</th>
    <th>Type</th>
    <th>Description</th>
    <th class="amt">Amount</th>
    <th class="amt">Balance Owed</th>
  </tr></thead>
  <tbody>
    ${rows.length === 0 ? `<tr><td colspan="5" style="text-align:center;color:#888;padding:20px">No transactions recorded.</td></tr>` : ''}
    ${rows.map(t => {
      const amt = parseFloat(t.amount);
      const cls = amt >= 0 ? 'amt-reduce' : 'amt-increase';
      return `<tr>
        <td>${fmtDate(t.txn_date)}</td>
        <td>${typeLabel(t.txn_type, t.txn_type_label)}</td>
        <td>${t.description || ''}${t.reason ? `<span class="reason-tag">Reason: ${t.reason}</span>` : ''}</td>
        <td class="amt ${cls}">${fmtUSD(amt)}</td>
        <td class="amt amt-balance">${fmtUSD(t.running_owed)}</td>
      </tr>`;
    }).join('')}
  </tbody>
</table>

<div class="summary">
  <div>
    <div class="label">Current Balance Owed</div>
    <div style="font-size:11px;color:#666;margin-top:2px">As of ${fmtDate(new Date().toISOString())}</div>
  </div>
  <div class="value">${fmtUSD(family.remaining_balance)}</div>
</div>

<div class="footer">
  This statement reflects all charges, payments, credits, and adjustments on file for this account.
  If you have any questions about a specific transaction, please contact The Children's Center billing office.
  This is a computer-generated statement.
</div>

</body></html>`;

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(html);
});

// ── Hook: record successful Stripe payments into the ledger ──────────────────
// When Stripe tells us a payment succeeded, also insert a ledger entry so the
// statement reflects it. The pay-in-full link also produces a 50% discount
// credit that zeros out the remaining balance.
const origHandleStripeEvent = handleStripeEvent;
handleStripeEvent = async function(event) {
  await origHandleStripeEvent(event);

  const type = event.type;
  const obj = event.data.object;

  // Recurring monthly subscription payment — log it in the ledger too
  if (type === 'invoice.payment_succeeded') {
    let famId = null;
    let metadata = obj.subscription_details?.metadata || {};
    if (metadata.family_id) famId = parseInt(metadata.family_id);
    if (!famId && obj.subscription) {
      try {
        const sub = await stripe.subscriptions.retrieve(obj.subscription);
        if (sub.metadata?.family_id) famId = parseInt(sub.metadata.family_id);
      } catch { /* ignore */ }
    }
    if (!famId) return;

    const amount = (obj.amount_paid || 0) / 100;
    const piId = obj.payment_intent || `inv_${obj.id}`;

    // Idempotent
    const { rows: existing } = await pool.query(
      `SELECT id FROM collections_transactions WHERE stripe_payment_intent_id=$1`, [piId]
    );
    if (existing.length) return;

    await pool.query(
      `INSERT INTO collections_transactions
         (family_id, txn_date, txn_type, amount, description, reason, stripe_payment_intent_id, created_by)
       VALUES ($1, CURRENT_DATE, 'payment', $2, $3, $4, $5, 'stripe_webhook')`,
      [famId, amount, 'Monthly subscription payment',
       `Stripe invoice ${obj.id}`, piId]
    );
    // Notify campaign engine — pause/complete sequence based on remaining balance
    try { await onPaymentReceived(famId, amount); } catch (e) { console.error('onPaymentReceived failed:', e.message); }
    return;
  }

  // One-time payments (pay-in-full or legacy one-time pay-what-you-can)
  if (type !== 'payment_intent.succeeded' && type !== 'checkout.session.completed') return;

  const pi = type === 'checkout.session.completed' && obj.payment_intent
    ? await stripe.paymentIntents.retrieve(obj.payment_intent)
    : obj;

  const metadata = pi.metadata || obj.metadata || {};
  const familyId = metadata.family_id ? parseInt(metadata.family_id) : null;
  if (!familyId) return;

  const amount = (pi.amount_received || pi.amount || 0) / 100;
  const linkType = metadata.link_type || '';

  // Skip — subscription invoices already handled above and would have a separate PI
  // We only want one-time pay-in-full or legacy one-time pwyc here.
  // Check if we already recorded this payment (idempotent)
  const { rows: existing } = await pool.query(
    `SELECT id FROM collections_transactions WHERE stripe_payment_intent_id=$1`,
    [pi.id]
  );
  if (existing.length) return;

  // Record the payment
  await pool.query(
    `INSERT INTO collections_transactions
       (family_id, txn_date, txn_type, amount, description, reason, stripe_payment_intent_id, created_by)
     VALUES ($1, CURRENT_DATE, 'payment', $2, $3, $4, $5, 'stripe_webhook')`,
    [familyId, amount,
     linkType === 'pay_in_full' ? 'Pay-in-Full payment (50% off)' : 'Online payment',
     `Stripe payment ${pi.id}`,
     pi.id]
  );

  // If this was a pay-in-full payment, add the matching 50% discount credit
  if (linkType === 'pay_in_full') {
    const family = await familyWithBalance(familyId);
    if (family && family.remaining_balance > 0.01) {
      await pool.query(
        `INSERT INTO collections_transactions
           (family_id, txn_date, txn_type, amount, description, reason, stripe_payment_intent_id, created_by)
         VALUES ($1, CURRENT_DATE, 'pay_in_full_discount', $2, $3, $4, $5, 'stripe_webhook')`,
        [familyId, family.remaining_balance,
         '50% pay-in-full discount',
         'Applied at time of pay-in-full payment',
         pi.id]
      );
    }
  }
  // Notify campaign engine — pause/complete sequence based on remaining balance
  try { await onPaymentReceived(familyId, amount); } catch (e) { console.error('onPaymentReceived failed:', e.message); }
};


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
