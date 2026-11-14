// ============================================================
//  eComplaint Server
//  Express + Socket.io + Neon Postgres + Gemini AI scoring + Cloudinary images
// ============================================================
require('dotenv').config();
// ---------- dependencies ----------
const express = require('express');
const http = require('http');
const cors = require('cors');
const crypto = require('crypto');
const multer = require('multer');
const cloudinary = require('cloudinary').v2;
const { Pool } = require('pg');
const { GoogleGenAI } = require('@google/genai');
const { Server } = require('socket.io');

// ---------- app / server / io setup ----------
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    }
});

const PORT = process.env.PORT || 3000;

app.use(cors({ origin: true }));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ---------- Gemini AI ----------
const ai = new GoogleGenAI({
    apiKey: process.env.crater
});

// ---------- Cloudinary (image storage) ----------
cloudinary.config({
    cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
    api_key: process.env.CLOUDINARY_API_KEY,
    api_secret: process.env.CLOUDINARY_API_SECRET
});

const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 5 * 1024 * 1024 } // 5MB cap
});

async function uploadImageIfPresent(file) {
    if (!file) return null;
    try {
        const result = await new Promise((resolve, reject) => {
            cloudinary.uploader
                .upload_stream({ folder: 'complaints' }, (err, res) => (err ? reject(err) : resolve(res)))
                .end(file.buffer);
        });
        return { url: result.secure_url, publicId: result.public_id };
    } catch (err) {
        console.error('❌ Cloudinary upload failed:', err);
        return null; // don't fail the whole complaint over a bad image upload
    }
}

// ---------- Neon Postgres ----------
console.log('Database URL exists:', !!process.env.reklamodb);

const pool = new Pool({
    connectionString: process.env.reklamodb,
    ssl: { rejectUnauthorized: false }
});

async function testDbConnection() {
    try {
        const result = await pool.query('SELECT NOW()');
        console.log('✅ Connected to Neon:', result.rows[0]);
    } catch (err) {
        console.error('❌ Neon connection failed:', err);
    }
}

async function initDb() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS complaints (
                id SERIAL PRIMARY KEY,
                trackingid VARCHAR(20) UNIQUE,
                studentid VARCHAR(50),
                email VARCHAR(255),
                name VARCHAR(255),
                section VARCHAR(255),
                text TEXT,
                score INT DEFAULT 0,
                priority VARCHAR(20),
                category VARCHAR(100),
                reason TEXT,
                "isAnonymous" BOOLEAN DEFAULT FALSE,
                date VARCHAR(50)
            );
        `);

        // Columns added after the table already existed in production —
        // ADD COLUMN IF NOT EXISTS makes this safe to run on every boot.
        await pool.query(`ALTER TABLE complaints ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'Pending';`);
        await pool.query(`ALTER TABLE complaints ADD COLUMN IF NOT EXISTS contactnumber VARCHAR(50);`);
        await pool.query(`ALTER TABLE complaints ADD COLUMN IF NOT EXISTS image_url TEXT;`);
        await pool.query(`ALTER TABLE complaints ADD COLUMN IF NOT EXISTS image_public_id TEXT;`);
        await pool.query(`ALTER TABLE complaints ADD COLUMN IF NOT EXISTS student_categories TEXT;`);
        await pool.query(`ALTER TABLE complaints ADD COLUMN IF NOT EXISTS feedback TEXT;`);

        console.log('💾 Neon database table checked/created successfully!');
    } catch (err) {
        console.error('❌ Failed to initialize database table:', err.message);
    }
}

// ---------- helpers ----------
function generateTrackingID() {
    return 'ER-' + crypto.randomBytes(4).toString('hex').toUpperCase();
}

// Backup keyword scoring, used only if the AI call fails
const FALLBACK_KEYWORDS = {
    basura: 20,
    sira: 40,
    ingay: 20,
    gulo: 25,
    away: 30,
    banyo: 35
};

function scoreByKeywords(complaintText) {
    const matched = Object.keys(FALLBACK_KEYWORDS).filter(word => complaintText.includes(word));
    const score = matched.reduce((sum, word) => sum + FALLBACK_KEYWORDS[word], 0);

    let priority = 'Low';
    if (score >= 75) priority = 'Critical';
    else if (score >= 50) priority = 'High';
    else if (score >= 25) priority = 'Medium';

    return { score, priority, category: 'Other', reason: 'No AI analysis.' };
}

async function scoreWithAI(complaint) {
    const response = await ai.models.generateContent({
        model: 'gemini-flash-latest',
        contents: `
You are an AI assistant for a Senior High School Complaint Management System.

Your job is to evaluate a student complaint and determine its severity.

Evaluate the complaint using these criteria:

- Student safety risk (40%)
- Urgency (20%)
- Number of students affected (20%)
- Disruption to school operations (20%)

Priority Scale:
0-24 = Low
25-49 = Medium
50-74 = High
75-100 = Critical

Possible Categories:
- Bullying
- Harassment
- Violence
- Facility Issue
- Sanitation
- Academic Concern
- Teacher Concern
- Property Damage
- Security
- Other

IMPORTANT:
- Return ONLY valid JSON.
- Keep the reason short as a summary of the whole text.
- Do NOT use markdown.
- Do NOT use \`\`\`json.
- Do NOT include explanations outside the JSON.

Return exactly this format:

{
  "score": 0,
  "priority": "",
  "category": "",
  "reason": ""
}

Complaint:
"${complaint}"
`
    });

    const cleaned = response.text.replace(/```json/g, '').replace(/```/g, '').trim();
    return JSON.parse(cleaned);
}

// ---------- socket.io ----------
io.on('connection', async (socket) => {
    console.log('🔌 A client connected to the WebSocket tunnel!');

    try {
        const result = await pool.query('SELECT * FROM complaints ORDER BY score DESC, id DESC');
        socket.emit('complaintHistory', result.rows);
    } catch (err) {
        console.error('❌ Error fetching history:', err.message);
    }

    socket.on('deleteComplaint', async ({ trackingId }) => {
        try {
            const result = await pool.query(
                `DELETE FROM complaints WHERE trackingid = $1 RETURNING image_public_id`,
                [trackingId]
            );

            const publicId = result.rows[0]?.image_public_id;
            if (publicId) {
                try {
                    await cloudinary.uploader.destroy(publicId);
                    console.log(`🗑️ Deleted Cloudinary image: ${publicId}`);
                } catch (err) {
                    // Don't block the complaint deletion just because Cloudinary cleanup failed —
                    // the record is already gone from the dashboard either way.
                    console.error('❌ Cloudinary delete failed:', err);
                }
            }

            io.emit('cardDeleted', { trackingId });
            console.log(`deleted ${trackingId}`);
        } catch (err) {
            console.error(err);
        }
    });

    socket.on('updateStatus', async ({ trackingId, status, feedback }) => {
        try {
            await pool.query(
                `UPDATE complaints SET status = $1, feedback = $2 WHERE trackingid = $3`,
                [status, feedback || null, trackingId]
            );
            console.log(`${trackingId} changed to ${status}`);
            io.emit('statusUpdated', { trackingId, status, feedback });
        } catch (err) {
            console.error(err);
        }
    });

    socket.on('disconnect', () => {
        console.log('❌ Client disconnected');
    });
});

async function bgProcess(data) {
    const {
        trackingId,
        complaintText,
        complaintDate,
        studentId,
        email,
        fullName,
        section,
        isAnonymous,
        status,
        contactNumber,
        studentCategories,
        imageFile
    } = data;

    let score, priority, category, reason;
    try {
        const aiData = await scoreWithAI(complaintText);
        ({ score, priority, category, reason } = aiData);
        console.log('✅ AI Analysis:', aiData);
    } catch (err) {
        console.warn('⚠️ AI failed. Using keyword scoring.');
        console.error(err);
        ({ score, priority, category, reason } = scoreByKeywords(complaintText));
    }

    // --- image upload (optional) ---
    const uploadedImage = await uploadImageIfPresent(imageFile);
    const imageUrl = uploadedImage ? uploadedImage.url : null;
    const imagePublicId = uploadedImage ? uploadedImage.publicId : null;

    io.emit('newComplaint', {
        trackingId,
        studentId,
        email,
        name: fullName,
        section: section,
        text: complaintText,
        score,
        priority,
        category,
        reason,
        isAnonymous,
        date: complaintDate,
        status,
        contactNumber,
        imageUrl,
        studentCategories
    });

    await pool.query(
        `UPDATE complaints
     SET score = $1,
         priority = $2,
         category = $3,
         reason = $4,
         image_url = $5,
         image_public_id = $6
     WHERE trackingid = $7`,
        [
            score,
            priority,
            category,
            reason,
            imageUrl,
            imagePublicId,
            trackingId
        ]
    );
}
// ---------- routes ----------
app.post('/submit', upload.single('image'), async (req, res) => {
    console.log('\n--- New Complaint Received ---');

    const trackingId = generateTrackingID();
    const studentId = req.body.studentId || null;
    const email = req.body.email || '';
    const contactNumber = req.body.contactNumber || '';
    const studentCategories = req.body.studentCategories || null; // one comma-joined string, e.g. "Bullying, Harassment"
    const complaint = req.body.complaint || '';
    const complaintText = complaint.toLowerCase();

    const isAnonymous = req.body.anonymous === true || req.body.anonymous === 'true';
    const complaintDate = req.body.complaintDate || new Date().toLocaleDateString();
    const status = 'Pending';

    const fullName = isAnonymous ? 'Anonymous' : req.body.fullName || 'Anonymous';
    const section = req.body.section || 'Anonymous';

    // --- save to Neon ---
    try {
        console.log('✅ Complaint saved to Neon.');
    } catch (err) {
        console.error('❌ Database Error:', err);
        return res.status(500).json({ success: false, error: err.message });
    }

    // --- broadcast to connected dashboards ---

    try {
        console.log(`[SUBMIT] Received complaint ${trackingId}`);

        // Save the complaint FIRST, before AI or image processing.
        await pool.query(
            `INSERT INTO complaints
                (
                    trackingid,
                    studentid,
                    email,
                    name,
                    section,
                    text,
                    score,
                    priority,
                    category,
                    reason,
                    "isAnonymous",
                    date,
                    status,
                    contactnumber,
                    image_url,
                    image_public_id,
                    student_categories
                )
             VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16, $17
             )`,
            [
                trackingId,
                studentId,
                email,
                fullName,
                section,
                complaintText,
                0,
                'Processing',
                'Other',
                'AI analysis is pending.',
                isAnonymous,
                complaintDate,
                status,
                contactNumber,
                null,
                null,
                studentCategories
            ]
        );

        console.log(`[DATABASE] Complaint ${trackingId} saved`);

        // Confirm to the student as soon as Neon has saved the complaint.
        res.status(200).json({
            success: true,
            trackingId
        });

        // Run the slower tasks after responding to the student.
        void bgProcess({
            trackingId,
            complaintText,
            complaintDate,
            studentId,
            email,
            fullName,
            section,
            isAnonymous,
            status,
            contactNumber,
            studentCategories,
            imageFile: req.file || null
        }).catch(err => {
            console.error('[BACKGROUND] Processing failed:', err);
        });;

    } catch (err) {
        console.error('[SUBMIT] Complaint could not be saved:', err);

        if (!res.headersSent) {
            return res.status(500).json({
                success: false,
                error: 'Unable to submit complaint. Please try again.'
            });
        }
    }
});

// ---------- startup ----------
(async () => {
    await testDbConnection();
    await initDb();
    server.listen(PORT, () => {
        console.log(`Server running on port ${PORT}`);
    });
})();
