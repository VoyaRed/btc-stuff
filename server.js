const express = require('express');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const PORT = process.env.PORT || 3000;

// Initialize Supabase Client
const supabaseUrl = process.env.SUPABASE_URL || 'https://bpizkikscieyhzajrrrg.supabase.co';
const supabaseKey = process.env.SUPABASE_KEY || 'sb_publishable_UwkvMmq91Y5jS1PEJ9IE_w_vN5-JWUP';
const supabase = createClient(supabaseUrl, supabaseKey);

// Serve the static HTML dashboard
app.use(express.static(path.join(__dirname, 'public')));

// API route to get the last 24 hours of predictions
app.get('/api/predictions', async (req, res) => {
    try {
        // Fetching the most recent 96 periods (24 hours * 4 15m candles)
        const { data, error } = await supabase
            .from('bot_predictions')
            .select('*')
            .order('timestamp', { ascending: false })
            .limit(96);

        if (error) throw error;
        res.json(data);
    } catch (err) {
        console.error('Error fetching data:', err);
        res.status(500).json({ error: 'Failed to load live predictions' });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 upsidedowncake-bot UI running on port ${PORT}`);
});
