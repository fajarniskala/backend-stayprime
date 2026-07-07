const express = require('express');
const cors = require('cors'); // <-- DITAMBAHKAN BIAR UI BISA NEMBAK API
const app = express();
const apiRoutes = require('./routes/api');
const { initKafka } = require('./config/kafka');
const { startAllWorkers } = require('./consumers/workers');

// Izinkan semua komunikasi lintas Port
app.use(cors()); 
app.use(express.json());

// Rute ROOT agar port 3001 ada halamannya
app.get('/', (req, res) => {
    res.status(200).json({
        service: "StayPrime Backend Worker (Consumer)",
        status: "⚙️ ONLINE & LISTENING",
        message: "Sistem Worker siap memproses antrean dari Kafka."
    });
});

async function startServer() {
    try {
        await initKafka();       // 1. Koneksi Kafka
        await startAllWorkers(); // 2. Jalankan Worker
        
        // 3. Buka API Sekunder (Jika ada)
        app.use('/api', apiRoutes); 
        
        const PORT = 3001;
        app.listen(PORT, () => console.log(`⚙️ [WORKER] Server API berjalan di port ${PORT}`));
    } catch (err) {
        console.error("❌ Gagal booting sistem worker:", err);
    }
}

startServer();