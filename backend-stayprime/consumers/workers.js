const { kafka } = require('../config/kafka');
const { initMongo, getDb } = require('../config/mongo');

async function startAllWorkers() {
    try {
        const consumer = kafka.consumer({ groupId: 'stayprime-audit-group' });
        await consumer.connect();
        
        // Subscribe semua topik sekaligus (Lebih rapi dan aman dari looping)
        await consumer.subscribe({ 
            topics: ['reservasi', 'check_in', 'layanan', 'checkout', 'pembayaran'], 
            fromBeginning: true 
        });

        // Coba pancing koneksi DB di awal
        await initMongo();

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    const payload = JSON.parse(message.value.toString());
                    console.log(`📥 [Kafka Worker] Menerima event '${topic}'`);
                    
                    // ====================================================
                    // TRIK ANTI NYANGKUT: Ambil database secara dinamis!
                    // ====================================================
                    let db = getDb();
                    
                    // Kalau ternyata DB belum konek (karena Docker lemot), paksa konek ulang!
                    if (!db) {
                        console.log("🔄 Koneksi MongoDB belum siap, mencoba reconnect...");
                        db = await initMongo();
                    }

                    if (db) {
                        await db.collection('audit_logs').insertOne({
                            topik: topic,
                            data_transaksi: payload,
                            waktu_eksekusi: new Date()
                        });
                        console.log(`✅ [MongoDB] Log ${topic} berhasil disimpan hari ini!`);
                    } else {
                        console.error(`❌ [MongoDB Fatal] Database masih mati, log ${topic} lewat begitu saja!`);
                    }
                } catch (err) {
                    console.error(`❌ [Kafka Worker Error] Gagal memproses pesan:`, err.message);
                }
            }
        });
        console.log('✅ [Kafka Worker] Berjalan & siap mencatat log ke MongoDB');
    } catch (error) {
        console.error('❌ [Kafka Worker Fatal] Gagal start:', error);
    }
}

module.exports = { startAllWorkers };