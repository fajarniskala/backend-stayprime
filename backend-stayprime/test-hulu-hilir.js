const { Kafka } = require('kafkajs');
const mysql = require('mysql2/promise');
const mongoose = require('mongoose');
require('dotenv').config();

// ==========================================
// 1. INISIALISASI KONEKSI (KAFKA & DATABASE)
// ==========================================
const kafka = new Kafka({ clientId: process.env.KAFKA_CLIENT_ID, brokers: [process.env.KAFKA_BROKER] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'stayprime-group' });

// Skema Mongoose untuk Audit Log MongoDB
const auditSchema = new mongoose.Schema({
    event_type: String,
    res_id: String,
    room_id: String,
    description: String,
    timestamp: { type: Date, default: Date.now }
});
const AuditLog = mongoose.model('AuditLog', auditSchema);

async function runSimulation() {
    console.log("🚀 Memulai Simulasi Hulu ke Hilir StayPrime...\n");

    // Koneksi ke MySQL Master (Write) & MongoDB
    const db = await mysql.createConnection({
        host: process.env.DB_HOST, port: process.env.DB_PORT, 
        user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_NAME
    });
    await mongoose.connect(process.env.MONGO_URI);
    console.log("✅ Database MySQL (Master) & MongoDB Terhubung!");

    // Siapkan data mentah (Dummy) di MySQL agar bisa di-update
    await db.execute(`INSERT IGNORE INTO guests (GuestID, Nama_Tamu, Kontak) VALUES (1, 'Budi Santoso', '08123456789')`);
    await db.execute(`INSERT IGNORE INTO reservations (ResID, GuestID, RoomID, Status) VALUES ('#RES-101', 1, '102', 'RESERVED')`);

    // ==========================================
    // 2. BAGIAN HILIR: CONSUMER MENDENGARKAN EVENT
    // ==========================================
    await consumer.connect();
    await consumer.subscribe({ topic: 'check_in', fromBeginning: true });
    
    // Proses berjalan di background, menunggu pesan masuk
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const eventData = JSON.parse(message.value.toString());
            console.log(`\n📥 [HILIR/CONSUMER] Menerima event '${topic}' dari Kafka:`, eventData);

            try {
                // A. Update transaksional di MySQL Master
                await db.execute("UPDATE reservations SET Status = 'CHECKED_IN' WHERE ResID = ?", [eventData.resId]);
                await db.execute("UPDATE rooms SET Status = 'OCCUPIED' WHERE RoomID = ?", [eventData.roomId]);
                console.log(`   🗄️ [MySQL] Status reservasi ${eventData.resId} & kamar ${eventData.roomId} berhasil diperbarui.`);

                // B. Insert rekam jejak di MongoDB (Polyglot Persistence)
                await AuditLog.create({
                    event_type: topic,
                    res_id: eventData.resId,
                    room_id: eventData.roomId,
                    description: `Tamu ${eventData.guestName} berhasil melakukan proses check-in.`
                });
                console.log(`   📝 [MongoDB] Audit log berhasil dicatat tanpa membebani MySQL.`);
                
                console.log("\n🎉 SIMULASI SUKSES! Tekan Ctrl+C untuk keluar.");
            } catch (error) {
                console.error("   ❌ Gagal memproses data database:", error);
            }
        },
    });

    // ==========================================
    // 3. BAGIAN HULU: PRODUCER MENGIRIM EVENT
    // ==========================================
    await producer.connect();
    console.log("⏳ Menunggu 3 detik sebelum menembakkan event Check-In...\n");
    
    setTimeout(async () => {
        const payload = { 
            resId: '#RES-101', 
            roomId: '102', 
            guestName: 'Budi Santoso' 
        };

        console.log("📤 [HULU/PRODUCER] Mengirim event 'check_in' ke Apache Kafka...");
        await producer.send({
            topic: 'check_in',
            messages: [{ value: JSON.stringify(payload) }],
        });
        
        await producer.disconnect();
    }, 3000);
}

runSimulation().catch(console.error);