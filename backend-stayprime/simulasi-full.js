const { Kafka } = require('kafkajs');
const mysql = require('mysql2/promise');
const mongoose = require('mongoose');
require('dotenv').config();

// ==========================================
// 1. INISIALISASI
// ==========================================
const kafka = new Kafka({ clientId: process.env.KAFKA_CLIENT_ID, brokers: [process.env.KAFKA_BROKER] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'stayprime-core-group' });

// Skema Audit Log (MongoDB)
const auditSchema = new mongoose.Schema({
    event_type: String,
    res_id: String,
    description: String,
    payload: Object,
    timestamp: { type: Date, default: Date.now }
});
const AuditLog = mongoose.model('AuditLog', auditSchema);

// Fungsi Delay untuk Simulasi Jeda Waktu
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function runFullSimulation() {
    console.log("🚀 MEMULAI SIMULASI PENUH STAYPRIME (HULU - HILIR)\n");

    const db = await mysql.createConnection({
        host: process.env.DB_HOST, port: process.env.DB_PORT, 
        user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_NAME
    });
    await mongoose.connect(process.env.MONGO_URI);

    // Persiapan Data Master (Guest & Room)
    await db.execute(`INSERT IGNORE INTO guests (GuestID, Nama_Tamu, Kontak) VALUES (2, 'Andi Pratama', '085555555')`);
    await db.execute(`INSERT IGNORE INTO rooms (RoomID, Tipe, Status) VALUES ('105', 'Standard', 'READY')`);

    // ==========================================
    // 2. KONSUMER MENDENGARKAN 4 TOPIK
    // ==========================================
    await consumer.connect();
    await consumer.subscribe({ topic: 'reservasi', fromBeginning: false });
    await consumer.subscribe({ topic: 'check_in', fromBeginning: false });
    await consumer.subscribe({ topic: 'layanan', fromBeginning: false });
    await consumer.subscribe({ topic: 'checkout', fromBeginning: false });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            const data = JSON.parse(message.value.toString());
            console.log(`\n📥 [CONSUMER] Event Masuk: [${topic.toUpperCase()}]`);

            try {
                // Routing logika berdasarkan Nama Topik
                if (topic === 'reservasi') {
                    // Sesuai entitas reservations [cite: 28]
                    await db.execute(
                        `INSERT INTO reservations (ResID, GuestID, RoomID, Tgl_Masuk, Tgl_Keluar, Harga, Status) 
                         VALUES (?, ?, ?, ?, ?, ?, 'RESERVED')`,
                        [data.resId, data.guestId, data.roomId, data.tglMasuk, data.tglKeluar, data.hargaKamar]
                    );
                    console.log(`   🗄️ [MySQL] Reservasi ${data.resId} ditambahkan. Status Kamar: RESERVED`);
                
                } else if (topic === 'check_in') {
                    // Update status kamar dan reservasi [cite: 27, 28]
                    await db.execute(`UPDATE reservations SET Status = 'CHECKED_IN' WHERE ResID = ?`, [data.resId]);
                    await db.execute(`UPDATE rooms SET Status = 'OCCUPIED' WHERE RoomID = ?`, [data.roomId]);
                    console.log(`   🗄️ [MySQL] Check-in berhasil. Kamar ${data.roomId} menjadi OCCUPIED`);
                
                } else if (topic === 'layanan') {
                    // Insert ke tabel services (F&B/Fasilitas) 
                    await db.execute(
                        `INSERT INTO services (ResID, Nama_Layanan, Harga, Status) VALUES (?, ?, ?, 'PENDING')`,
                        [data.resId, data.namaLayanan, data.hargaLayanan]
                    );
                    console.log(`   🗄️ [MySQL] Pesanan layanan "${data.namaLayanan}" ditambahkan ke tagihan.`);
                
                } else if (topic === 'checkout') {
                    // Update kamar jadi kotor, reservasi selesai, cetak invoice [cite: 27, 28, 29]
                    await db.execute(`UPDATE reservations SET Status = 'CHECKED_OUT' WHERE ResID = ?`, [data.resId]);
                    await db.execute(`UPDATE rooms SET Status = 'DIRTY' WHERE RoomID = ?`, [data.roomId]);
                    await db.execute(
                        `INSERT INTO invoices (ResID, Total, Status) VALUES (?, ?, 'PAID')`,
                        [data.resId, data.totalPembayaran]
                    );
                    console.log(`   🗄️ [MySQL] Checkout selesai. Kamar ${data.roomId} menjadi DIRTY. Invoice dibuat.`);
                }

                // Log terpusat di MongoDB untuk semua jenis event [cite: 21]
                await AuditLog.create({
                    event_type: topic,
                    res_id: data.resId,
                    description: `Event ${topic} berhasil diproses sistem`,
                    payload: data
                });
                console.log(`   📝 [MongoDB] Rekam jejak (Audit Log) ${topic} disimpan.`);

            } catch (error) {
                console.error(`   ❌ [MySQL/Mongo Error]:`, error.message);
            }
        },
    });

    // ==========================================
    // 3. PRODUSER MENSIMULASIKAN SKENARIO TAMU
    // ==========================================
    await producer.connect();
    
    const sendEvent = async (topicName, payloadData) => {
        await producer.send({ topic: topicName, messages: [{ value: JSON.stringify(payloadData) }] });
        console.log(`\n📤 [PRODUCER] Menembakkan event '${topicName}'...`);
    };

    console.log("\n--- SKENARIO DIMULAI DALAM 3 DETIK ---");
    await delay(3000);

    // Skenario 1: Tamu Booking Kamar secara Online
    await sendEvent('reservasi', {
        resId: '#RES-200', guestId: 2, roomId: '105', 
        tglMasuk: '2026-06-20', tglKeluar: '2026-06-22', hargaKamar: 1000000
    });
    await delay(4000); // Jeda 4 detik

    // Skenario 2: Tamu Tiba di Hotel dan Front Desk melakukan Check-In
    await sendEvent('check_in', { resId: '#RES-200', roomId: '105' });
    await delay(4000);

    // Skenario 3: Malam hari tamu pesan Nasi Goreng (F&B)
    await sendEvent('layanan', { resId: '#RES-200', namaLayanan: 'Nasi Goreng Spesial', hargaLayanan: 75000 });
    await delay(3000);

    // Skenario 4: Pagi hari tamu request Laundry
    await sendEvent('layanan', { resId: '#RES-200', namaLayanan: 'Laundry Express', hargaLayanan: 50000 });
    await delay(5000);

    // Skenario 5: Tamu Check-Out & Bayar Total Keseluruhan
    const totalSemua = 1000000 + 75000 + 50000;
    await sendEvent('checkout', { resId: '#RES-200', roomId: '105', totalPembayaran: totalSemua });

    console.log("\n✅ SEMUA SKENARIO SELESAI DITEMBAKKAN!");
    // Biarkan script tetap jalan untuk memastikan Consumer memproses event terakhir
}

runFullSimulation().catch(console.error);