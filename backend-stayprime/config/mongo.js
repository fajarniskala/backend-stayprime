const { MongoClient } = require('mongodb');

// URL Sakti: Memanggil nama container 'mongodb' dan bawa kunci admin!
const url = 'mongodb://admin:adminpassword@mongodb:27017/?authSource=admin';
const client = new MongoClient(url);

let db;

async function initMongo() {
    try {
        // TAMBAHAN KUNCI: Kalau udah pernah konek, jangan connect lagi! (Cegah Overload)
        if (db) return db;

        await client.connect();
        console.log('✅ [MongoDB] Berhasil terhubung ke database Audit Log');
        
        // Membuat/Memilih database khusus untuk project lu
        db = client.db('stayprime_audit_db'); 
        
        return db;
    } catch (error) {
        console.error('❌ [MongoDB Error] Gagal koneksi:', error.message);
    }
}

module.exports = { initMongo, getDb: () => db };